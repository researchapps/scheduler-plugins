/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeflux

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"sigs.k8s.io/scheduler-plugins/pkg/coscheduling/core"
	pgclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	schedinformer "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
	kfcore "sigs.k8s.io/scheduler-plugins/pkg/kubeflux/core"
	pb "sigs.k8s.io/scheduler-plugins/pkg/kubeflux/fluxcli-grpc"
	"sigs.k8s.io/scheduler-plugins/pkg/kubeflux/utils"
)

type KubeFlux struct {
	mutex          sync.Mutex
	handle         framework.Handle
	podNameToJobId map[string]uint64
	pgMgr          *core.PodGroupManager
}

var _ framework.PreFilterPlugin = &KubeFlux{}
var _ framework.FilterPlugin = &KubeFlux{}

// Name is the name of the plugin used in the Registry and configurations.
const Name = "KubeFlux"

func (kf *KubeFlux) Name() string {
	return Name
}

// initialize and return a new Flux Plugin
func New(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {

	kf := &KubeFlux{handle: handle, podNameToJobId: make(map[string]uint64)}
	klog.Info("Create plugin")
	ctx := context.TODO()
	kfcore.Init()


	fluxPodsInformer := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	fluxPodsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: kf.updatePod,
		DeleteFunc: kf.deletePod,
	})
	
	go fluxPodsInformer.Run(ctx.Done())
	klog.Info("Create generic pod informer")

	pgclient := pgclientset.NewForConfigOrDie(handle.KubeConfig())
	podGroupInformerFactory := schedinformer.NewSharedInformerFactory(pgclient, 0)
	podGroupInformer := podGroupInformerFactory.Scheduling().V1alpha1().PodGroups()

	klog.Info("Create pod group")
	
	fieldSelector, err := fields.ParseSelector(",status.phase!=" + string(v1.PodSucceeded) + ",status.phase!=" + string(v1.PodFailed))
	if err != nil {
		klog.ErrorS(err, "ParseSelector failed")
		os.Exit(1)
	}
	informerFactory := informers.NewSharedInformerFactoryWithOptions(handle.ClientSet(), 0, informers.WithTweakListOptions(func(opt *metav1.ListOptions) {
		opt.FieldSelector = fieldSelector.String()
	}))
	podInformer := informerFactory.Core().V1().Pods()

	scheduleTimeDuration := time.Duration(500) * time.Second

	pgMgr := core.NewPodGroupManager(pgclient, handle.SnapshotSharedLister(), &scheduleTimeDuration, podGroupInformer, podInformer)
	kf.pgMgr = pgMgr


	// stopCh := make(chan struct{})
	// defer close(stopCh)
	// informerFactory.Start(stopCh)
	podGroupInformerFactory.Start(ctx.Done())
	informerFactory.Start(ctx.Done())


	if !cache.WaitForCacheSync(ctx.Done(), podGroupInformer.Informer().HasSynced, podInformer.Informer().HasSynced) {
		err := fmt.Errorf("WaitForCacheSync failed")
		klog.ErrorS(err, "Cannot sync caches")
		return nil, err
	}

	klog.Info("kubeflux starts")
	return kf, nil
}

func (kf *KubeFlux) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	klog.Infof("Examining the pod")
	var err error
	var nodename string
	if pgname, ok := kf.isGroup(pod); ok {
		// klog.Infof("GROUP (%s:%s)", pod.Name, pgname)
		if !kfcore.HaveList(pgname) {
			klog.Infof("Getting a pod group")
			groupSize, _ := kf.groupPreFilter(ctx, pod)
			// if err != nil {
			// 	klog.ErrorS(err, "Group not completed yet", "pod", klog.KObj(pod))
			// 	state.Write(framework.StateKey("Prefilter-Coscheduling"), utils.NewNoopStateData())
			// 	return framework.NewStatus(framework.Unschedulable, err.Error())
			// }
			if _, err = kf.AskFlux(pod, groupSize); err != nil {
				return nil, framework.NewStatus(framework.Unschedulable, err.Error())
			}
			// klog.Infof("Group size %d: ", nodename, groupSize)
		}
		nodename, err = kfcore.GetNextNode(pgname)
		klog.Infof("Node Selected %s (%s:%s)", nodename, pod.Name, pgname)
		if err != nil {
			return nil, framework.NewStatus(framework.Unschedulable, err.Error())
		}
	} else {
		nodename, err = kf.AskFlux(pod, 1)
		if err != nil {
			return nil, framework.NewStatus(framework.Unschedulable, err.Error())
		}
	}

	klog.Info("Node Selected: ", nodename)
	state.Write(framework.StateKey(pod.Name), &kfcore.FluxStateData{NodeName: nodename})
	return nil, framework.NewStatus(framework.Success, "")

	// if nodenames == nil {
	// 	klog.Warning("Pod cannot be scheduled by KubeFlux, nodename ", nodenames)
	// 	return framework.NewStatus(framework.Unschedulable, "Pod cannot be scheduled by KubeFlux")
	// }
}

func (kf *KubeFlux) isGroup(pod *v1.Pod) (string, bool) {
	pgFullName, pg := kf.pgMgr.GetPodGroup(pod)

	if pg == nil {
		klog.InfoS("Not in group", "pod", klog.KObj(pod))
		return "", false
	}
	return pgFullName, true
}

func (kf *KubeFlux) groupPreFilter(ctx context.Context, pod *v1.Pod) (int, error) {
	// klog.InfoS("Flux Pre-Filter", "pod", klog.KObj(pod))
	klog.InfoS("Flux Pre-Filter", "pod labels", pod.Labels)
	_, pg := kf.pgMgr.GetPodGroup(pod)
	if pg == nil {
		klog.InfoS("Not in group", "pod", klog.KObj(pod))
		return 0, nil
	}
	// if _, ok := kf.pgMgr.LastDeniedPG.Get(pgFullName); ok {
	// 	return 0, fmt.Errorf("pod with pgName: %v last failed in 3s, deny", pgFullName)
	// }
	// pods, err := kf.pgMgr.PodLister.Pods(pod.Namespace).List(
	// 	labels.SelectorFromSet(labels.Set{util.PodGroupLabel: util.GetPodGroupLabel(pod)}),
	// )

	// if err != nil {
	// 	return 0, fmt.Errorf("podLister list pods failed: %v", err)
	// }
	// klog.Info("Min pod group ", int(pg.Spec.MinMember), " pods avail ", len(pods))
	// if len(pods) < int(pg.Spec.MinMember) {
	// 	return 0, fmt.Errorf("pre-filter pod %v cannot find enough sibling pods, "+
	// 		"current pods number: %v, minMember of group: %v", pod.Name, len(pods), pg.Spec.MinMember)
	// }
	klog.Info("pod group members ", pg.Spec.MinMember)
	return int(pg.Spec.MinMember) /*len(pods)*/, nil
}

func (kf *KubeFlux) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.Info("Filtering input node ", nodeInfo.Node().Name)
	if v, e := cycleState.Read(framework.StateKey(pod.Name)); e == nil {
		if value, ok := v.(*kfcore.FluxStateData); ok && value.NodeName != nodeInfo.Node().Name {
			return framework.NewStatus(framework.Unschedulable, "pod is not permitted")
		} else {
			klog.Info("Filter: node selected by Flux ", value.NodeName)
		}
	}

	return framework.NewStatus(framework.Success)
}

func (kf *KubeFlux) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (kf *KubeFlux) AskFlux(pod *v1.Pod, count int) (string, error) {
	// clean up previous match if a pod has already allocated previously
	kf.mutex.Lock()
	_, isPodAllocated := kf.podNameToJobId[pod.Name]
	kf.mutex.Unlock()

	if isPodAllocated {
		klog.Info("Clean up previous allocation")
		kf.mutex.Lock()
		kf.cancelFluxJobForPod(pod.Name)
		kf.mutex.Unlock()
	}

	jobspec := utils.InspectPodInfo(pod)
	conn, err := grpc.Dial("127.0.0.1:4242", grpc.WithInsecure())

	if err != nil {
		klog.Errorf("[FluxClient] Error connecting to server: %v", err)
		return "", err
	}
	defer conn.Close()

	grpcclient := pb.NewFluxcliServiceClient(conn)
	_, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	request := &pb.MatchRequest{
		Ps:      jobspec,
		Request: "allocate",
		Count:   int32(count)}

	r, err2 := grpcclient.Match(context.Background(), request)
	if err2 != nil {
		klog.Errorf("[FluxClient] did not receive any match response: %v", err2)
		return "", err
	}

	klog.Infof("[FluxClient] response podID %s", r.GetPodID())

	_, ok := kf.isGroup(pod)
	if count > 1 || ok {
		pgFullName, _ := kf.pgMgr.GetPodGroup(pod)
		nodelist := kfcore.CreateNodePodsList(r.GetNodelist(), pgFullName)
		klog.Infof("[FluxClient] response nodeID %s", r.GetNodelist())
		klog.Info("[FluxClient] Parsed Nodelist ", nodelist)
		jobid := uint64(r.GetJobID())

		kf.mutex.Lock()
		kf.podNameToJobId[pod.Name] = jobid
		klog.Info("Check job set: ", kf.podNameToJobId)
		kf.mutex.Unlock()
	} else {
		nodename := r.GetNodelist()[0].GetNodeID()
		jobid := uint64(r.GetJobID())

		kf.mutex.Lock()
		kf.podNameToJobId[pod.Name] = jobid
		klog.Info("Check job set: ", kf.podNameToJobId)
		kf.mutex.Unlock()

		return nodename, nil
	}

	return "", nil
}

func (kf *KubeFlux) cancelFluxJobForPod(podName string) error {
	jobid := kf.podNameToJobId[podName]

	klog.Infof("Cancel flux job: %v for pod %s", jobid, podName)

	start := time.Now()
	//err := fluxcli.ReapiCliCancel(kf.fluxctx, int64(jobid), false)

	conn, err := grpc.Dial("127.0.0.1:4242", grpc.WithInsecure())
	// or with sockets
	// conn, err := grpc.Dial(
	// 	sock,
	// 	grpc.WithInsecure(),
	// 	grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
	// 		return net.DialTimeout("unix", addr, timeout)
	// 	}))
	if err != nil {
		klog.Errorf("[FluxClient] Error connecting to server: %v", err)
		return err
	}
	defer conn.Close()

	grpcclient := pb.NewFluxcliServiceClient(conn)
	_, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	request := &pb.CancelRequest{
		JobID: int64(jobid),
	}

	res, err := grpcclient.Cancel(context.Background(), request)
	if err != nil {
		klog.Errorf("[FluxClient] did not receive any cancel response: %v", err)
		return err
	}

	if res.Error == 0 {
		delete(kf.podNameToJobId, podName)
	} else {
		klog.Warningf("Failed to delete pod %s from the podname-jobid map.", podName)
	}

	elapsed := metrics.SinceInSeconds(start)
	klog.Info("Time elapsed (Cancel Job) :", elapsed)

	klog.Infof("Job cancellation for pod %s result: %d", podName, err)
	if klog.V(2).Enabled() {
		klog.Info("Check job set: after delete")
		klog.Info(kf.podNameToJobId)
	}
	return nil
}

// EventHandlers
func (kf *KubeFlux) updatePod(oldObj, newObj interface{}) {
	// klog.Info("Update Pod event handler")
	newPod := newObj.(*v1.Pod)
	klog.Infof("Processing event for pod %s", newPod)
	switch newPod.Status.Phase {
	case v1.PodPending:
		// in this state we don't know if a pod is going to be running, thus we don't need to update job map
	case v1.PodRunning:
		// if a pod is start running, we can add it state to the delta graph if it is scheduled by other scheduler
	case v1.PodSucceeded:
		klog.Infof("Pod %s succeeded, kubeflux needs to free the resources", newPod.Name)

		kf.mutex.Lock()
		defer kf.mutex.Unlock()

		if _, ok := kf.podNameToJobId[newPod.Name]; ok {
			kf.cancelFluxJobForPod(newPod.Name)
		} else {
			klog.Infof("Succeeded pod %s/%s doesn't have flux jobid", newPod.Namespace, newPod.Name)
		}
	case v1.PodFailed:
		// a corner case need to be tested, the pod exit code is not 0, can be created with segmentation fault pi test
		klog.Warningf("Pod %s failed, kubeflux needs to free the resources", newPod.Name)

		kf.mutex.Lock()
		defer kf.mutex.Unlock()

		if _, ok := kf.podNameToJobId[newPod.Name]; ok {
			kf.cancelFluxJobForPod(newPod.Name)
		} else {
			klog.Errorf("Failed pod %s/%s doesn't have flux jobid", newPod.Namespace, newPod.Name)
		}
	case v1.PodUnknown:
		// don't know how to deal with it as it's unknown phase
	default:
		// shouldn't enter this branch
	}
}

func (kf *KubeFlux) deletePod(podObj interface{}) {
	klog.Info("Delete Pod event handler")

	pod := podObj.(*v1.Pod)
	klog.Info("Pod status: ", pod.Status.Phase)
	switch pod.Status.Phase {
	case v1.PodSucceeded:
	case v1.PodPending:
		klog.Infof("Pod %s completed and is Pending termination, kubeflux needs to free the resources", pod.Name)

		kf.mutex.Lock()
		defer kf.mutex.Unlock()

		if _, ok := kf.podNameToJobId[pod.Name]; ok {
			kf.cancelFluxJobForPod(pod.Name)
		} else {
			klog.Infof("Terminating pod %s/%s doesn't have flux jobid", pod.Namespace, pod.Name)
		}
	case v1.PodRunning:
		kf.mutex.Lock()
		defer kf.mutex.Unlock()

		if _, ok := kf.podNameToJobId[pod.Name]; ok {
			kf.cancelFluxJobForPod(pod.Name)
		} else {
			klog.Infof("Deleted pod %s/%s doesn't have flux jobid", pod.Namespace, pod.Name)
		}
	}
}
