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
	"sync"
	"time"

	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	pb "sigs.k8s.io/scheduler-plugins/pkg/kubeflux/fluxcli-grpc"
	"sigs.k8s.io/scheduler-plugins/pkg/kubeflux/utils"
)

type KubeFlux struct {
	mutex          sync.Mutex
	handle         framework.Handle
	podNameToJobId map[string]uint64
}

var _ framework.PreFilterPlugin = &KubeFlux{}
var _ framework.FilterPlugin = &KubeFlux{}

// Name is the name of the plugin used in the Registry and configurations.
const Name = "KubeFlux"

func (kf *KubeFlux) Name() string {
	return Name
}

type fluxStateData struct {
	nodeName string
}

func (s *fluxStateData) Clone() framework.StateData {
	clone := &fluxStateData{
		nodeName: s.nodeName,
	}
	return clone
}

func (kf *KubeFlux) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) *framework.Status {
	klog.Infof("Examining the pod")

	nodename, err := kf.askFlux(pod)
	if err != nil {
		return framework.NewStatus(framework.Unschedulable, err.Error())
	}

	if nodename == "NONE" {
		klog.Warning("Pod cannot be scheduled by KubeFlux, nodename ", nodename)
		return framework.NewStatus(framework.Unschedulable, "Pod cannot be scheduled by KubeFlux, nodename "+nodename)
	}

	klog.Info("Node Selected: ", nodename)

	state.Write(framework.StateKey(pod.Name), &fluxStateData{nodeName: nodename})

	return framework.NewStatus(framework.Success, "")
}

func (kf *KubeFlux) Filter(ctx context.Context, cycleState *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	klog.Info("Filtering input node ", nodeInfo.Node().Name)
	if v, e := cycleState.Read(framework.StateKey(pod.Name)); e == nil {
		if value, ok := v.(*fluxStateData); ok && value.nodeName != nodeInfo.Node().Name {
			return framework.NewStatus(framework.Unschedulable, "pod is not permitted")
		} else {
			klog.Info("Filter: node selected by Flux ", value.nodeName)
		}
	}

	return framework.NewStatus(framework.Success)
}

func (kf *KubeFlux) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

func (kf *KubeFlux) askFlux(pod *v1.Pod) (string, error) {
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
		Request: "allocate"}

	r, err2 := grpcclient.Match(context.Background(), request)
	if err2 != nil {
		klog.Errorf("[FluxClient] did not receive any match response: %v", err2)
		return "", err
	}

	klog.Infof("[FluxClient] response nodeID %s", r.GetNodeID())
	klog.Infof("[FluxClient] response podID %s", r.GetPodID())

	nodename := r.GetNodeID()
	jobid := uint64(r.GetJobID())

	kf.mutex.Lock()
	kf.podNameToJobId[pod.Name] = jobid
	klog.Info("Check job set: ", kf.podNameToJobId)
	kf.mutex.Unlock()

	return nodename, nil
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

// initialize and return a new Flux Plugin
func New(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	// creates the in-cluster config
	config, err := rest.InClusterConfig()
	if err != nil {
		klog.Error("Error getting InClusterConfig")
		return nil, err
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Error("Error getting ClientSet")
		return nil, err
	}

	factory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := factory.Core().V1().Pods().Informer()

	klog.Infof("KubeFlux starts")

	kf := &KubeFlux{handle: handle, podNameToJobId: make(map[string]uint64)}

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: kf.updatePod,
	})

	stopPodInformer := make(chan struct{})
	go podInformer.Run(stopPodInformer)

	return kf, nil
}

// EventHandlers
func (kf *KubeFlux) updatePod(oldObj, newObj interface{}) {
	klog.Info("updatePod event handler")
	newPod := newObj.(*v1.Pod)

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
