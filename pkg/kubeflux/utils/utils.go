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

package utils

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	pb "sigs.k8s.io/scheduler-plugins/pkg/kubeflux/fluxcli-grpc"
)

type NoopStateData struct{}

func NewNoopStateData() framework.StateData {
	return &NoopStateData{}
}

func (d *NoopStateData) Clone() framework.StateData {
	return d
}

// InspectPodInfo takes a pod object and returns the pod.spec
func InspectPodInfo(pod *v1.Pod) *pb.PodSpec {
	ps := new(pb.PodSpec)
	ps.Id = pod.Name
	cont := pod.Spec.Containers[0]
	/**
	This will need to be done here AND at client level
	if len(pr.Labels) > 0 {
			r := make([]Resource, 0)
			for key, val := range pr.Labels {
				if strings.Contains(key, "nfd") {
					count, _ := strconv.Atoi(val)

					r = append(r, Resource{Type: strings.Split(key,".")[1], Count: int64(count)})
				}
			}
			if len(r) > 0 {
				socket_resources[0].With = r
			}
		}

	**/
	specRequests := cont.Resources.Requests
	specLimits := cont.Resources.Limits

	if specRequests.Cpu().Value() == 0 {
		ps.Cpu = 1
	} else {
		ps.Cpu = int32(specRequests.Cpu().Value())
	}
	if specRequests.Memory().Value() > 0 {
		ps.Memory = specRequests.Memory().Value()
	}
	gpu := specLimits["nvidia.com/gpu"]
	ps.Gpu = gpu.Value()
	ps.Storage = specRequests.StorageEphemeral().Value()

	klog.Infof("[Jobspec] Pod spec: CPU %v/%v-milli, memory %v/%v-milli, GPU %v, storage %v", ps.Cpu, specRequests.Cpu().MilliValue(),
		ps.Memory, specRequests.Memory().MilliValue(), ps.Gpu, ps.Storage)

	return ps
}
