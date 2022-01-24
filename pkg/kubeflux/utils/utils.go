package utils

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	pb "sigs.k8s.io/scheduler-plugins/pkg/kubeflux/fluxcli-grpc"
)

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
		ps.Cpu = specRequests.Cpu().Value()
	}
	if specRequests.Memory().Value() > 0 {
		ps.Memory = specRequests.Memory().Value()
	}
	gpu := specLimits["nvidia.com/gpu"]
	ps.Gpu = gpu.Value()
	ps.Storage = specRequests.StorageEphemeral().Value()

	fmt.Printf("[Jobspec] Pod spec: CPU %v/%v-milli, memory %v/%v-milli, GPU %v, storage %v\n", ps.Cpu, specRequests.Cpu().MilliValue(),
		ps.Memory, specRequests.Memory().MilliValue(), ps.Gpu, ps.Storage)

	return ps
}
