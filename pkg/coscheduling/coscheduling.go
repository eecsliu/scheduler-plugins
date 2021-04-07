/*
Copyright 2020 The Kubernetes Authors.

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

package coscheduling

import (
	"context"
	"encoding/json"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sort"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/util"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
)

// Args defines the scheduling parameters for Coscheduling plugin.
type Args struct {
	// PermitWaitingTime is the wait timeout in seconds.
	PermitWaitingTimeSeconds int64
	// PodGroupGCInterval is the period to run gc of PodGroup in seconds.
	PodGroupGCIntervalSeconds int64
	// If the deleted PodGroup stays longer than the PodGroupExpirationTime,
	// the PodGroup will be deleted from PodGroupInfos.
	PodGroupExpirationTimeSeconds int64
}

// Coscheduling is a plugin that implements the mechanism of gang scheduling.
type Coscheduling struct {
	frameworkHandle framework.FrameworkHandle
	podLister       corelisters.PodLister
	clientSet		*kubernetes.Clientset
	// key is <namespace>/<PodGroup name> and value is *PodGroupInfo.
	podGroupInfos sync.Map
	// clock is used to get the current time.
	clock util.Clock
	// args is coscheduling parameters.
	args Args
	// waitingPods is used to track what Pods are waiting for determined-preemption.
	waitingGroup *WaitingGroup
}

type WaitingGroup struct {
	name string
	pods map[string]bool
	preempting bool
	approved bool
	// creation holds the timestamp the group was first encountered. The plugin will check
	// if the podgroup exists after PodScheduleTimeout number of seconds. If a PodGroup
	// has been deleted while it is waiting to be scheduled, it will be replaced
	creation time.Time
	// priority of the pod that is waiting. If a higher priority job arrives, its PodGroup
	// will replace the currently waiting PodGroup.
	priority int32
}

// PodGroupInfo is a wrapper to a PodGroup with additional information.
// A PodGroup's priority, timestamp and minAvailable are set according to
// the values of the PodGroup's first pod that is added to the scheduling queue.
type PodGroupInfo struct {
	// key is a unique PodGroup ID and currently implemented as <namespace>/<PodGroup name>.
	key string
	// name is the PodGroup name and defined through a Pod label.
	// The PodGroup name of a regular pod is empty.
	name string
	// priority is the priority of pods in a PodGroup.
	// All pods in a PodGroup should have the same priority.
	priority int32
	// timestamp stores the initialization timestamp of a PodGroup.
	timestamp time.Time
	// minAvailable is the minimum number of pods to be co-scheduled in a PodGroup.
	// All pods in a PodGroup should have the same minAvailable.
	minAvailable int
	// deletionTimestamp stores the timestamp when the PodGroup marked as expired.
	deletionTimestamp *time.Time
}

// pathStringValue is a struct for the json payload passed to the k8s Patch function for pods.
// Patch allows us to modify some of the Pod's metadata, like Labels and Annotations.
type patchStringValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

var _ framework.QueueSortPlugin = &Coscheduling{}
var _ framework.PreFilterPlugin = &Coscheduling{}
var _ framework.PermitPlugin = &Coscheduling{}
var _ framework.UnreservePlugin = &Coscheduling{}

const (
	// Name is the name of the plugin used in Registry and configurations.
	Name = "Coscheduling"
	// PodGroupName is the name of a pod group that defines a coscheduling pod group.
	PodGroupName = "pod-group.scheduling.sigs.k8s.io/name"
	// PodGroupMinAvailable specifies the minimum number of pods to be scheduled together in a pod group.
	PodGroupMinAvailable = "pod-group.scheduling.sigs.k8s.io/min-available"
	// PodScheduleTimeout is the number of seconds before a pod is checked to make sure it isn't deleted
	PodScheduleTimeout = 10
)

// Name returns name of the plugin. It is used in logs, etc.
func (cs *Coscheduling) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(config *runtime.Unknown, handle framework.FrameworkHandle) (framework.Plugin, error) {
	args := Args{
		PermitWaitingTimeSeconds:      10,
		PodGroupGCIntervalSeconds:     30,
		PodGroupExpirationTimeSeconds: 600,
	}

	if err := framework.DecodeInto(config, &args); err != nil {
		return nil, err
	}

	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		klog.Errorf("Unable to get the kubernetes cluster configuration: %w", err)
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil {
		klog.Errorf("Unable to create a client configuration: %w", err)
		return nil, err
	}


	podLister := handle.SharedInformerFactory().Core().V1().Pods().Lister()
	cs := &Coscheduling{frameworkHandle: handle,
		podLister: podLister,
		clientSet: clientset,
		clock:     util.RealClock{},
		args:      args,
		waitingGroup: nil,
	}
	podInformer := handle.SharedInformerFactory().Core().V1().Pods().Informer()
	podInformer.AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					return responsibleForPod(t)
				case cache.DeletedFinalStateUnknown:
					if pod, ok := t.Obj.(*v1.Pod); ok {
						return responsibleForPod(pod)
					}
					return false
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				DeleteFunc: cs.markPodGroupAsExpired,
			},
		},
	)
	go wait.Until(cs.podGroupInfoGC, time.Duration(cs.args.PodGroupGCIntervalSeconds)*time.Second, nil)

	return cs, nil
}

// Less is used to sort pods in the scheduling queue.
// 1. Compare the priorities of Pods.
// 2. Compare the initialization timestamps of PodGroups/Pods.
// 3. Compare the keys of PodGroups/Pods, i.e., if two pods are tied at priority and creation time, the one without podGroup will go ahead of the one with podGroup.
func (cs *Coscheduling) Less(podInfo1, podInfo2 *framework.PodInfo) bool {
	pgInfo1, _ := cs.getOrCreatePodGroupInfo(podInfo1.Pod, podInfo1.InitialAttemptTimestamp)
	pgInfo2, _ := cs.getOrCreatePodGroupInfo(podInfo2.Pod, podInfo2.InitialAttemptTimestamp)

	priority1 := pgInfo1.priority
	priority2 := pgInfo2.priority

	if priority1 != priority2 {
		return priority1 > priority2
	}

	time1 := pgInfo1.timestamp
	time2 := pgInfo2.timestamp

	if !time1.Equal(time2) {
		return time1.Before(time2)
	}

	return pgInfo1.key < pgInfo2.key
}

// getOrCreatePodGroupInfo returns the existing PodGroup in PodGroupInfos if present.
// Otherwise, it creates a PodGroup and returns the value, It stores
// the created PodGroup in PodGroupInfo if the pod defines a  PodGroup and its
// PodGroupMinAvailable is greater than one. It also returns the pod's
// PodGroupMinAvailable (0 if not specified).
func (cs *Coscheduling) getOrCreatePodGroupInfo(pod *v1.Pod, ts time.Time) (*PodGroupInfo, int) {
	podGroupName, podMinAvailable, _ := GetPodGroupLabels(pod)

	var pgKey string
	if len(podGroupName) > 0 && podMinAvailable > 0 {
		pgKey = fmt.Sprintf("%v/%v", pod.Namespace, podGroupName)
	}

	// If it is a PodGroup and present in PodGroupInfos, return it.
	if len(pgKey) != 0 {
		value, exist := cs.podGroupInfos.Load(pgKey)
		if exist {
			pgInfo := value.(*PodGroupInfo)
			// If the deleteTimestamp isn't nil, it means that the PodGroup is marked as expired before.
			// So we need to set the deleteTimestamp as nil again to mark the PodGroup active.
			if pgInfo.deletionTimestamp != nil {
				pgInfo.deletionTimestamp = nil
				cs.podGroupInfos.Store(pgKey, pgInfo)
			}
			return pgInfo, podMinAvailable
		}
	}

	// If the PodGroup is not present in PodGroupInfos or the pod is a regular pod,
	// create a PodGroup for the Pod and store it in PodGroupInfos if it's not a regular pod.
	pgInfo := &PodGroupInfo{
		name:         podGroupName,
		key:          pgKey,
		priority:     podutil.GetPodPriority(pod),
		timestamp:    ts,
		minAvailable: podMinAvailable,
	}

	// If it's not a regular Pod, store the PodGroup in PodGroupInfos
	if len(pgKey) > 0 {
		cs.podGroupInfos.Store(pgKey, pgInfo)
	}
	return pgInfo, podMinAvailable
}

func (cs *Coscheduling) PreemptionTag(pod *v1.Pod) {
	if _, ok := pod.Labels["determined-preemption"]; ok {
		return
	}

	pod.Labels["determined-preemption"] = "true"
	payload := []patchStringValue{{
		Op:    "replace",
		Path:  "/metadata/labels/determined-preemption",
		Value: "yes",
	}}

	payloadBytes, _ := json.Marshal(payload)

	_, err := cs.clientSet.CoreV1().Pods("default").Patch(context.TODO(), pod.Name, types.JSONPatchType,
		payloadBytes, metav1.PatchOptions{})
	if err == nil {
		klog.V(3).Infof("Tagged pod %v for preemption", pod.Name)
	} else {
		klog.V(3).Infof("WARNING: Unable to tag pod %v for preemption", pod.Name)
		klog.V(3).Infof("%v", err)
	}
}

// PreFilter performs the following validations.
// 1. Validate if the PodGroup still exists and the pods are still pending
// 2. If there isn't enough space in the cluster, tag lesser priority Pods for preemption
// 3. Unless approved, Pods are marked as UnschedulableAndUnresolvable and put back in the scheduling queue
func (cs *Coscheduling) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) *framework.Status {
	pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
	pgKey := pgInfo.key
	if len(pgKey) == 0 {
		return framework.NewStatus(framework.Success, "")
	}
	pgMinAvailable := pgInfo.minAvailable

	if cs.waitingGroup == nil {
		cs.waitingGroup = cs.getNewWaitingGroup()
		if cs.waitingGroup == nil {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable,
				"The highest priority PodGroup does not contain enough pods")
		}
		klog.V(3).Infof("INFO: PodGroup %v is on deck. All other groups will be blocked", cs.waitingGroup.name)
	} else {
		oldName := cs.waitingGroup.name
		if *pod.Spec.Priority > cs.waitingGroup.priority {
			cs.waitingGroup = cs.getNewWaitingGroup()
		} else {
			waitingTime := time.Now().Sub(cs.waitingGroup.creation)
			if waitingTime > time.Duration(PodScheduleTimeout) * time.Second {
				cs.waitingGroup = cs.getNewWaitingGroup()
			}
		}

		if cs.waitingGroup == nil {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable,
				"The highest priority PodGroup is not ready")
		}

		if oldName != cs.waitingGroup.name {
			klog.V(3).Infof("INFO: PodGroup %s has expired. PodGroup %s is now on deck",
				oldName, cs.waitingGroup.name)
		}

		if cs.waitingGroup.name != pod.Labels[PodGroupName] {
			return framework.NewStatus(framework.UnschedulableAndUnresolvable,
				"Pod's group (%v) doesn't match the on-deck PodGroup (%v)",
				pod.Labels[PodGroupName], cs.waitingGroup.name)
		}
	}

	nodesAvailable := cs.calculateAvailableNodes()

	if !cs.waitingGroup.approved && pgMinAvailable > 0{
		if nodesAvailable < pgMinAvailable {
			if cs.waitingGroup.preempting {
				return framework.NewStatus(framework.UnschedulableAndUnresolvable,
					"Determined preemption is in progress")
			} else {
				cs.preemptPods(pgMinAvailable, pgInfo)
				return framework.NewStatus(framework.UnschedulableAndUnresolvable,
					"Determined preemption has been initiated")
			}
		}
	}

	delete(cs.waitingGroup.pods, pod.Name)
	if len(cs.waitingGroup.pods) == 0 {
		cs.waitingGroup = nil
	} else {
		cs.waitingGroup.approved = true
	}

	return framework.NewStatus(framework.Success, "")
}

func (cs *Coscheduling) preemptPods(pgMinAvailable int, pgInfo *PodGroupInfo) {
	klog.V(3).Infof("INFO: Preemption required! Finding preemption candidates")
	podsList := cs.getBoundPods("", "default", true)

	sort.Slice(podsList, func(i, j int) bool {
		if *podsList[i].Spec.Priority == *podsList[j].Spec.Priority {
			return podsList[i].CreationTimestamp.After(podsList[j].CreationTimestamp.Time)
		}
		return *podsList[i].Spec.Priority < *podsList[j].Spec.Priority
	})

	freed := 0
	lastPg := ""
	preemptionCandidates := make([]*v1.Pod, pgMinAvailable)
	for _, p := range podsList {
		if *p.Spec.Priority >= pgInfo.priority {
			break
		} else if lastPg != "" && lastPg != p.Labels[PodGroupName] {
			break
		} else {
			preemptionCandidates[freed] = p
			freed += 1
		}

		if freed >= pgMinAvailable && lastPg == "" {
			lastPg = p.Labels[PodGroupName]
		}
	}

	if freed < pgMinAvailable {
		klog.V(3).Infof("INFO: No preemption occurred. Not enough nodes are able to be freed")
		return
	}

	for _, p := range preemptionCandidates {
		klog.V(3).Infof("INFO: Preempting pod %s", p.Name)
		cs.PreemptionTag(p)
	}

	cs.waitingGroup.preempting = true
}

// PreFilterExtensions returns nil.
func (cs *Coscheduling) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// Permit is the functions invoked by the framework at "Permit" extension point.
func (cs *Coscheduling) Permit(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (*framework.Status, time.Duration) {
	pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
	if len(pgInfo.key) == 0 {
		return framework.NewStatus(framework.Success, ""), 0
	}

	namespace := pod.Namespace
	podGroupName := pgInfo.name
	minAvailable := pgInfo.minAvailable
	// bound includes both assigned and assumed Pods.
	boundPods := cs.getBoundPods(podGroupName, namespace, false)
	bound := len(boundPods)
	// The bound is calculated from the snapshot. The current pod does not exist in the snapshot during this scheduling cycle.
	current := bound + 1

	if current < minAvailable {
		klog.V(3).Infof("The count of podGroup %v/%v/%v is not up to minAvailable(%d) in Permit: current(%d)",
			pod.Namespace, podGroupName, pod.Name, minAvailable, current)
		// TODO Change the timeout to a dynamic value depending on the size of the `PodGroup`
		return framework.NewStatus(framework.Wait, ""), time.Duration(cs.args.PermitWaitingTimeSeconds) * time.Second
	}

	klog.V(3).Infof("The count of PodGroup %v/%v/%v is up to minAvailable(%d) in Permit: current(%d)",
		pod.Namespace, podGroupName, pod.Name, minAvailable, current)
	cs.frameworkHandle.IterateOverWaitingPods(func(waitingPod framework.WaitingPod) {
		if waitingPod.GetPod().Namespace == namespace && waitingPod.GetPod().Labels[PodGroupName] == podGroupName {
			klog.V(3).Infof("Permit allows the pod: %v/%v", podGroupName, waitingPod.GetPod().Name)
			waitingPod.Allow(cs.Name())
		}
	})

	return framework.NewStatus(framework.Success, ""), 0
}

// Unreserve rejects all other Pods in the PodGroup when one of the pods in the group times out.
func (cs *Coscheduling) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	pgInfo, _ := cs.getOrCreatePodGroupInfo(pod, time.Now())
	if len(pgInfo.key) == 0 {
		return
	}
	podGroupName := pgInfo.name
	cs.frameworkHandle.IterateOverWaitingPods(func(waitingPod framework.WaitingPod) {
		if waitingPod.GetPod().Namespace == pod.Namespace && waitingPod.GetPod().Labels[PodGroupName] == podGroupName {
			klog.V(3).Infof("Unreserve rejects the pod: %v/%v", podGroupName, waitingPod.GetPod().Name)
			waitingPod.Reject(cs.Name())
		}
	})
}

// GetPodGroupLabels checks if the pod belongs to a PodGroup. If so, it will return the
// podGroupName, minAvailable of the PodGroup. If not, it will return "" and 0.
func GetPodGroupLabels(pod *v1.Pod) (string, int, error) {
	podGroupName, exist := pod.Labels[PodGroupName]
	if !exist || len(podGroupName) == 0 {
		return "", 0, nil
	}
	minAvailable, exist := pod.Labels[PodGroupMinAvailable]
	if !exist || len(minAvailable) == 0 {
		return "", 0, nil
	}
	minNum, err := strconv.Atoi(minAvailable)
	if err != nil {
		klog.Errorf("PodGroup %v/%v : PodGroupMinAvailable %v is invalid", pod.Namespace, pod.Name, minAvailable)
		return "", 0, err
	}
	if minNum < 1 {
		klog.Errorf("PodGroup %v/%v : PodGroupMinAvailable %v is less than 1", pod.Namespace, pod.Name, minAvailable)
		return "", 0, err
	}
	return podGroupName, minNum, nil
}

func (cs *Coscheduling) calculateAvailableNodes() int {
	assignedNodes := map[string]bool{}
	podsList := cs.getBoundPods("", "default", false)
	for _, pod := range podsList {
		assignedNodes[pod.Spec.NodeName] = true
	}

	nodesAvailable := 0
	for _, node := range cs.getAllNodes() {
		if _, ok := assignedNodes[node.Node().Name]; !ok {
			nodesAvailable += 1
		}
	}
	return nodesAvailable
}

func (cs *Coscheduling) calculateTotalPods(podGroupName, namespace string) int {
	// TODO get the total pods from the scheduler cache and queue instead of the hack manner.
	selector := labels.Set{PodGroupName: podGroupName}.AsSelector()
	pods, err := cs.podLister.Pods(namespace).List(selector)
	if err != nil {
		klog.Error(err)
		return 0
	}
	return len(pods)
}

func (cs *Coscheduling) getNewWaitingGroup() *WaitingGroup {
	podsList := cs.getWaitingPods("default")
	if podsList == nil || len(podsList.Items) == 0 {
		return nil
	}
	sort.Slice(podsList.Items, func(i, j int) bool {
		if *podsList.Items[i].Spec.Priority == *podsList.Items[j].Spec.Priority {
			return podsList.Items[j].CreationTimestamp.After(podsList.Items[i].CreationTimestamp.Time)
		}
		return *podsList.Items[i].Spec.Priority > *podsList.Items[j].Spec.Priority
	})
	candidate := podsList.Items[0]
	pgName, minAvailable, _ := GetPodGroupLabels(&candidate)
	pgPods := make(map[string]bool, minAvailable)

	for _, p := range podsList.Items {
		candidateGroup, exist := p.Labels[PodGroupName]
		if exist && candidateGroup == pgName {
			pgPods[p.Name] = true
		}
	}

	if len(pgPods) < minAvailable {
		return nil
	}

	return &WaitingGroup{
		name:       pgName,
		pods:       pgPods,
		preempting: false,
		approved:   false,
		creation:   time.Now(),
		priority:   *candidate.Spec.Priority,
	}
}

func (cs *Coscheduling)getBoundPods(podGroupName, namespace string, determined bool) []*v1.Pod {
	var pods []*v1.Pod
	var err error
	if podGroupName == "" {
		pods, err = cs.frameworkHandle.SnapshotSharedLister().Pods().FilteredList(func(pod *v1.Pod) bool {
			ok := true
			if determined {
				_, ok = pod.Labels["determined"]
			}
			if ok && pod.Namespace == namespace && pod.Spec.NodeName != "" {
				return true
			}
			return false
		}, labels.NewSelector())
	} else {
		pods, err = cs.frameworkHandle.SnapshotSharedLister().Pods().FilteredList(func(pod *v1.Pod) bool {
			if pod.Labels[PodGroupName] == podGroupName && pod.Namespace == namespace &&
				pod.Spec.NodeName != "" {
				return true
			}
			return false
		}, labels.NewSelector())
	}

	if err != nil {
		klog.Error(err)
		return nil
	}

	return pods
}

func (cs *Coscheduling) getWaitingPods(namespace string) *v1.PodList {
	pods, err := cs.clientSet.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
			LabelSelector: "determined",
			FieldSelector: "status.phase=Pending",
		})

	if err != nil {
		klog.Error(err)
		return nil
	}

	return pods
}

func (cs *Coscheduling) expExists(namespace string, name string) bool {
	pods := cs.getWaitingPods(namespace)
	if pods == nil {
		return false
	}

	for _, pod := range pods.Items {
		if pod.Labels[PodGroupName] == name {
			return true
		}
	}
	return false
}

func (cs *Coscheduling) getAllNodes() []*schedulernodeinfo.NodeInfo {
	nodes, err := cs.frameworkHandle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		klog.Error(err)
		return nil
	}
	return nodes
}

// markPodGroupAsExpired set the deletionTimestamp of PodGroup to mark PodGroup as expired.
func (cs *Coscheduling) markPodGroupAsExpired(obj interface{}) {
	pod := obj.(*v1.Pod)
	podGroupName, podMinAvailable, _ := GetPodGroupLabels(pod)
	if len(podGroupName) == 0 || podMinAvailable == 0 {
		return
	}

	pgKey := fmt.Sprintf("%v/%v", pod.Namespace, podGroupName)
	// If it's a PodGroup and present in PodGroupInfos, set its deletionTimestamp.
	value, exist := cs.podGroupInfos.Load(pgKey)
	if !exist {
		return
	}
	pgInfo := value.(*PodGroupInfo)
	if pgInfo.deletionTimestamp == nil {
		now := cs.clock.Now()
		pgInfo.deletionTimestamp = &now
		cs.podGroupInfos.Store(pgKey, pgInfo)
	}
}

// responsibleForPod selects pod that belongs to a PodGroup.
func responsibleForPod(pod *v1.Pod) bool {
	podGroupName, podMinAvailable, _ := GetPodGroupLabels(pod)
	if len(podGroupName) == 0 || podMinAvailable == 0 {
		return false
	}
	return true
}

func (cs *Coscheduling) podGroupInfoGC() {
	cs.podGroupInfos.Range(func(key, value interface{}) bool {
		pgInfo := value.(*PodGroupInfo)
		if pgInfo.deletionTimestamp != nil && pgInfo.deletionTimestamp.Add(time.Duration(cs.args.PodGroupExpirationTimeSeconds)*time.Second).Before(cs.clock.Now()) {
			klog.V(3).Infof("%v is out of date and has been deleted in PodGroup GC", key)
			cs.podGroupInfos.Delete(key)
		}
		return true
	})
}
