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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"sort"
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
	"k8s.io/kubernetes/pkg/scheduler/util"
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
	// key is <namespace>/<PodGroup name> and value is *PodGroupInfo.
	podGroupInfos sync.Map
	// clock is used to get the current time.
	clock util.Clock
	// args is coscheduling parameters.
	args Args
	// waitingPods is used to track what Pods are currently being scheduled.
	waitingGroups map[string]*waitingGroup
	// If a podgroup has not been encountered, we refresh our cached information
	encounteredGroups map[string]bool
	// creation holds the timestamp the group was last updated. The plugin will check
	// the status of all podgroups after PodScheduleTimeout seconds. If a PodGroup
	// has been deleted while it is waiting to be scheduled, it will be replaced
	creation time.Time
	noFit bool
}

type waitingGroup struct {
	name        string
	pods        map[string]bool
	preempting  bool
	approved    bool
	priority    int32
	tolerations []v1.Toleration
	selector    map[string]string
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
	PodScheduleTimeout = 1
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

	podLister := handle.SharedInformerFactory().Core().V1().Pods().Lister()
	cs := &Coscheduling{frameworkHandle: handle,
		podLister:     podLister,
		clock:         util.RealClock{},
		args:          args,
		waitingGroups: map[string]*waitingGroup{},
		encounteredGroups: map[string]bool{},
		creation:      time.Now(),
		noFit: 		   false,
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

	_, err := cs.frameworkHandle.ClientSet().CoreV1().Pods("default").Patch(
		context.TODO(), pod.Name, types.JSONPatchType, payloadBytes, metav1.PatchOptions{})
	if err == nil {
		klog.V(3).Infof("Tagged pod %v for preemption", pod.Name)
	} else {
		klog.V(3).Infof("Unable to tag pod %v for preemption", pod.Name)
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

	waitingTime := time.Now().Sub(cs.creation)
	if _, ok := cs.encounteredGroups[pgInfo.name]; !ok {
		klog.V(9).Infof("encountered a new pod group, refreshing info")
		cs.getNewWaitingGroups()
	} else if waitingTime > time.Duration(PodScheduleTimeout)*time.Second {
		klog.V(9).Infof("waiting groups timed out, refreshing info")
		cs.getNewWaitingGroups()
	}

	//TODO: turn this into an if else statement
	// add a variable that takes into account whether the highest priority job was scheduled or not
	// if the variable is true, then allow through every pod and reset if it is able to schedule the
	// waiting group. They will be preempted if necessary anyways.
	if _, ok := cs.waitingGroups[pgInfo.name]; ok {
		group := cs.waitingGroups[pgInfo.name]

		nodesAvailable := cs.calculateAvailableNodes(pgInfo.name)

		if !group.approved && pgMinAvailable > 0 {
			if nodesAvailable < pgMinAvailable {
				if group.preempting {
					return framework.NewStatus(framework.UnschedulableAndUnresolvable,
						"Waiting for lower priority pods to be preempted")
				} else {
					cs.preemptPods(pgInfo.name)
					return framework.NewStatus(framework.UnschedulableAndUnresolvable, "")
				}
			}
		}

		delete(group.pods, pod.Name)
		if len(group.pods) == 0 {
			delete(cs.waitingGroups, pgInfo.name)
			cs.encounteredGroups = map[string]bool{}
		} else {
			group.approved = true
		}

		klog.V(3).Infof("Pod %v in group %v has been approved", pod.Name, group.name)
		return framework.NewStatus(framework.Success, "")
	} else if cs.noFit {
		klog.V(3).Infof("We are backfilling with pod %v!", pod.Name)
		return framework.NewStatus(framework.Success, "")
	}

	return framework.NewStatus(framework.UnschedulableAndUnresolvable,
		"PodGroup rejected because it is not ready or higher priority PodGroups exist.")
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

func (cs *Coscheduling) getNewWaitingGroups() {
	// updates cs.waitingGroups to everything that is still alive
	podsList := cs.getWaitingPods("default")
	if podsList == nil || len(podsList.Items) == 0 {
		cs.waitingGroups = map[string]*waitingGroup{}
	}
	sort.Slice(podsList.Items, func(i, j int) bool {
		if *podsList.Items[i].Spec.Priority == *podsList.Items[j].Spec.Priority {
			pgNamei, oki := podsList.Items[i].Labels[PodGroupName]
			pgNamej, okj := podsList.Items[j].Labels[PodGroupName]
			if !oki && !okj {
				return podsList.Items[j].CreationTimestamp.After(podsList.Items[i].CreationTimestamp.Time)
			} else if !oki {
				return true
			} else if !okj {
				return false
			}

			return pgNamei < pgNamej
		}
		return *podsList.Items[i].Spec.Priority > *podsList.Items[j].Spec.Priority
	})

	cs.waitingGroups = map[string]*waitingGroup{}
	cs.encounteredGroups = map[string]bool{}
	var encounteredSelectors []map[string]string
	candidate := podsList.Items[0]
	pgName, minAvailable, _ := GetPodGroupLabels(&candidate)
	pgPods := make(map[string]bool, minAvailable)
	skip := false

	for _, p := range podsList.Items {
		skip = false
		candidateGroup, exist := p.Labels[PodGroupName]
		if exist && candidateGroup == pgName {
			pgPods[p.Name] = true
		} else if exist && candidateGroup != pgName {
			if len(encounteredSelectors) > 0 {
				for _, selector := range encounteredSelectors {
					if cs.compareSelectors(candidate.Spec.NodeSelector, selector) {
						skip = true
						break
					}
				}
			}
			if len(pgPods) == minAvailable && !skip {
				cs.waitingGroups[pgName] = &waitingGroup{
					name:        pgName,
					pods:        pgPods,
					preempting:  false,
					approved:    false,
					priority:    *candidate.Spec.Priority,
					tolerations: candidate.Spec.Tolerations,
					selector:    candidate.Spec.NodeSelector,
				}
				encounteredSelectors = append(encounteredSelectors, candidate.Spec.NodeSelector)
			}
			cs.encounteredGroups[pgName] = true
			candidate = p
			pgName, minAvailable, _ = GetPodGroupLabels(&candidate)
			pgPods = make(map[string]bool, minAvailable)
			pgPods[p.Name] = true
		}
	}

	if len(encounteredSelectors) > 0 { // if there are previous tolerations, check if they match
		skip = false
		for _, selector := range encounteredSelectors {
			if cs.compareSelectors(candidate.Spec.NodeSelector, selector) {
				skip = true // skip adding it to the wait groups
				break
			}
		}

	}
	if len(pgPods) == minAvailable && !skip { //if length pods is not min available, we aren't ready. If skip, tolerations don't match
		cs.waitingGroups[pgName] = &waitingGroup{
			name:        pgName,
			pods:        pgPods,
			preempting:  false,
			approved:    false,
			priority:    *candidate.Spec.Priority,
			tolerations: candidate.Spec.Tolerations,
			selector:    candidate.Spec.NodeSelector,
		}
	}
	cs.encounteredGroups[pgName] = true

	cs.creation = cs.clock.Now()
}

func (cs *Coscheduling) getBoundPods(podGroupName, namespace string, determined bool) []*v1.Pod {
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
	pods, err := cs.frameworkHandle.ClientSet().CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: "determined",
		FieldSelector: "status.phase=Pending",
	})

	if err != nil {
		klog.Error(err)
		return nil
	}

	return pods
}

func (cs *Coscheduling) getAllNodes() []*schedulernodeinfo.NodeInfo {
	nodes, err := cs.frameworkHandle.SnapshotSharedLister().NodeInfos().List()

	if err != nil {
		klog.Error(err)
		return nil
	}
	return nodes
}

func (cs *Coscheduling) compareSelectors(s1, s2 map[string]string) bool {
	if s1 == nil && s2 == nil {
		return true
	}
	if s1 == nil || s2 == nil {
		return false
	}

	if len(s1) != len(s2) {
		return false
	}

	for k, v := range s1 {
		v2, ok := s2[k]
		if !ok {
			return false
		}
		if v != v2 {
			return false
		}
	}
	return true
}

func (cs *Coscheduling) doesTolerate(tolerations []v1.Toleration, taints []v1.Taint) bool {
	tolerationMap := map[int]v1.Toleration{}
	for i, toleration := range tolerations {
		tolerationMap[i] = toleration
	}

	for _, taint := range taints {
		found := -1
		if len(tolerationMap) == 0 {
			return false
		}
		for k, t := range tolerationMap {
			if t.ToleratesTaint(&taint) {
				found = k
				break
			}
		}
		if found < 0 { // if the taint can't find a matching toleration
			return false
		}
		delete(tolerationMap, found)
	}
	return true
}

func (cs *Coscheduling) preemptPods(podgroup string) {
	klog.V(3).Infof("Preemption required! Finding preemption candidates")
	podsList := cs.getBoundPods("", "default", true)
	group := cs.waitingGroups[podgroup]
	pgMinAvailable := len(group.pods)

	sort.Slice(podsList, func(i, j int) bool {
		if *podsList[i].Spec.Priority == *podsList[j].Spec.Priority {
			pgNamei, oki := podsList[i].Labels[PodGroupName]
			pgNamej, okj := podsList[j].Labels[PodGroupName]
			if !oki && !okj {
				return podsList[j].CreationTimestamp.After(podsList[i].CreationTimestamp.Time)
			} else if !oki {
				return true
			} else if !okj {
				return false
			}

			return pgNamei < pgNamej
		}
		return *podsList[i].Spec.Priority < *podsList[j].Spec.Priority
	})

	freed := 0
	lastPg := ""
	preemptionCandidates := map[int]*v1.Pod{}
	for _, p := range podsList {
		if *p.Spec.Priority >= group.priority {
			break
		} else if lastPg != "" && lastPg != p.Labels[PodGroupName] {
			break
		} else {
			if cs.compareSelectors(group.selector, p.Spec.NodeSelector) {
				preemptionCandidates[freed] = p
				freed += 1
			}
		}

		if freed >= pgMinAvailable && lastPg == "" {
			lastPg = p.Labels[PodGroupName]
		}
	}

	if freed < pgMinAvailable {
		klog.V(3).Infof("No preemption occurred. Not enough nodes are able to be freed")
		cs.noFit = true
		return
	}

	for _, p := range preemptionCandidates {
		cs.PreemptionTag(p)
	}

	klog.V(3).Infof("Preemption of lower priority pods is in progress")
	cs.noFit = false
	group.preempting = true
}

func (cs *Coscheduling) calculateAvailableNodes(podgroup string) int {
	assignedNodes := map[string]bool{}
	podsList := cs.getBoundPods("", "default", false)
	for _, pod := range podsList {
		assignedNodes[pod.Spec.NodeName] = true
	}

	nodesAvailable := 0
	for _, node := range cs.getAllNodes() {
		failedSelector := false
		for k, v := range cs.waitingGroups[podgroup].selector {
			v2, ok := node.Node().Labels[k]
			if !ok || v != v2 {
				klog.V(9).Infof("NODE %v doesn't have the right label\n", node.Node().Name)
				failedSelector = true
				break
			}
		}
		if failedSelector {
			continue
		}

		taints, err := node.Taints()
		if err != nil {
			continue
		}
		if !cs.doesTolerate(cs.waitingGroups[podgroup].tolerations, taints) {
			klog.V(9).Infof("NODE %v doesn't fit\n", node.Node().Name)
			continue
		}
		// if it does tolerate and there is not an assigned node to it, we're ok.
		if _, ok := assignedNodes[node.Node().Name]; !ok {
			klog.V(9).Infof("NODE fits:", node.Node().Name)
			nodesAvailable += 1
		}
	}
	return nodesAvailable
}
