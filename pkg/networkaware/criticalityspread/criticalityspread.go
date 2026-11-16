/*
Copyright 2026 The Kubernetes Authors.

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

// Package criticalityspread implements a Score plugin that spreads the pods of
// critical AppGroup workloads across zones and nodes, and keeps critical
// workloads off the same node, so that one node or zone failure breaks fewer
// requests.
//
// A workload's criticality combines its centrality in the AppGroup dependency
// graph, the entropy of its dependency bandwidths and an operator-declared
// criticality label, as in the Centrality-Entropy Index (CEI) of Pokharel,
// "Governance-Aware Dynamic Resource Allocation" (FTC 2026).
package criticalityspread

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pluginconfig "sigs.k8s.io/scheduler-plugins/apis/config"
	"sigs.k8s.io/scheduler-plugins/apis/config/validation"
	networkawareutil "sigs.k8s.io/scheduler-plugins/pkg/networkaware/util"
	"sigs.k8s.io/scheduler-plugins/pkg/util"

	agv1alpha1 "github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "CriticalitySpread"

	// CriticalityLabel is the pod label an operator sets to declare how critical
	// a workload is, from 0 (not critical) to 100.
	CriticalityLabel = "scheduling.x-k8s.io/criticality"

	preScoreStateKey = "PreScore" + Name

	// A replica of the same workload already in the node's zone costs twice as
	// much as one already on the node, so zones are spread before nodes.
	zoneSpreadCost = 2.0
	nodeSpreadCost = 1.0
)

var _ fwk.PreScorePlugin = &CriticalitySpread{}
var _ fwk.ScorePlugin = &CriticalitySpread{}

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(agv1alpha1.AddToScheme(scheme))
}

// weights are the criticality weights, normalised to sum to 1.
type weights struct {
	centrality, entropy, governance float64
}

type cachedScores struct {
	resourceVersion string
	scores          map[string]graphScores
}

// CriticalitySpread scores nodes for pods that belong to an AppGroup.
type CriticalitySpread struct {
	client.Client
	logger     klog.Logger
	handle     fwk.Handle
	namespaces []string
	weights    weights

	mu    sync.Mutex
	cache map[types.UID]cachedScores
}

// preScoreState is computed in PreScore and used by Score and NormalizeScore.
type preScoreState struct {
	// criticality of the pod's workload, in [0, 1].
	criticality float64
	// cost of each candidate node: replicas of the same workload already in its
	// zone and on it, plus the criticality of other AppGroup pods on it.
	cost map[string]float64
}

// Clone the preScore state.
func (s *preScoreState) Clone() fwk.StateData {
	return s
}

// Name returns the name of the plugin.
func (cs *CriticalitySpread) Name() string {
	return Name
}

// New creates a CriticalitySpread plugin.
func New(ctx context.Context, obj runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
	logger := klog.FromContext(ctx).WithValues("plugin", Name)
	logger.V(4).Info("Creating new instance of the CriticalitySpread plugin")

	args, ok := obj.(*pluginconfig.CriticalitySpreadArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type CriticalitySpreadArgs, got %T", obj)
	}
	if err := validation.ValidateCriticalitySpreadArgs(nil, args); err != nil {
		return nil, err
	}
	c, _, err := util.NewClientWithCachedReader(ctx, handle.KubeConfig(), scheme)
	if err != nil {
		return nil, err
	}
	return &CriticalitySpread{
		Client:     c,
		logger:     logger,
		handle:     handle,
		namespaces: args.Namespaces,
		weights:    newWeights(args.CentralityWeight, args.EntropyWeight, args.GovernanceWeight),
		cache:      map[types.UID]cachedScores{},
	}, nil
}

func newWeights(centrality, entropy, governance int64) weights {
	sum := float64(centrality + entropy + governance)
	return weights{
		centrality: float64(centrality) / sum,
		entropy:    float64(entropy) / sum,
		governance: float64(governance) / sum,
	}
}

// PreScore counts, for every candidate node, the replicas of the pod's workload
// already in its zone and on it, and the criticality of the other AppGroup pods
// already on it. Pods outside an AppGroup, and workloads with no criticality,
// skip this plugin.
func (cs *CriticalitySpread) PreScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	agName := networkawareutil.GetPodAppGroupLabel(pod)
	selector := networkawareutil.GetPodAppGroupSelector(pod)
	if agName == "" || selector == "" {
		return fwk.NewStatus(fwk.Skip)
	}
	scores := cs.appGroupScores(ctx, pod.Namespace, agName)
	if scores == nil {
		return fwk.NewStatus(fwk.Skip)
	}
	own := cs.criticality(scores, pod)
	if own <= 0 {
		return fwk.NewStatus(fwk.Skip)
	}

	all, err := cs.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return fwk.AsStatus(fmt.Errorf("listing nodes: %w", err))
	}
	zoneReplicas := map[string]float64{}
	nodeCost := make(map[string]float64, len(all))
	for _, nodeInfo := range all {
		node := nodeInfo.Node()
		if node == nil {
			continue
		}
		for _, podInfo := range nodeInfo.GetPods() {
			p := podInfo.GetPod()
			if p.Namespace != pod.Namespace || networkawareutil.GetPodAppGroupLabel(p) != agName {
				continue
			}
			if networkawareutil.GetPodAppGroupSelector(p) == selector {
				zoneReplicas[zoneOf(node)]++
				nodeCost[node.Name] += nodeSpreadCost
			} else {
				nodeCost[node.Name] += cs.criticality(scores, p)
			}
		}
	}

	cost := make(map[string]float64, len(nodes))
	for _, nodeInfo := range nodes {
		node := nodeInfo.Node()
		if node == nil {
			continue
		}
		cost[node.Name] = zoneSpreadCost*zoneReplicas[zoneOf(node)] + nodeCost[node.Name]
	}
	state.Write(preScoreStateKey, &preScoreState{criticality: own, cost: cost})
	return nil
}

// Score returns the node's spreading cost (scaled by 100). NormalizeScore turns
// costs into scores, so lower costs score higher.
func (cs *CriticalitySpread) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	s, err := getPreScoreState(state)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}
	return int64(math.Round(100 * s.cost[nodeInfo.Node().Name])), nil
}

// ScoreExtensions returns the ScoreExtensions interface.
func (cs *CriticalitySpread) ScoreExtensions() fwk.ScoreExtensions {
	return cs
}

// NormalizeScore maps costs to [0, MaxNodeScore], cheapest node highest, and
// scales the spread by the workload's criticality: a critical workload gets a
// strong preference, a leaf workload an almost flat one.
func (cs *CriticalitySpread) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	s, err := getPreScoreState(state)
	if err != nil {
		return fwk.AsStatus(err)
	}
	if len(scores) == 0 {
		return nil
	}
	minCost, maxCost := scores[0].Score, scores[0].Score
	for _, ns := range scores {
		minCost = min(minCost, ns.Score)
		maxCost = max(maxCost, ns.Score)
	}
	for i := range scores {
		penalty := 0.0
		if maxCost > minCost {
			penalty = s.criticality * float64(scores[i].Score-minCost) / float64(maxCost-minCost)
		}
		scores[i].Score = fwk.MaxNodeScore - int64(math.Round(float64(fwk.MaxNodeScore)*penalty))
	}
	return nil
}

// criticality of a pod's workload: the weighted graph scores plus the declared label.
func (cs *CriticalitySpread) criticality(scores map[string]graphScores, pod *v1.Pod) float64 {
	gs := scores[networkawareutil.GetPodAppGroupSelector(pod)]
	return cs.weights.centrality*gs.Centrality +
		cs.weights.entropy*gs.Entropy +
		cs.weights.governance*declaredCriticality(pod)
}

// declaredCriticality reads CriticalityLabel as an integer from 0 to 100 and
// returns it in [0, 1]. A missing or invalid label counts as 0.
func declaredCriticality(pod *v1.Pod) float64 {
	v, err := strconv.Atoi(pod.Labels[CriticalityLabel])
	if err != nil {
		return 0
	}
	return math.Min(100, math.Max(0, float64(v))) / 100
}

// appGroupScores returns the graph scores of the named AppGroup, recomputing
// them only when the AppGroup changes.
func (cs *CriticalitySpread) appGroupScores(ctx context.Context, namespace, agName string) map[string]graphScores {
	ag := cs.findAppGroup(ctx, namespace, agName)
	if ag == nil {
		return nil
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cached, ok := cs.cache[ag.UID]; ok && cached.resourceVersion == ag.ResourceVersion {
		return cached.scores
	}
	scores := newWorkloadGraph(ag).scores()
	cs.cache[ag.UID] = cachedScores{resourceVersion: ag.ResourceVersion, scores: scores}
	return scores
}

// findAppGroup looks in the pod's namespace first, then in the configured namespaces.
func (cs *CriticalitySpread) findAppGroup(ctx context.Context, namespace, agName string) *agv1alpha1.AppGroup {
	for _, ns := range append([]string{namespace}, cs.namespaces...) {
		ag := &agv1alpha1.AppGroup{}
		if err := cs.Get(ctx, client.ObjectKey{Namespace: ns, Name: agName}, ag); err == nil {
			return ag
		}
	}
	cs.logger.V(4).Info("AppGroup not found", "appGroup", agName, "namespace", namespace)
	return nil
}

// zoneOf returns the node's zone label, or its name when it has none, so a
// cluster without zones is spread by node only.
func zoneOf(node *v1.Node) string {
	if zone := networkawareutil.GetNodeZone(node); zone != "" {
		return zone
	}
	return node.Name
}

func getPreScoreState(state fwk.CycleState) (*preScoreState, error) {
	c, err := state.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("reading %q from cycleState: %w", preScoreStateKey, err)
	}
	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("%+v cannot be converted to criticalityspread.preScoreState", c)
	}
	return s, nil
}
