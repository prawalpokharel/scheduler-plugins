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

package criticalityspread

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	testClientSet "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	schedruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	tf "k8s.io/kubernetes/pkg/scheduler/testing/framework"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	agv1alpha1 "github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
)

var _ fwk.SharedLister = &testSharedLister{}

type testSharedLister struct {
	nodeInfos   []fwk.NodeInfo
	nodeInfoMap map[string]fwk.NodeInfo
}

func newTestSharedLister(nodeInfos []fwk.NodeInfo) *testSharedLister {
	m := map[string]fwk.NodeInfo{}
	for _, ni := range nodeInfos {
		m[ni.Node().Name] = ni
	}
	return &testSharedLister{nodeInfos: nodeInfos, nodeInfoMap: m}
}

func (f *testSharedLister) StorageInfos() fwk.StorageInfoLister     { return nil }
func (f *testSharedLister) PodGroupStates() fwk.PodGroupStateLister { return nil }
func (f *testSharedLister) NodeInfos() fwk.NodeInfoLister           { return f }
func (f *testSharedLister) List() ([]fwk.NodeInfo, error)           { return f.nodeInfos, nil }
func (f *testSharedLister) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return nil, nil
}
func (f *testSharedLister) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nil, nil
}
func (f *testSharedLister) Get(nodeName string) (fwk.NodeInfo, error) {
	return f.nodeInfoMap[nodeName], nil
}

// appGroup builds an AppGroup from workload -> dependencies, with an optional
// bandwidth per edge (default "100Mi").
func appGroup(name string, deps map[string][]string, bandwidth map[[2]string]string) *agv1alpha1.AppGroup {
	ag := &agv1alpha1.AppGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name)},
	}
	for _, w := range sortedKeys(deps) {
		wl := agv1alpha1.AppGroupWorkload{
			Workload: agv1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: w, Selector: w, APIVersion: "apps/v1", Namespace: "default"},
		}
		for _, d := range deps[w] {
			bw := "100Mi"
			if b, ok := bandwidth[[2]string{w, d}]; ok {
				bw = b
			}
			wl.Dependencies = append(wl.Dependencies, agv1alpha1.DependenciesInfo{
				Workload:     agv1alpha1.AppGroupWorkloadInfo{Kind: "Deployment", Name: d, Selector: d, APIVersion: "apps/v1", Namespace: "default"},
				MinBandwidth: resource.MustParse(bw),
			})
		}
		ag.Spec.Workloads = append(ag.Spec.Workloads, wl)
	}
	return ag
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func makePod(name, ag, workload, criticality, node string) *v1.Pod {
	b := st.MakePod().Name(name).Namespace("default").UID(name)
	if ag != "" {
		b = b.Label(agv1alpha1.AppGroupLabel, ag)
	}
	if workload != "" {
		b = b.Label(agv1alpha1.AppGroupSelectorLabel, workload)
	}
	if criticality != "" {
		b = b.Label(CriticalityLabel, criticality)
	}
	if node != "" {
		b = b.Node(node)
	}
	return b.Obj()
}

func makeNode(name, zone string) *v1.Node {
	return st.MakeNode().Name(name).Label(v1.LabelTopologyZone, zone).Capacity(
		map[v1.ResourceName]string{v1.ResourceCPU: "8", v1.ResourceMemory: "16Gi"}).Obj()
}

func newPlugin(t testing.TB, ag *agv1alpha1.AppGroup, w weights, nodes []*v1.Node, pods []*v1.Pod) (*CriticalitySpread, []fwk.NodeInfo) {
	t.Helper()
	s := clientgoscheme.Scheme
	utilruntime.Must(agv1alpha1.AddToScheme(s))
	builder := fake.NewClientBuilder().WithScheme(s)
	if ag != nil {
		builder = builder.WithObjects(ag)
	}
	ctx := context.Background()
	cs := testClientSet.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(cs, 0)

	byNode := map[string][]*v1.Pod{}
	for _, p := range pods {
		byNode[p.Spec.NodeName] = append(byNode[p.Spec.NodeName], p)
	}
	var nodeInfos []fwk.NodeInfo
	for _, n := range nodes {
		ni := framework.NewNodeInfo(byNode[n.Name]...)
		ni.SetNode(n)
		nodeInfos = append(nodeInfos, ni)
	}
	registered := []tf.RegisterPluginFunc{
		tf.RegisterBindPlugin(defaultbinder.Name, defaultbinder.New),
		tf.RegisterQueueSortPlugin(queuesort.Name, queuesort.New),
	}
	fh, err := tf.NewFramework(ctx, registered, "default-scheduler", schedruntime.WithClientSet(cs),
		schedruntime.WithInformerFactory(informerFactory), schedruntime.WithSnapshotSharedLister(newTestSharedLister(nodeInfos)))
	if err != nil {
		t.Fatal(err)
	}
	return &CriticalitySpread{
		Client:     builder.Build(),
		logger:     klog.Background(),
		handle:     fh,
		namespaces: []string{"default"},
		weights:    w,
		cache:      map[types.UID]cachedScores{},
	}, nodeInfos
}

// scoreAll runs PreScore, Score and NormalizeScore the way the framework does.
func scoreAll(t *testing.T, pl *CriticalitySpread, pod *v1.Pod, nodeInfos []fwk.NodeInfo) (fwk.NodeScoreList, *fwk.Status) {
	t.Helper()
	ctx := context.Background()
	state := framework.NewCycleState()
	if status := pl.PreScore(ctx, state, pod, nodeInfos); !status.IsSuccess() {
		return nil, status
	}
	var scores fwk.NodeScoreList
	for _, ni := range nodeInfos {
		s, status := pl.Score(ctx, state, pod, ni)
		if !status.IsSuccess() {
			t.Fatalf("Score: %v", status)
		}
		scores = append(scores, fwk.NodeScore{Name: ni.Node().Name, Score: s})
	}
	if status := pl.NormalizeScore(ctx, state, pod, scores); !status.IsSuccess() {
		t.Fatalf("NormalizeScore: %v", status)
	}
	return scores, nil
}

var shop = map[string][]string{
	"frontend": {"cart", "catalog"},
	"cart":     {"redis"},
	"catalog":  nil,
	"redis":    nil,
}

func TestDeclaredCriticality(t *testing.T) {
	for label, want := range map[string]float64{"100": 1, "50": 0.5, "0": 0, "-5": 0, "250": 1, "high": 0, "": 0} {
		pod := makePod("p", "shop", "frontend", label, "")
		if got := declaredCriticality(pod); got != want {
			t.Errorf("label %q: got %v, want %v", label, got, want)
		}
	}
}

func TestNewWeights(t *testing.T) {
	got := newWeights(40, 35, 25)
	want := weights{centrality: 0.4, entropy: 0.35, governance: 0.25}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if got := newWeights(0, 0, 3); got.governance != 1 {
		t.Errorf("governance-only weights: got %+v", got)
	}
}

func TestPreScoreSkips(t *testing.T) {
	nodes := []*v1.Node{makeNode("n1", "z1"), makeNode("n2", "z2")}
	governanceOnly := weights{governance: 1}
	tests := []struct {
		name string
		ag   *agv1alpha1.AppGroup
		pod  *v1.Pod
	}{
		{"pod outside any AppGroup", appGroup("shop", shop, nil), makePod("p", "", "", "100", "")},
		{"AppGroup not found", nil, makePod("p", "shop", "frontend", "100", "")},
		{"workload with no criticality", appGroup("shop", shop, nil), makePod("p", "shop", "catalog", "0", "")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl, nodeInfos := newPlugin(t, tt.ag, governanceOnly, nodes, nil)
			_, status := scoreAll(t, pl, tt.pod, nodeInfos)
			if status.Code() != fwk.Skip {
				t.Errorf("want Skip, got %v", status)
			}
		})
	}
}

func TestScoreSpreadsReplicasByZoneThenNode(t *testing.T) {
	nodes := []*v1.Node{makeNode("n1", "z1"), makeNode("n2", "z1"), makeNode("n3", "z2")}
	existing := []*v1.Pod{makePod("fe-0", "shop", "frontend", "100", "n1")}
	tests := []struct {
		name        string
		criticality string
		want        fwk.NodeScoreList
	}{
		{
			// costs: n1 = 2 (zone) + 1 (node) = 3, n2 = 2, n3 = 0
			name:        "critical workload gets the full spread",
			criticality: "100",
			want:        fwk.NodeScoreList{{Name: "n1", Score: 0}, {Name: "n2", Score: 33}, {Name: "n3", Score: 100}},
		},
		{
			name:        "low-criticality workload gets a nearly flat score",
			criticality: "10",
			want:        fwk.NodeScoreList{{Name: "n1", Score: 90}, {Name: "n2", Score: 93}, {Name: "n3", Score: 100}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl, nodeInfos := newPlugin(t, appGroup("shop", shop, nil), weights{governance: 1}, nodes, existing)
			got, status := scoreAll(t, pl, makePod("fe-1", "shop", "frontend", tt.criticality, ""), nodeInfos)
			if status != nil {
				t.Fatalf("unexpected status %v", status)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScoreKeepsCriticalWorkloadsApart(t *testing.T) {
	nodes := []*v1.Node{makeNode("n1", "z1"), makeNode("n2", "z2"), makeNode("n3", "z3")}
	existing := []*v1.Pod{
		makePod("cart-0", "shop", "cart", "100", "n1"),
		makePod("catalog-0", "shop", "catalog", "50", "n2"),
		makePod("other-0", "", "", "100", "n3"), // not in the AppGroup: ignored
	}
	pl, nodeInfos := newPlugin(t, appGroup("shop", shop, nil), weights{governance: 1}, nodes, existing)
	got, status := scoreAll(t, pl, makePod("fe-0", "shop", "frontend", "100", ""), nodeInfos)
	if status != nil {
		t.Fatalf("unexpected status %v", status)
	}
	// costs: n1 = 1.0 (cart), n2 = 0.5 (catalog), n3 = 0
	want := fwk.NodeScoreList{{Name: "n1", Score: 0}, {Name: "n2", Score: 50}, {Name: "n3", Score: 100}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestScoreIgnoresOtherNamespacesAndAppGroups(t *testing.T) {
	nodes := []*v1.Node{makeNode("n1", "z1"), makeNode("n2", "z2")}
	otherNamespace := makePod("fe-x", "shop", "frontend", "100", "n1")
	otherNamespace.Namespace = "elsewhere"
	existing := []*v1.Pod{otherNamespace, makePod("fe-y", "other-app", "frontend", "100", "n1")}
	pl, nodeInfos := newPlugin(t, appGroup("shop", shop, nil), weights{governance: 1}, nodes, existing)
	got, status := scoreAll(t, pl, makePod("fe-0", "shop", "frontend", "100", ""), nodeInfos)
	if status != nil {
		t.Fatalf("unexpected status %v", status)
	}
	want := fwk.NodeScoreList{{Name: "n1", Score: 100}, {Name: "n2", Score: 100}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestScoreWithoutZoneLabelsSpreadsByNode(t *testing.T) {
	n1 := st.MakeNode().Name("n1").Obj()
	n2 := st.MakeNode().Name("n2").Obj()
	existing := []*v1.Pod{makePod("fe-0", "shop", "frontend", "100", "n1")}
	pl, nodeInfos := newPlugin(t, appGroup("shop", shop, nil), weights{governance: 1}, []*v1.Node{n1, n2}, existing)
	got, status := scoreAll(t, pl, makePod("fe-1", "shop", "frontend", "100", ""), nodeInfos)
	if status != nil {
		t.Fatalf("unexpected status %v", status)
	}
	want := fwk.NodeScoreList{{Name: "n1", Score: 0}, {Name: "n2", Score: 100}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGraphCriticalityRanksTheHubFirst(t *testing.T) {
	nodes := []*v1.Node{makeNode("n1", "z1"), makeNode("n2", "z2")}
	pl, _ := newPlugin(t, appGroup("shop", shop, nil), newWeights(40, 35, 0), nodes, nil)
	scores := pl.appGroupScores(context.Background(), "default", "shop")
	hub := pl.criticality(scores, makePod("p", "shop", "frontend", "", ""))
	for _, leaf := range []string{"catalog", "redis"} {
		if c := pl.criticality(scores, makePod("p", "shop", leaf, "", "")); c >= hub {
			t.Errorf("%s criticality %.3f should be below frontend's %.3f", leaf, c, hub)
		}
	}
}

func TestAppGroupScoresAreCachedByResourceVersion(t *testing.T) {
	ag := appGroup("shop", shop, nil)
	pl, _ := newPlugin(t, ag, weights{centrality: 1}, []*v1.Node{makeNode("n1", "z1")}, nil)
	first := pl.appGroupScores(context.Background(), "default", "shop")
	if len(pl.cache) != 1 || pl.cache[ag.UID].resourceVersion == "" {
		t.Fatalf("expected one cached entry, got %v", pl.cache)
	}
	second := pl.appGroupScores(context.Background(), "default", "shop")
	if fmt.Sprintf("%p", first) != fmt.Sprintf("%p", second) {
		t.Errorf("expected the cached map to be reused")
	}
}
