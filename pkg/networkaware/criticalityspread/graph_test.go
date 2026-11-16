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
	"math"
	"testing"

	agv1alpha1 "github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
)

// Expected values were computed with NetworkX 3.x (pagerank, betweenness_centrality,
// degree_centrality and closeness_centrality on a DiGraph) for the same graphs.
func TestWorkloadGraphScores(t *testing.T) {
	onlineBoutique := map[string][]string{
		"frontend":              {"cartservice", "productcatalogservice", "currencyservice", "shippingservice", "checkoutservice", "adservice"},
		"cartservice":           {"frontend", "redis-cart"},
		"productcatalogservice": {"frontend", "checkoutservice", "recommendationservice"},
		"currencyservice":       {"frontend", "checkoutservice"},
		"paymentservice":        {"checkoutservice"},
		"shippingservice":       {"frontend", "checkoutservice"},
		"emailservice":          {"checkoutservice"},
		"checkoutservice":       {"frontend", "cartservice", "productcatalogservice", "currencyservice", "paymentservice", "shippingservice", "emailservice"},
		"recommendationservice": {"frontend", "productcatalogservice"},
		"adservice":             {"frontend"},
		"redis-cart":            {"cartservice"},
	}
	tests := []struct {
		name string
		ag   *agv1alpha1.AppGroup
		want map[string]graphScores
	}{
		{
			name: "chain",
			ag:   appGroup("chain", map[string][]string{"a": {"b"}, "b": {"c"}, "c": {"d"}}, nil),
			want: map[string]graphScores{
				"a": {0.265263009, 0},
				"b": {0.886204709, 0.630929754},
				"c": {1, 0.630929754},
				"d": {0.67036208, 0},
			},
		},
		{
			name: "star with unequal bandwidth",
			ag: appGroup("star", map[string][]string{"hub": {"x", "y", "z"}, "x": {"y"}},
				map[[2]string]string{{"hub", "x"}: "300", {"hub", "y"}: "100", {"hub", "z"}: "100", {"x", "y"}: "100"}),
			want: map[string]graphScores{
				"hub": {0.693757738, 0.864973521},
				"x":   {0.693826325, 0.511859507},
				"y":   {1, 0.630929754},
				"z":   {0.515193839, 0},
			},
		},
		{
			name: "online boutique AppGroup example",
			ag:   appGroup("ob", onlineBoutique, nil),
			want: map[string]graphScores{
				"adservice":             {0.205824592, 0},
				"cartservice":           {0.484493877, 0.458146008},
				"checkoutservice":       {0.933628997, 0.83606951},
				"currencyservice":       {0.303396409, 0.301029996},
				"emailservice":          {0.191103446, 0},
				"frontend":              {1, 0.83606951},
				"paymentservice":        {0.191103446, 0},
				"productcatalogservice": {0.444132168, 0.477121255},
				"recommendationservice": {0.213233873, 0.276434591},
				"redis-cart":            {0.200886898, 0},
				"shippingservice":       {0.303396409, 0.301029996},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newWorkloadGraph(tt.ag).scores()
			if len(got) != len(tt.want) {
				t.Fatalf("got %d workloads, want %d", len(got), len(tt.want))
			}
			for name, w := range tt.want {
				g := got[name]
				if math.Abs(g.Centrality-w.Centrality) > 1e-6 || math.Abs(g.Entropy-w.Entropy) > 1e-6 {
					t.Errorf("%s: got %+v, want %+v", name, g, w)
				}
			}
		})
	}
}

func TestWorkloadGraphIgnoresSelfLoopsDuplicatesAndUnknownWorkloads(t *testing.T) {
	ag := appGroup("g", map[string][]string{"a": {"a", "b", "b"}, "b": nil}, nil)
	g := newWorkloadGraph(ag)
	if len(g.out[g.index["a"]]) != 1 {
		t.Errorf("want one edge a->b, got %v", g.out[g.index["a"]])
	}
	if got := newWorkloadGraph(&agv1alpha1.AppGroup{}).scores(); len(got) != 0 {
		t.Errorf("empty AppGroup: got %v", got)
	}
}
