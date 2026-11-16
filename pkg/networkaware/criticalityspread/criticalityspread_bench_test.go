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
	"testing"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// BenchmarkScoreCycle measures one PreScore, a Score per node and NormalizeScore
// for a pod of an 11-workload AppGroup, with 3 AppGroup pods already on every node.
func BenchmarkScoreCycle(b *testing.B) {
	deps := map[string][]string{
		"frontend": {"cart", "catalog", "currency", "shipping", "checkout", "ads"},
		"cart":     {"redis"}, "catalog": {"recommend"}, "checkout": {"cart", "catalog", "currency", "payment", "shipping", "email"},
		"currency": nil, "shipping": nil, "payment": nil, "email": nil, "recommend": {"catalog"}, "ads": nil, "redis": nil,
	}
	workloads := sortedKeys(deps)
	for _, n := range []int{100, 500, 1000} {
		b.Run(fmt.Sprintf("%d-nodes", n), func(b *testing.B) {
			var nodes []*v1.Node
			var pods []*v1.Pod
			for i := 0; i < n; i++ {
				name := fmt.Sprintf("n%d", i)
				nodes = append(nodes, makeNode(name, fmt.Sprintf("z%d", i%5)))
				for j := 0; j < 3; j++ {
					w := workloads[(i*3+j)%len(workloads)]
					pods = append(pods, makePod(fmt.Sprintf("%s-%d-%d", w, i, j), "shop", w, "50", name))
				}
			}
			pl, nodeInfos := newPlugin(b, appGroup("shop", deps, nil), newWeights(40, 35, 25), nodes, pods)
			pod := makePod("frontend-new", "shop", "frontend", "100", "")
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				state := framework.NewCycleState()
				if status := pl.PreScore(ctx, state, pod, nodeInfos); !status.IsSuccess() {
					b.Fatal(status)
				}
				scores := make(fwk.NodeScoreList, 0, len(nodeInfos))
				for _, ni := range nodeInfos {
					s, _ := pl.Score(ctx, state, pod, ni)
					scores = append(scores, fwk.NodeScore{Name: ni.Node().Name, Score: s})
				}
				_ = pl.NormalizeScore(ctx, state, pod, scores)
			}
		})
	}
}
