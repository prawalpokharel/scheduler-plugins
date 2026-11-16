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
	"sort"

	agv1alpha1 "github.com/diktyo-io/appgroup-api/pkg/apis/appgroup/v1alpha1"
)

// workloadGraph is the directed dependency graph of an AppGroup. An edge u -> v
// means that workload u lists workload v as a dependency.
type workloadGraph struct {
	names []string
	index map[string]int
	out   [][]int
	in    [][]int
	// bandwidth of each edge, keyed by [from, to]; 1 when minBandwidth is unset.
	bandwidth map[[2]int]float64
}

// graphScores holds the graph-derived parts of a workload's criticality.
type graphScores struct {
	// Centrality is the composite centrality divided by its maximum, in [0, 1].
	Centrality float64
	// Entropy is the normalised Shannon entropy of the workload's dependency bandwidths, in [0, 1].
	Entropy float64
}

func newWorkloadGraph(ag *agv1alpha1.AppGroup) *workloadGraph {
	seen := map[string]bool{}
	for _, w := range ag.Spec.Workloads {
		seen[w.Workload.Selector] = true
		for _, d := range w.Dependencies {
			seen[d.Workload.Selector] = true
		}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		if n != "" {
			names = append(names, n)
		}
	}
	sort.Strings(names)

	g := &workloadGraph{
		names:     names,
		index:     make(map[string]int, len(names)),
		out:       make([][]int, len(names)),
		in:        make([][]int, len(names)),
		bandwidth: map[[2]int]float64{},
	}
	for i, n := range names {
		g.index[n] = i
	}
	for _, w := range ag.Spec.Workloads {
		u, ok := g.index[w.Workload.Selector]
		if !ok {
			continue
		}
		for _, d := range w.Dependencies {
			v, ok := g.index[d.Workload.Selector]
			if !ok || u == v {
				continue
			}
			key := [2]int{u, v}
			if _, dup := g.bandwidth[key]; dup {
				continue
			}
			bw := d.MinBandwidth.AsApproximateFloat64()
			if bw <= 0 {
				bw = 1
			}
			g.bandwidth[key] = bw
			g.out[u] = append(g.out[u], v)
			g.in[v] = append(g.in[v], u)
		}
	}
	return g
}

// scores returns the centrality and entropy of every workload, keyed by selector.
// Centrality is 0.35 PageRank + 0.30 betweenness + 0.20 degree + 0.15 closeness
// (each as defined by NetworkX for directed graphs), divided by the maximum over
// workloads.
func (g *workloadGraph) scores() map[string]graphScores {
	n := len(g.names)
	result := make(map[string]graphScores, n)
	if n == 0 {
		return result
	}
	pr := g.pageRank(0.85, 100, 1e-6)
	bw := g.betweenness()
	cl := g.closeness()
	composite := make([]float64, n)
	maxComposite := 0.0
	for i := 0; i < n; i++ {
		degree := 0.0
		if n > 1 {
			degree = float64(len(g.out[i])+len(g.in[i])) / float64(n-1)
		}
		composite[i] = 0.35*pr[i] + 0.30*bw[i] + 0.20*degree + 0.15*cl[i]
		maxComposite = math.Max(maxComposite, composite[i])
	}
	for i, name := range g.names {
		c := 0.0
		if maxComposite > 0 {
			c = composite[i] / maxComposite
		}
		result[name] = graphScores{Centrality: c, Entropy: g.entropy(i)}
	}
	return result
}

// pageRank follows networkx.pagerank: uniform teleport, and dangling nodes
// spread their rank uniformly.
func (g *workloadGraph) pageRank(alpha float64, maxIter int, tol float64) []float64 {
	n := len(g.names)
	x := make([]float64, n)
	for i := range x {
		x[i] = 1 / float64(n)
	}
	for iter := 0; iter < maxIter; iter++ {
		next := make([]float64, n)
		dangling := 0.0
		for u := 0; u < n; u++ {
			if len(g.out[u]) == 0 {
				dangling += x[u]
				continue
			}
			share := alpha * x[u] / float64(len(g.out[u]))
			for _, v := range g.out[u] {
				next[v] += share
			}
		}
		base := (1-alpha)/float64(n) + alpha*dangling/float64(n)
		diff := 0.0
		for i := range next {
			next[i] += base
			diff += math.Abs(next[i] - x[i])
		}
		x = next
		if diff < float64(n)*tol {
			break
		}
	}
	return x
}

// betweenness is Brandes' algorithm on the unweighted directed graph, normalised
// by 1/((n-1)(n-2)) as networkx.betweenness_centrality does for directed graphs.
func (g *workloadGraph) betweenness() []float64 {
	n := len(g.names)
	cb := make([]float64, n)
	for s := 0; s < n; s++ {
		stack := make([]int, 0, n)
		pred := make([][]int, n)
		sigma := make([]float64, n)
		dist := make([]int, n)
		for i := range dist {
			dist[i] = -1
		}
		sigma[s], dist[s] = 1, 0
		queue := []int{s}
		for len(queue) > 0 {
			v := queue[0]
			queue = queue[1:]
			stack = append(stack, v)
			for _, w := range g.out[v] {
				if dist[w] < 0 {
					dist[w] = dist[v] + 1
					queue = append(queue, w)
				}
				if dist[w] == dist[v]+1 {
					sigma[w] += sigma[v]
					pred[w] = append(pred[w], v)
				}
			}
		}
		delta := make([]float64, n)
		for i := len(stack) - 1; i >= 0; i-- {
			w := stack[i]
			for _, v := range pred[w] {
				delta[v] += sigma[v] / sigma[w] * (1 + delta[w])
			}
			if w != s {
				cb[w] += delta[w]
			}
		}
	}
	if n > 2 {
		scale := 1 / float64((n-1)*(n-2))
		for i := range cb {
			cb[i] *= scale
		}
	}
	return cb
}

// closeness follows networkx.closeness_centrality for directed graphs: it uses
// the distances to each node and the Wasserman-Faust correction for graphs that
// are not strongly connected.
func (g *workloadGraph) closeness() []float64 {
	n := len(g.names)
	cl := make([]float64, n)
	if n < 2 {
		return cl
	}
	for u := 0; u < n; u++ {
		dist := map[int]int{u: 0}
		queue := []int{u}
		total := 0
		for len(queue) > 0 {
			v := queue[0]
			queue = queue[1:]
			for _, w := range g.in[v] {
				if _, ok := dist[w]; !ok {
					dist[w] = dist[v] + 1
					total += dist[w]
					queue = append(queue, w)
				}
			}
		}
		reached := float64(len(dist) - 1)
		if total > 0 {
			cl[u] = reached / float64(total) * reached / float64(n-1)
		}
	}
	return cl
}

// entropy is the Shannon entropy of workload i's bandwidth to each neighbour
// (edges in both directions summed per neighbour), divided by log(n-1), the
// largest value it can take.
func (g *workloadGraph) entropy(i int) float64 {
	n := len(g.names)
	if n <= 2 {
		return 0
	}
	perNeighbour := map[int]float64{}
	for _, v := range g.out[i] {
		perNeighbour[v] += g.bandwidth[[2]int{i, v}]
	}
	for _, u := range g.in[i] {
		perNeighbour[u] += g.bandwidth[[2]int{u, i}]
	}
	weights := make([]float64, 0, len(perNeighbour))
	for _, w := range perNeighbour {
		weights = append(weights, w)
	}
	// Sum in a fixed order so the result does not depend on map iteration.
	sort.Float64s(weights)
	total := 0.0
	for _, w := range weights {
		total += w
	}
	if total <= 0 {
		return 0
	}
	h := 0.0
	for _, w := range weights {
		p := w / total
		if p > 0 {
			h -= p * math.Log(p)
		}
	}
	return math.Min(1, h/math.Log(float64(n-1)))
}
