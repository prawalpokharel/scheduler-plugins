# CriticalitySpread

## Overview

`CriticalitySpread` is a **PreScore and Score** plugin for pods that belong to an
[AppGroup](https://github.com/diktyo-io/appgroup-api), the same CRD that the
`TopologicalSort` and `NetworkOverhead` plugins read.

`NetworkOverhead` places dependent pods close together to cut latency. This plugin
looks at the other side of the same dependency graph: if a node or a zone fails,
how many requests break? It prefers nodes that spread the replicas of critical
workloads across zones and then nodes, and it keeps critical workloads off the
same node, so that one failure does not take down several of them at once.

## How a node is scored

1. **Workload criticality.** From the AppGroup dependency graph, each workload gets

   `criticality = (α·C + β·H + γ·R) / (α + β + γ)`

   - `C`: composite centrality in the dependency graph
     (0.35 PageRank + 0.30 betweenness + 0.20 degree + 0.15 closeness, divided by the maximum),
   - `H`: normalised Shannon entropy of the workload's `minBandwidth` to each neighbour,
   - `R`: the operator-declared label `scheduling.x-k8s.io/criticality` on the pod, from 0 to 100.

   This is the Centrality-Entropy Index (CEI) of Pokharel, "Governance-Aware Dynamic
   Resource Allocation" (FTC 2026). The graph parts are cached per AppGroup
   `resourceVersion`.

2. **Node cost** (PreScore): 2 × the replicas of the same workload already in the
   node's zone, plus 1 × those already on the node, plus the criticality of the
   other AppGroup pods already on the node.

3. **Normalisation**: costs are mapped to `[0, 100]`, cheapest node highest, and the
   spread is scaled by the workload's criticality. A critical workload gets a strong
   preference; a workload with criticality near 0 gets an almost flat score, and the
   default plugins decide where it goes. Pods outside an AppGroup skip the plugin.

Nodes without a `topology.kubernetes.io/zone` label are treated as their own zone.

## Configuration

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: false
profiles:
  - schedulerName: criticality-scheduler
    plugins:
      multiPoint:
        enabled:
          - name: CriticalitySpread
            weight: 3
    pluginConfig:
      - name: CriticalitySpread
        args:
          namespaces: ["default"]  # where to look for AppGroups besides the pod's own namespace
          centralityWeight: 40     # α (defaults shown)
          entropyWeight: 35        # β
          governanceWeight: 25     # γ
```

Pods opt in with the AppGroup labels and, optionally, a declared criticality:

```yaml
metadata:
  labels:
    appgroup.diktyo.x-k8s.io: online-boutique
    appgroup.diktyo.x-k8s.io.workload: frontend
    scheduling.x-k8s.io/criticality: "100"
spec:
  schedulerName: criticality-scheduler
```

The plugin runs with every default plugin enabled, as a second scheduler, so
workloads that do not set `schedulerName` are not affected.

## Limitations

- It only scores; it never filters. If a node is full or excluded by other plugins,
  the spread gives way.
- It spreads at scheduling time only. It does not move running pods; use the
  descheduler for that.
- Spreading trades latency for resilience: more dependency traffic crosses nodes and
  zones than with `NetworkOverhead`.
