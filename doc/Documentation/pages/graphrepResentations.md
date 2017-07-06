---
title: Graph Representations
sidebar: mydoc_sidebar
hide_sidebar: true
permalink: graphRepresentations.html
summary: Comparision of different graph representations of Portal.
---




## Graph Representaions

### Vertex Edge
VertexEdge (VE) is a direct implementation of the TGraph-relational model, and is the most compact when there is only a single property: one RDD contains all nodes and another all edges. Consistently with the model and the GraphX API, all node properties are stored together as a single nested attribute, as are all edge properties. The main advantage of this schema-less attribute representation is that it can easily deal with schema evolution and leaves the details of attribute processing to the user. The disadvantage is that different properties may have different evolution rates, and a change to a single property requires a new attribute tuple.

### RepresentativeGraph
RepresentativeGraph (RG) stores each representative graph explicitly, and so naturally preserves structural locality, but temporal locality is lost. RG is a collection (parallel sequence) of GraphX graphs, one for each representative graph of TGraph, where nodes and edges store the attribute values for the specific time interval, thus using structural locality. This representation supports all operations of TGA that can be expressed over snapshots, i.e., any operation that does not explicitly refer to time. GraphX provides Pregel API to support recursive non-temporal aggregation. While the RG representation is simple, it is not compact, considering that in many real-world evolving graphs there is a 80% or larger similarity between consecutive snapshots. In a distributed architecture, however, this data structure provides some benefits as operations on it can be easily parallelized by assigning different representative graphs to different workers. Each representative graph in the RG representation is loaded from parquet files using filter pushdown with a time predicate. We include this representation mainly as a naive implementation that serves as a performance baseline.

### OneGraph
OneGraph (OG) stores all nodes and edges of an evolving graph once, in a single data structure. This representation emphasizes temporal locality. OneGraph (OG) is the most topologically compact representation and stores all nodes from TVand edges from TE once, in a single aggregated data structure.OG emphasizes temporal locality, while also preserving structural locality, but leads to a much denser graph than RG. This, in turn, makes parallelizing computation challenging. An OG is implemented as a single GraphX graph where the node and edge attributes are bitsets that encode the presence of a node or edge in each time period associated with some representative graph of a TGraph. To construct an OG from on-disk node and edge archives, nodes and edges are grouped by key and mapped to bits corresponding to periods of change over the graph. Because OG stores information only about graph topology, far fewer periods must be represented and computed for OG than for RG. The actual reduction depends on the rate and nature of graph evolution. Information about time validity is stored together with each node and edge. 

Figure below shows a small OG over five intervals.

### HybridGraph
HG trades compactness of OG for better structural locality of RG, by aggregating together several consecutive representative graphs, computing a single OG for each graph group, and storing these as a parallel sequence. In our current implementation each OG in the sequence corresponds to one snapshot group on disk. Thus if no-partitioning snapshot group method is used, HG is equivalent to OG. Like OG, HG focuses on topology-based analysis, and so does not represent node and edge attributes. HG implements analytics, node creation, and set operators, and supports all other operations through inheritance from VE. Analytics are implemented similarly to OG, with batching within each graph group.
