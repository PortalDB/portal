---
title: Portal Programming Guide
keywords: portal homepage
sidebar: mydoc_sidebar
hide_sidebar: true
permalink: index.html
summary: These instructions will introduce you to Portal.
---


## Overview
Portal is a system for efficient querying and exploratory analysis of evolving graphs, built on [Apache Spark](https://spark.apache.org/). At a high level, Portal extends Apache Spark's graph library, [GraphX](http://spark.apache.org/graphx/), by introducing a new Temporal Graph abstraction (TGraph) with four different concrete implementaion of TGraph (RepresentativeGraph, OneGraph, HybridGraph and Vertex Edge). To support evolving graph computation, Portal exposes a set of fundamental operators (e.g., trim, subgraph, aggregate, difference, union, intersection and node creation) for TGraph.

TODO: Add diagrams to make it definitations clear

## Temporal Graph
A TGraph represents evolution of a single graph, e.g., the Web. A snapshot of a TGraph is the state of the relations at any time point. 

TGraph relations are coalesced — each fact is represented exactly once for each time period of maximal length during which it holds. Referential integrity holds on E w.r.t. V, guaranteeing that edges only exist if their end points exist at the same time, on VA w.r.t. V, and on EA w.r.t. E

## Graph Representaions
Portal provides four in-memory TGraph representations that differ in compactness and the kind of locality they prioritize. The different representaions and how to use them is explained below. For a more detailed comparision of these graphs, check out [Graph Representaion comparisions](/graphRepresentations). There are three ways to create there graph representations. First one is from RDD's, second is from DataFrames and and third is creating empty graphs.

### Vertex Edge
VertexEdge (VE) is a direct implementation of the TGraph-relational model, and is the most compact when there is only a single property: one RDD contains all nodes and another all edges. The three ways to create a VEGraph are shown below.

#### VEGraph from RDD 
To create a new VE Graph from RDD use

`VEGraph.fromRDDs(nodes, edges, "Default")`

where nodes and edges are RDD's.

#### VE Graph from Dataframes 

To create a new VEGraph from Dataframes use

`VEGraph.fromRDDs(nodes, edges, "Default")`

where nodes and edges are Dataframes's.

#### Empty VEGraph 

To create a new empty VE Graph use

`VEGraph.emptyGraph()`

### RepresentativeGraph  SGP????
RepresentativeGraph (RG) stores each representative graph explicitly, and so naturally preserves structural locality, but temporal locality is lost. 


### OneGraph
OneGraph (OG) stores all nodes and edges of an evolving graph once, in a single data structure. This representation emphasizes temporal locality.

The three ways to create a OneGraph are shown below.

#### OneGraph from RDD 
To create a new OneGraph from RDD use

`OneGraph.fromRDDs(nodes, edges, "Default")`

If new nodes are to be created during this operation, the value of the new nodes is "Default". 

where nodes and edges are RDD's.

#### OneGraph from Dataframes 

To create a new OneGraph from Dataframes use

`OneGraph.fromRDDs(nodes, edges, "Default")`

where nodes and edges are Dataframes's.

#### Empty OneGraph 

To create a new empty OneGraph use

`OneGraph.emptyGraph()`
 

Figure below shows a small OG over five intervals.

### HybridGraph
HybridGraph (HG) trades compactness for better structural locality, by aggregating together several consecutive snapshots, and computing a OneGraph for each group. We can convert from one representation to any other at a small cost, so it is useful to think of them as access methods in the context of individual operations.

The three ways to create a HybridGraph are shown below.

#### HybridGraph from RDD 
To create a new HybridGraph from RDD use

`HybridGraph.fromRDDs(nodes, edges, "Default")`

where nodes and edges are RDD's.

#### HybridGraph from Dataframes 

To create a new HybridGraph from Dataframes use

`HybridGraph.fromRDDs(nodes, edges, "Default")`

where nodes and edges are Dataframes's.

#### Empty HybridGraph 

To create a new empty HybridGraph use

`HybridGraph.emptyGraph()`


## TGraph Operators
Portal implements an algebraic query language called TGraph algebra (TGA). The operations that can be done on a TGraphs are listed below.

### Trim (SLICE??)
The trim operator takes a specified period and computes a new TGraph, limited only to those nodes and edges that existed during the specified period.

Select a temporal subset of the graph. T-Select. To call this function provide a Time period to extract and it will return the temporal intersection. If the requested interval is completely outside of the graph bounds, the function will return an empty graph.

In the example below, you will see the the graph `graph` is sliced with an interval of 2012-01-01 to 2015-01-01. The resulting graph will only contain nodes and edges within that time interval.

`val sliceInterval = (Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))`

`var slicedGraph = graph.slice(sliceInterval)`


### Map
Vertex-map and Edge-map can be used to manipulate node and edge attributes of a TGraph.Vertex-map and edge-map apply user-defined map functions to attributes in the same spirit as map in functional languages. While the map functions are arbitrary user-specified functions, there are some common cases. Map may specify the set of properties to project out or retain, it may aggregate (e.g., COUNT) or values of a collection property, or unnest a nested value in a property. In other words, mapping is on an entity-by-entity, tuple-by-tuple basis. The time period can be referenced in the mapping function but cannot be changed, that is, the period of validity remains unchanged.


#### vertex Map

The vertex map operator takes in a map function which transforms each vertex attribute in the graph for each time period using the map function. The vmap function can also be specified with the data type of the new vertex object. If no data type is specified, the funciton will consider the data type of the new vertex is the same old vertex.

`var mappedGraph = graph.vmap((vertex,intv, name) => name.toUpperCase, "Default")`
   
If new nodes are to be created during this operation, the value of the new nodes is "Default". 

#### edge Map

The edge map operator takes in a map function which transforms each edge attribute in the graph for each time period using the map function. The emap function can also be specified with the data type of the new edge object. If no data type is specified, the funciton will consider the data type of the new edge is the same old edge.

`var mappedGraph = graph.emap(edge => edge.attr * tedge.attr)`


### SubGraph

#### VsubGraph
`var selectFunction = (id: VertexId, attr: String, x: Interval) => x.equals(Interval(LocalDate.parse("2012-01-01"), LocalDate.parse("2015-01-01")))`

`var subgraph = graph.vsubgraph(selectFunction)`

#### esubGraph
`val actualg = g.esubgraph(pred = tedgeTriplet => tedgeTriplet.srcId > 2 && tedgeTriplet.attr==42)`


### Aggregation

#### AggregateByChange
The aggregateByChange operator takes in ChangeSpec, vertex Quantifier, edge quantifier, vertex aggregateFunction and edgeAggregate functions and returns the aggregated graph.

`var aggregatedGraph = graph.aggregateByChange(c, vquant, equant, vAggFunc, eAggFunc)`

TODO:
ChangeSpec: ??

Vertex Quantifier: ??

Edge Quantifier: ??

Vertex Aggregation function: ??

EdgeAggregation function: ??


#### AggregateByTime
The aggregateByTime operator takes in TimeSpec, vertex Quantifier, edges quantifier, vertex aggregateFunction and edgeAggregate functions and returns the aggregated graph.

`var aggregatedGraph = graph.aggregateByTime(t, vquant, equant, vAggFunc, eAggFunc)`

TODO:
TimeSpec: ??

Vertex Quantifier: ??

Edge Quantifier: ??

Vertex Aggregation function: ??

EdgeAggregation function: ??


### Union
The union operator takes two temporal graphs and returns a new TGraph with the union of the both graphs within each chronon???.

`val unionGraph = graph1.union(graph2)`

### Intersection

The intersection operator takes two temporal graphs and returns a new TGraph with the intersection of the both graphs within each chronon???.

`val intersectionGraph = graph1.intersection(graph2)`

### Difference
The difference operator takes two temporal graphs and returns a new TGraph with the difference of the both graphs within each chronon???.

`val unionGraph = graph1.union(graph2)`

### Node creation
TODO:

### Edge Creation
TODO:

## TGraph Analytics

### Pagerank
The pagerank operation runs pagerank on all intervals. It is up to the implementation to run sequantially, in parallel, incrementally, etc. The number of iterations will depend on both the numIter argument and the rate of convergence, whichever occurs first. The pagerank fuction takes parameters 
uni: to treat the graph as undirected or directed where true is directed
tol: measure of convergence
resetProb: probability of reset/jump
numIter: number of iterations of the algorithm to run. If omitted, will run
until: convergence of the tol argument.

The function returns a new graph with pageranks for each interval (coalesced).

TODO: add example

### ShortestPath
The shortestPath operation computes shortest paths to the given set of landmark vertices. The shortestPath fuction takes parameters 
landmarks: the list of landmark vertex ids to which shortest paths will be computed
uni: Treat the graph as undirected or directed where true is directed
   
The function returns a graph with vertices where each vertex attribute is the shortest-path distance to each reachable landmark vertex.
  
TODO: add example

### ConnectedComponents
The connectedComponents operation runs connected components algorithm on a temporal graph and returns a graph with the vertex attribute of the lowest vertex id in the connected component containing that vertex.
   
TODO: add example

### Triangle Count
The trinagle count operation computes the clustering coefficient of each vertex, which is equal to the number of triangles that pass through it divided by k*(k-1), where k is the vertex degree.

TODO: add example

### Clustering Coefficient

The clustering coefficient operation aggregates values from the neighboring edges and vertices of each vertex, for each representative graph. Unlike in GraphX, this returns a new graph, not an RDD. The user-supplied `sendMsg` function is invoked on each edge of the graph, generating 0 or more messages to be sent to either vertex in the edge. The `mergeMsg` function is then used to combine all messages
   
TODO: add example
