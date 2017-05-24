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

## Temporal Graph
A TGraph represents evolution of a single graph, e.g., the Web. A snapshot of a TGraph is the state of the relations at any time point. 

TGraph relations are coalesced — each fact is represented exactly once for each time period of maximal length during which it holds. Referential integrity holds on E w.r.t. V, guaranteeing that edges only exist if their end points exist at the same time, on VA w.r.t. V, and on EA w.r.t. E

## Graph Representaions
Portal provides four in-memory TGraph representations that differ in compactness and the kind of locality they prioritize. The different representaions and how to use them is explained below. For a more detailed comparision of these graphs, check out [Graph Representaion comparisions](/graphRepresentations)

### Vertex Edge
VertexEdge (VE) is a direct implementation of the TGraph-relational model, and is the most compact when there is only a single property: one RDD contains all nodes and another all edges. 


### RepresentativeGraph
RepresentativeGraph (RG) stores each representative graph explicitly, and so naturally preserves structural locality, but temporal locality is lost. 


### OneGraph
OneGraph (OG) stores all nodes and edges of an evolving graph once, in a single data structure. This representation emphasizes temporal locality.
 

Figure below shows a small OG over five intervals.

### HybridGraph
HybridGraph (HG) trades compactness for better structural locality, by aggregating together several consecutive snapshots, and computing a OneGraph for each group. We can convert from one representation to any other at a small cost, so it is useful to think of them as access methods in the context of individual operations.


## Graph Operators
Portal implements an algebraic query language called TGraph algebra (TGA). The operations that can be done on a TGraphs are listed below.

### Trim
The trim operator takes a specified period and computes a new TGraph, limited only to those nodes and edges that existed during the specified period.


### Map
Vertex-map and Edge-map can be used to manipulate node and edge attributes of a TGraph.Vertex-map and edge-map apply user-defined map functions to attributes in the same spirit as map in functional languages. While the map functions are arbitrary user-specified functions, there are some common cases. Map may specify the set of properties to project out or retain, it may aggregate (e.g., COUNT) or values of a collection property, or unnest a nested value in a property. In other words, mapping is on an entity-by-entity, tuple-by-tuple basis. The time period can be referenced in the mapping function but cannot be changed, that is, the period of validity remains unchanged.

### SubGraph

### Aggregation

### Node creation

### Union

### Intersection

### Difference

### Edge Creation


