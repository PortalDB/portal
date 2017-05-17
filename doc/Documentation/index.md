---
title: Portal Programming Guide
keywords: portal homepage
tags: [getting_started]
sidebar: mydoc_sidebar
hide_sidebar: true
permalink: index.html
summary: These instructions will introduce you to Portal.
---


## Overview
Portal is a system for efficient querying and exploratory analysis of evolving graphs, built on Apache Spark. 

## Temporal Graph
Portal implements an algebraic query language called TGraph algebra (TGA). A TGraph represents evolution of a single graph, e.g., the Web. A snapshot of a TGraph is the state of the relations at any time point. TGraph relations are coalesced — each fact is represented exactly once for each time period of maximal length during which it holds. Referential integrity holds on E w.r.t. V, guaranteeing that edges only exist if their end points exist at the same time, on VA w.r.t. V, and on EA w.r.t. E

## Graph Operators

## List of Operators


