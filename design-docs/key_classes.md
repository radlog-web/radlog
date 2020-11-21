## Key Classes

### API

RaSQLRunner: Utility class to execute example RaSQL queries

DatalogRunner: Utility class to execute example Datalog queries

SessionBuilder: Utility class to help setup a BigDatalogSession

BigDatalogSession: Entry point for executing BigDatalog queries on Spark

BigDatalogSessionState: Keep session-specific states, including RaSQL/Datalog compiler related classes 

### Compiler

SqlBase.g4: Parser Rules for Spark SQL

LogicalPlanGenerator: Transform **Datalog Operator Program** to **Unresolved Logical Plan**

JoinPlanGenerator: Part of LogicalPlanGenerator to determine join types

BigDatalogAnalyzer: Transform **Unresolved Logical Plan** to **Resolved Logical Plan**

BigDatalogPlanner: Transform **Resolved Logical Plan** to **Physical Plan** (Recursion, Monotonic Aggregate)

### Executor

BigDatalogQueryExecution: The primary workflow for executing a query with preparation rules

RewriteRecursion: A preparation rule to replace exchange operators in recursion by partition-aware ExitExchange/RecExchange operators

RewriteAggregateRecursion: A preparation rule to replace exchange operators in aggregate recursion by partition-aware ExitAggrExchange/RecAggrExchange operators

Recursion: Drive the recursive iterations using Semi-Naive evaluation until fixpoint

AggregateRecursion: Similar to Recursion, but with aggregates

PartitionDataKeeper: Data holder for intermediate results during recursive iterations on each worker, may have better implementations in the future
