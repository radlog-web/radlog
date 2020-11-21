package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

/*APS 9/13/2014
 * While all nodes in this package are designed for pipelining, some nodes can be executed using materialization to avoid recomputation.
 * This is also helpful for root nodes of the program, so some query form can avoid having to populate relations. */
public enum ExecutionMode {
	Pipelined, Materialized;
}
