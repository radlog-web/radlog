package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive;

public enum CliqueRuleType {
	COPY_RULE(0),
	DELETE_RULE(1),
	Y_RULE(2),
	EXIT_RULE(3), 
	RECURSIVE_RULE(4), // only used in regular recursion - not xy
	X_RULE(5),
	RULE_TRANSITION(6);
	
	private int id;
	
	private CliqueRuleType(int id) {
		this.id = id;
	}
	
	public int getId() {return this.id;}
}
