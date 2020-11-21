package edu.ucla.cs.wis.bigdatalog.compiler.type;

public enum ArgumentType {
	UNKNOWN("u"),
	CONSTANT("c"),
	VARIABLE("v"),
	AGGREGATE("a"),
	FSAGGREGATE("f"),
	INPUT_VARIABLE("i"),
	EXPRESSION("e");
	
	private String abbreviation;
	
	private ArgumentType(String abbreviation) {
		this.abbreviation = abbreviation;
	}
	
	public String getAbbreviation() { return this.abbreviation; }
}
