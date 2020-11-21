package edu.ucla.cs.wis.bigdatalog.compiler.variable;

public enum BindingType {
	FREE("f"),
	BOUND("b"),
	UNKNOWN("u");
	
	private String symbol;
	
	private BindingType(String symbol) {
		this.symbol = symbol;
	}
	
	public String toString() { return this.symbol; }
}
