package edu.ucla.cs.wis.bigdatalog.interpreter;

public enum ComparisonOperation {
	EQUALITY("=", true),
	INEQUALITY("~=", true), 
	GREATER_THAN(">", true),
	GREATER_THAN_OR_EQUAL(">=", true), 
	LESS_THAN("<", false),
	LESS_THAN_OR_EQUAL("<=", false),
	NONE("", false);
	
	private String symbol;
	private boolean isMonotonic;
	
	private ComparisonOperation(String symbol, boolean isMonotonic) {
		this.symbol = symbol;
		this.isMonotonic = isMonotonic;
	}
	
	public String getSymbol() { return this.symbol; }
	
	public boolean isMonotonic() { return this.isMonotonic; }
	
	public static ComparisonOperation getOperation(String symbol) {
		for (ComparisonOperation co : ComparisonOperation.values()) {
			if (co.symbol.equals(symbol))
				return co;
		}
		return ComparisonOperation.NONE;
	}
	
	public String toString() { return this.symbol; }
}
