package edu.ucla.cs.wis.bigdatalog.interpreter;

public enum ArithmeticOperation {
	ADDITION("+", true, 2), 
	SUBTRACTION("-", false, 2),
	MULTIPLICATION("*", true, 2),
	DIVISION("/", false, 2),
	INTEGER_DIVISION("DIV", false, 2),
	MOD("mod", false, 2),
	OPC("opc", false, 2),
	LOG("log", true, 1),
    EXP("exp", true, 1),
    STEP("step", true, 1),
	NONE("", false, 0);
	
	private String symbol;
	private boolean isMonotonic;
	private int numberOfArguments;
	
	private ArithmeticOperation(String symbol, boolean isMonotonic, int numberOfArguments) {
		this.symbol = symbol;
		this.isMonotonic = isMonotonic;
		this.numberOfArguments = numberOfArguments;
	}
	
	public String getSymbol() { return this.symbol; }
	
	public boolean isMonotonic() { return this.isMonotonic; }
	
	public boolean isBinary() { return (this.numberOfArguments == 2); }
	
	public boolean isUnary() { return (this.numberOfArguments == 1); }
	
	public static ArithmeticOperation getOperation(String symbol) {
		for (ArithmeticOperation ao : ArithmeticOperation.values()) {
			if (ao.symbol.equals(symbol))
				return ao;
		}
		
		return ArithmeticOperation.NONE;
	}
	
	public String toString() { return this.symbol; }
}
