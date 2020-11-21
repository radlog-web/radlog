package edu.ucla.cs.wis.bigdatalog.compiler.type.expression;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerArithmeticExpression extends CompilerTypeBase {
	private static final long serialVersionUID = 1L;
	public ArithmeticOperation operation;
	public CompilerTypeBase argument1;
	public CompilerTypeBase argument2;
	public DataType 		dataType;
	
	public CompilerArithmeticExpression(String operation, CompilerTypeBase argument1) {
		this(operation, argument1, null);
	}
	
	public CompilerArithmeticExpression(String operation, CompilerTypeBase argument1, CompilerTypeBase argument2) {
		this(ArithmeticOperation.getOperation(operation), argument1, argument2, DataType.UNKNOWN);
	}
	
	public CompilerArithmeticExpression(ArithmeticOperation operation, CompilerTypeBase argument1, DataType dataType) {
		this(operation, argument1, null, dataType);
	}		
	
	public CompilerArithmeticExpression(ArithmeticOperation operation, CompilerTypeBase argument1, CompilerTypeBase argument2, DataType dataType) {
		super(CompilerType.ARITHMETIC_EXPRESSION);
		
		if (argument1 == null)
			throw new CompilerException("ArithmeticExpression requires at least one argument.");
		
		if (operation.isUnary() && (argument2 != null))
			throw new CompilerException("Unary ArithmeticExpression can only have one argument.");
		
		if (operation.isBinary() && (argument2 == null))
			throw new CompilerException("Binary ArithmeticExpression requires two arguments.");
		
		this.operation = operation;
		this.argument1 = argument1;
		this.argument2 = argument2;
		this.dataType = dataType;
	}
	
	public boolean isUnary() { return this.operation.isUnary(); }
	
	public boolean isBinary() { return this.operation.isBinary(); }
	
	public ArithmeticOperation getOperation() { return this.operation; }

	public CompilerTypeBase getArgument1() { return this.argument1; }
	
	public void setArgument1(CompilerTypeBase argument1) { this.argument1 = argument1; }
	
	public CompilerTypeBase getArgument2() { return this.argument2; }
	
	public void setArgument2(CompilerTypeBase argument2) { this.argument2 = argument2; }
	
	public DataType getDataType() { return this.dataType; }
	
	public void setDataType(DataType dataType) { this.dataType = dataType; }
	
	@Override
	public CompilerArithmeticExpression copy() {
		if (this.isBinary())
			return new CompilerArithmeticExpression(this.operation, this.argument1.copy(), this.argument2.copy(), this.dataType);
		
		return new CompilerArithmeticExpression(this.operation, this.argument1.copy(), this.dataType);
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		if (this.isBinary())
			return new CompilerArithmeticExpression(this.operation, this.argument1.copy(variableList), this.argument2.copy(variableList), this.dataType) ;
		
		return new CompilerArithmeticExpression(this.operation, this.argument1.copy(variableList), this.dataType) ;
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerArithmeticExpression))
			return false;
		
		CompilerArithmeticExpression otherExpression = (CompilerArithmeticExpression)other;
		
		return ((this.operation == otherExpression.operation) 
				&& this.argument1.equals(otherExpression.argument1)
				&& (((this.argument2 == null) && (otherExpression.argument2 == null))
						|| this.argument2.equals(otherExpression.argument2)) 
				&& (this.dataType == otherExpression.dataType));
	}

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.operation);
		output.append("(");
		output.append(this.argument1.toString());
		if (this.argument2 != null) {
			output.append(", ");
			output.append(this.argument2.toString());
		}
		output.append(")");
		//output.append(" as ");
		//output.append(this.dataType);		
		return output.toString();
	}
	
	public String toStringInfix() {
		if (this.isUnary())
			return toString();
		
		StringBuilder output = new StringBuilder();		
		output.append("(");
		output.append(this.argument1.toString());
		output.append(" ");
		output.append(this.operation);
		output.append(" ");
		output.append(this.argument2.toString());
		output.append(")");	
		return output.toString();
	}

	public static CompilerArithmeticExpression createExpression(String operation, CompilerTypeBase argument1) {
		return new CompilerArithmeticExpression(operation, argument1);
	}
	
	public static CompilerArithmeticExpression createExpression(String operation, CompilerTypeBase argument1, CompilerTypeBase argument2) {
		return new CompilerArithmeticExpression(operation, argument1, argument2);
	}
	
}
