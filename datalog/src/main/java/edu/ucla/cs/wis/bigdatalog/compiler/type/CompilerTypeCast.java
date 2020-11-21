package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerTypeCast extends CompilerTypeBase {
	private static final long serialVersionUID = 1L;
	private CompilerTypeBase value;
	private DataType dataType;
	
	private CompilerTypeCast(CompilerTypeBase value, DataType dataType) {
		super(CompilerType.CAST);
		this.value = value;
		this.dataType = dataType;
	}
	
	public void setValue(CompilerTypeBase value) { this.value = value; }
	
	public CompilerTypeBase getValue() { return this.value; }
	
	public DataType getDataType() { return this.dataType; }
	
	@Override
	public String toString() {
		return "(CAST " + this.value.toString() + " as " + this.dataType + ")";				
	}

	@Override
	public CompilerTypeBase copy() {
		return new CompilerTypeCast(this.value.copy(), dataType);
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return new CompilerTypeCast(this.value.copy(variableList), dataType);
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerTypeCast))
			return false;
		
		CompilerTypeCast otherCast = (CompilerTypeCast)other;
		return this.value.equals(otherCast.getValue()) && (this.dataType == otherCast.getDataType());
	}
	
	public static CompilerTypeBase create(CompilerTypeBase object, DataType dataType) {
		
		if (object.getType() == CompilerType.ARITHMETIC_EXPRESSION) {
			((CompilerArithmeticExpression)object).setDataType(dataType);
			return object;
		}
		
		return new CompilerTypeCast(object, dataType);
	}
}
