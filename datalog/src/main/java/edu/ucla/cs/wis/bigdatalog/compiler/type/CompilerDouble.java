package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerDouble extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected double value;
	
	public CompilerDouble(double value) {
		super(CompilerType.COMPILER_DOUBLE);
		this.value = value;
	}

	public double getValue() {return this.value;}

	public CompilerDouble copy() {
		//return new CompilerDouble(this.value);
		return this;
	}

	public String toString() {
		return String.valueOf(this.value);
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerDouble))
			return false;
		
		CompilerDouble otherDouble = (CompilerDouble)other;
		return (this.value == otherDouble.getValue());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return typeManager.createDouble(this.value);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.DOUBLE;
	}
}
