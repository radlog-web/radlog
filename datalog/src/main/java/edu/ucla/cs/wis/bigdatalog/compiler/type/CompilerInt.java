package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerInt extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected int value;
	
	public CompilerInt(int value) {
		super(CompilerType.COMPILER_INT);
		this.value = value;
	}

	public int getValue() {return this.value;}

	public CompilerInt copy() {
		//return new CompilerInt(this.value);
		return this;
	}

	public String toString() {
		return Integer.toString(this.value);
	}

	public boolean equals(CompilerTypeBase other)  {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerInt))
			return false;
		
		CompilerInt otherInt = (CompilerInt)other;
		
		return (this.value == otherInt.getValue());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return typeManager.createInt(this.value);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.INT;
	}
}
