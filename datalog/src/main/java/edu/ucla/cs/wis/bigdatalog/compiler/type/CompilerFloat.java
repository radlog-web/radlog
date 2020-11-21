package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerFloat extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected float value;
	
	public CompilerFloat(float value) {
		super(CompilerType.COMPILER_FLOAT);
		this.value = value;
	}

	public float getValue() {return this.value;}

	public CompilerFloat copy() {
		//return new CompilerFloat(this.value);
		return this;
	}

	public String toString() {
		return String.valueOf(this.value);
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerFloat))
			return false;
		
		CompilerFloat otherFloat = (CompilerFloat)other;
		return (this.value == otherFloat.getValue());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return typeManager.createFloat(this.value);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.FLOAT;
	}
}
