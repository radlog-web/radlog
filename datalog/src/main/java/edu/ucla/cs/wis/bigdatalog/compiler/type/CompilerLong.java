package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerLong extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected long value;
	
	public CompilerLong(long value) {
		super(CompilerType.COMPILER_LONG);
		this.value = value;
	}

	public long getValue() {return this.value;}

	public CompilerLong copy() {
		//return new CompilerLong(this.value);
		return this;
	}

	public String toString() {
		return Long.toString(this.value);
	}

	public boolean equals(CompilerTypeBase other)  {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerLong))
			return false;
		
		CompilerLong otherLong = (CompilerLong)other;
		
		return (this.value == otherLong.getValue());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return typeManager.createLong(this.value);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.LONG;
	}
}
