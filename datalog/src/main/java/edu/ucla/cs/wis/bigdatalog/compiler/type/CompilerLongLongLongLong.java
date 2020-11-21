package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerLongLongLongLong extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	// this is just a placeholder class, so the long type is fine - for now APS 4/11/2014
	protected long value;
	
	public CompilerLongLongLongLong(long value) {
		super(CompilerType.COMPILER_LONGLONGLONGLONG);
		this.value = value;
	}

	public long getValue() {return this.value;}

	public CompilerLongLongLongLong copy() {
		//return new CompilerLongLongLongLong(this.value);
		return this;
	}

	public String toString() {
		return Long.toString(this.value);
	}

	public boolean equals(CompilerTypeBase other)  {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerLongLongLongLong))
			return false;
		
		CompilerLongLongLongLong otherLong = (CompilerLongLongLongLong)other;
		
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
		return typeManager.createLongLongLongLong(this.value);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.LONGLONGLONGLONG;
	}
}
