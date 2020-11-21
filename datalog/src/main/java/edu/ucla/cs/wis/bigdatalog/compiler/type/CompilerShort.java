package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerShort extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected short value;
	
	public CompilerShort(short value) {
		super(CompilerType.COMPILER_SHORT);
		this.value = value;
	}

	public short getValue() {return this.value;}

	public CompilerShort copy() {
		//return new CompilerShort(this.value);
		return this;
	}

	public String toString() {
		return Short.toString(this.value);
	}

	public boolean equals(CompilerTypeBase other)  {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerShort))
			return false;
		
		CompilerShort otherShort = (CompilerShort)other;
		
		return (this.value == otherShort.getValue());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return typeManager.createShort(this.value);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.SHORT;
	}
}
