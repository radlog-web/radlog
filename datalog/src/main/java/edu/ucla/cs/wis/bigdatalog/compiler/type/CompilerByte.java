package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerByte extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected byte value;
	
	public CompilerByte(byte value) {
		super(CompilerType.COMPILER_BYTE);
		this.value = value;
	}

	public byte getValue() {return this.value;}

	public void setValue(byte value) {
		this.value = value;
	}

	public CompilerByte copy() {
		//return new CompilerByte(this.value);
		return this;
	}

	public String toString() {
		return Byte.toString(this.value);
	}

	public boolean equals(CompilerTypeBase other)  {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerByte))
			return false;
		
		CompilerByte otherByte = (CompilerByte)other;
		
		return (this.value == otherByte.getValue());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return typeManager.createByte(this.value);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.BYTE;
	}
}
