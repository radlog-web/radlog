package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerString extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected String value;
	
	public CompilerString(String str) {
		super(CompilerType.COMPILER_STRING);
		this.value = str;
	}

	public CompilerString(CompilerString str) {
		this(str.getText());
	}

	public int getLength() {
		if (this.value != null)
			return this.value.length();
		return 0;
	}

	public String getText() {return this.value;}

	@Override
	public CompilerString copy() {
		//return new CompilerString(this.value);
		return this;
	}
	
	@Override
	public CompilerString copy(CompilerVariableList variableList) {
		return copy();
	}

	public String toString() {
		if (this.value == null)
			return "";
		
		return this.value;
	}
	
	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerString))
			return false;
		
		CompilerString otherString = (CompilerString)other;
		return this.value.equals(otherString.getText());
	}
	
	@Override
	public boolean isConstant() { return true; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return typeManager.createString(this.value);
	}
	
	public static CompilerTypeBase create(String value) {
		CompilerTypeBase compilerTypeObject = CompilerDateTime.create(value);	
		if (compilerTypeObject == null)
			return new CompilerString(value);
		
		return compilerTypeObject;
	}
	
	@Override
	public DataType getDataType() {
		return DataType.STRING;
	}
}
