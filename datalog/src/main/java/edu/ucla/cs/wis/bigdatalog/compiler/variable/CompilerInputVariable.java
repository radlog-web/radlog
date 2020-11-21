package edu.ucla.cs.wis.bigdatalog.compiler.variable;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerInputVariable 
	extends CompilerTypeBase  {
	protected String name;
	public CompilerTypeBase value;
	protected DataType dataType;

	public CompilerInputVariable(String name) {
		super(CompilerType.INPUT_VARIABLE);
		this.name = name;
	}

	public CompilerTypeBase getValue() { return this.value; }

	public DataType getDataType() { return this.dataType; }
	
	public void setDataType(DataType dataType) { this.dataType = dataType; }
	
	public void setValue(CompilerTypeBase argument) { 
		this.value = argument; 
		//if (argument != null)
			//this.dataType = argument.getDataType();
	}

	public void makeFree() { this.value = null; }

	public boolean isBound() { return this.value != null; }

	public String getName() { return this.name; }

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append(this.name.toString());
		if (this.value != null) {
			retval.append(" <= ");
			retval.append(this.value.toString());
		}
		return retval.toString();
	}
	
	public CompilerInputVariable copy() {
		CompilerInputVariable inputVariable = new CompilerInputVariable(this.name);
		inputVariable.value = this.value;
		if (this.dataType != null)
			inputVariable.dataType = this.dataType;
		return inputVariable;
	}

	public boolean equals(CompilerInputVariable other) {
		if (this.value != null && other.getValue() != null) 
			return this.value.equals(other.getValue());
		return false;
	}

	public boolean equals(CompilerTypeBase other) {
		if (this.value != null && other != null) 
			return this.value.equals(other);		
		return false;
	}

	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}
}
