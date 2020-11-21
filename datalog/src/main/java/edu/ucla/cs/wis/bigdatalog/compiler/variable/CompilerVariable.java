package edu.ucla.cs.wis.bigdatalog.compiler.variable;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerVariable 
	extends CompilerTypeBase {
	private static final long serialVersionUID = 1L;
	public static final String ANONYMOUS_VARIABLE_NAME = "_";
	public static final String ANONYMOUS_PREFIX	= "_$anonymous$";

	protected String name;
	public CompilerTypeBase value;
	protected DataType dataType;
	
	public CompilerVariable(String name) {
		this(name, null);
	}
	
	public CompilerVariable(String name, CompilerTypeBase value) {
		super(CompilerType.VARIABLE);

		if (name.equals(ANONYMOUS_VARIABLE_NAME))
			this.name = ANONYMOUS_PREFIX + String.valueOf(CompilerVariableList.globalVariableCount++);
		else
			this.name = name;

		this.value = value;

		if (this.value != null && this.value.isConstant())
			this.dataType = this.value.getDataType();
			//this.dataType = ((DbConvertible)value).toDbType().getDataType();
		
		if (this.dataType == null)
			this.dataType = DataType.UNKNOWN;
	} 

	public CompilerTypeBase getValue() { return this.value;}

	public void setValue(CompilerTypeBase value) {
		if (value instanceof CompilerVariable) {
			CompilerVariable varValue = (CompilerVariable)value;
			if (varValue.dataType == DataType.UNKNOWN) {
				varValue.dataType = this.dataType;
			} else {
				if (this.dataType == DataType.UNKNOWN)
					this.dataType = varValue.dataType;
			}
		}

		this.value = value; 
	}

	public DataType getDataType() { return this.dataType; }
	
	public void setDataType(DataType dataType) { 
		this.dataType = dataType; 
	}

	public boolean isAnonymous() { return this.name.startsWith(ANONYMOUS_PREFIX); }

	public String getVariableName() { return this.name;}

	public void renameVariableName() {
		this.name += "." + String.valueOf(CompilerVariableList.globalVariableCount++);
	}
	
	public void rename(String name) {
		this.name = name;
	}

	public boolean isBound() { return this.value != null;}
	
	public boolean hasValueAssigned() {
		if (!this.isBound())
			return false;
			
		return this.deepDereference().isBound();
	}
	
	public String toStringVariableName() {
		if (this.isAnonymous())
			return CompilerVariable.ANONYMOUS_VARIABLE_NAME;
		return this.name.toString();
	}

	public CompilerTypeBase deepDereference() {
		CompilerTypeBase temp = this;

		while (temp instanceof CompilerVariable && (((CompilerVariable)temp).value != null))
			temp = ((CompilerVariable)temp).value;

		if (temp instanceof CompilerInputVariable && ((CompilerInputVariable)temp).isBound())
			temp = ((CompilerInputVariable)temp).getValue();

		return temp;
	}
	
	public void makeFree() { this.value = null; }
	
	public CompilerTypeBase dereference() {
		if (this.value == null)
			return this;
		
		return this.value;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		/*if (this.deALSContext.isDebugEnabled()) {
			output.append(this.toStringVariableName());

			if (this.value != null) {
				output.append(" -> ");
				output.append(this.value.toString());
			}
		} else {*/
		CompilerTypeBase argument = this.deepDereference();

		// Display the dereferenced object if it is not a variable
		if (argument instanceof CompilerVariable)
			output.append(this.toStringVariableName());
		else
			output.append(argument.toString());
		//}
		
		//if (Runtime.isDebugEnabled())	output.append("[" + this.hashCode() + "]");
		//if (Runtime.isDebugEnabled())	output.append("[" + this.dataType + "]");
		
		return output.toString();
	}

	public CompilerVariable copy() {
		return this;
	}

	public CompilerVariable copy(CompilerVariableList variableList) {
		CompilerVariable var = null;
		//APS 1/2/2014 - don't use existing anonymous variables, since the lookup is by name
		// without this, anonymous variables could be shared between literals which is horrible w/ bound values  
		//if (this.isAnonymous())
		//	var = new Variable(ANONYMOUS_VARIABLE_NAME);
		//else
			var = variableList.getVariable(this);
				
		if (var != null)
			var.setDataType(this.getDataType());
				
		return var;
	}

	public CompilerVariable deepCopy() {
		CompilerVariable copy = new CompilerVariable(this.name);
		if (this.value != null)
			copy.setValue(this.value);
		copy.dataType = this.dataType;
		return copy;
	}
	
	@Override
	public boolean equals(CompilerTypeBase other) {
		return (this == other);
	}
	
	public static CompilerVariable generateAnonymousVariable() {
		return new CompilerVariable(ANONYMOUS_VARIABLE_NAME);
	}
}
