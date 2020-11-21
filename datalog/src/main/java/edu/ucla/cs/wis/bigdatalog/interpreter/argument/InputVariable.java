package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class InputVariable 
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected String name;
	public Argument value;
	protected DataType dataType;

	public InputVariable(String name, DataType dataType) {
		this.name = name;
		this.dataType = dataType;
	}
	
	public InputVariable(String name, Argument value) {
		this.name = name;
		this.value = value;
		if (this.value != null)
			this.dataType = value.getDataType();
	}
	
	public Argument getValue() { return this.value; }

	public DataType getDataType() { return this.dataType; }
	
	public void setDataType(DataType dataType) { this.dataType = dataType; }
	
	public void setValue(Argument argument) { 
		this.value = argument; 
		if (argument != null)
			this.dataType = argument.getDataType();
	}

	public void makeFree() { this.value = null; }

	public boolean isBound() { return this.value != null; }
	
	public boolean isGround() { return this.value != null; }
	
	public boolean isConstant() { return false; }

	public String getName() { return this.name; }

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.name.toString());
		if (this.value != null) {
			output.append(" <= ");
			output.append(this.value.toString());
		}
		//if (this.dataType != null)
		//	output.append("[" + this.dataType.toString() + "]");
		//output.append("[" + this.hashCode() + "]");
		return output.toString();
	}
	
	public InputVariable copy() {
		InputVariable inputVariable = new InputVariable(this.name, this.value);
		if (this.dataType != null)
			inputVariable.dataType = this.dataType;
		return inputVariable;
	}

	public boolean equals(InputVariable other) {
		if (this.value != null && other.getValue() != null) 
			return this.value.equals(other.getValue());
		return false;
	}

	public Argument copy(ArgumentList argumentList) {
		for (Argument arg : argumentList) {
			if ((arg instanceof InputVariable) 
					&& ((InputVariable)arg).getName().equals(this.name))
				return arg;
		}
		InputVariable copy = this.copy();
		argumentList.add(copy);
		return copy;
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		if (!this.isBound())
			throw new InterpreterException("Input variable is not ground.  cannot make db object.");
		
		return this.getValue().toDbType(typeManager);
	}

	@Override
	public Argument reduce() { return this.getValue(); }

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		return this.value.equals(dbTypeObject);
	}

	@Override
	public boolean matchByFree(Argument argument) {
		return this.value.matchByFree(argument);
	}

	@Override
	public boolean matchByBound(Argument freeArgument) {
		return freeArgument.matchByFree(this.value);
	}

	@Override
	public String toFact() {
		StringBuilder fact = new StringBuilder();
		fact.append("argument(");
		fact.append(this.hashCode());
		fact.append(",'inputvariable','");
		fact.append(this.toString());
		fact.append("','");
		fact.append(this.getDataType());
		fact.append("').");
		return fact.toString();		
	}
}
