package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

public class VariableList {
	public static int globalVariableCount = 1;
	public Variable[] variables;
	
	public VariableList(int size) {
		this.variables = new Variable[size];
	}
	
	public VariableList() {
		this(0);
	}

	public void reset() {
		this.variables = new Variable[0];
	}
	
	public Variable getVariable(String name) {
		for (int i = 0; i < this.variables.length; i++) {
			if (this.variables[i].getName().equals(name))
				return this.variables[i];
		}

		Variable variable = new Variable(name);
		this.append(variable);
				
		return variable;
	}
		
	public Variable getVariable(Variable variable) {
		return this.getVariable(variable.getName());
	}

	public void add(Variable variable) {
		for (int i = 0; i < this.variables.length; i++)
			if (this.variables[i].equals(variable))
				return;
				
		this.append(variable);
	}

	public VariableList copy() {
		VariableList newList = new VariableList(this.variables.length);
		for (int i = 0; i < this.variables.length; i++)
			newList.set(i, this.variables[i].deepCopy());

		return newList;	  
	}
	
	public VariableList copy(VariableList variableList) {
		VariableList newList = new VariableList(this.variables.length);
		for (int i = 0; i < this.variables.length; i++)
			newList.set(i, this.variables[i].copy(variableList));

		return newList;
	}
	
	public VariableList copy(ArgumentList argumentList) {
		VariableList variableList = argumentList.getVariables();
		
		VariableList newList = new VariableList(this.variables.length);
		for (int i = 0; i < this.variables.length; i++) {
			Variable var = this.variables[i].copy(variableList);
			if (!argumentList.contains(var))
				argumentList.add(var);
			newList.set(i, var);
		}

		return newList;
	}
	
	public VariableList copyList() {
		VariableList newList = new VariableList(this.variables.length);
		for (int i = 0; i < this.variables.length; i++)
			newList.set(i, this.variables[i]);

		return newList;
	}
	
	public void appendArrayToVariables(Variable[] array) {
		grow(array.length);
		for (int i = 0; i < array.length; i++)
			append(array[i]);
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < this.variables.length; i++) {
			if (i > 0)
				output.append("\n");
			output.append(this.variables[i].toString());
		}
		return output.toString();
	}

	public void appendList(VariableList variableList) {
		for (int i = 0; i < variableList.size(); i++)
			this.append(variableList.get(i));
	}
	
	public void appendListUnique(VariableList variableList) {
		for (int i = 0; i < variableList.size(); i++)
			if (!this.contains(variableList.get(i)))
				this.append(variableList.get(i));		
	}
	
	// put the argument list in front of this list
	public void prependList(VariableList variableList) {
		if (variableList == null)
			return;
		for (int i = variableList.size() - 1; i >= 0; i--)
			this.prepend(variableList.get(i));
	}
	
	public boolean contains(Variable variable) {
		for (int i = 0; i < this.variables.length; i++)
			if (this.variables[i].equals(variable))
				return true;
		return false;
	}
	
	public void clear() {
		this.variables = new Variable[0];
	}
	
	public int size() {
		return this.variables.length;
	}
	
	public Variable get(int position) {
		return this.variables[position];
	}
	
	public Variable remove(int position) {
		Variable variable = this.variables[position];
		this.shiftLeft(position);
		this.shrink(1);
		return variable;
	}
	
	public Variable remove(Variable variable) {
		for (int i = 0; i < this.variables.length; i++) {
			if (this.variables[i] == variable) {
				this.shiftLeft(i);
				this.shrink(1);
				return variable;
			}
		}
		return null;
	}

	public void set(int position, Variable variable) {
		this.variables[position] = variable;
	}
	
	public boolean isEmpty() {
		return (this.variables.length == 0);
	}
	
	public int getPosition(Variable variable) {
		for (int i = 0; i < this.variables.length; i++) {
			if (this.variables[i].equals(variable))
				return i;
		}
		
		return -1;
	}
	
	public void makeFree() {
		for (int i = 0; i < this.variables.length; i++)
			this.variables[i].value = null;		
	}
	
	private void grow(int numberToGrowBy) {
		Variable[] temp = new Variable[this.variables.length + numberToGrowBy];
		for (int i = 0; i < this.variables.length; i++)
			temp[i] = this.variables[i];
	
		this.variables = temp;
	}
	
	private void shrink(int numberToShrinkBy) {
		if ((this.variables.length - numberToShrinkBy) < 0) {
			this.variables = new Variable[0];
		} else {
			// always the tail
			Variable[] temp = new Variable[this.variables.length - numberToShrinkBy];
			for (int i = 0; i < this.variables.length - numberToShrinkBy; i++)
				temp[i] = this.variables[i];
		
			this.variables = temp;
		}
	}
	
	private void append(Variable variable) {
		this.grow(1);
		this.variables[this.variables.length - 1] = variable;
	}

	private void prepend(Variable variable) {
		this.grow(1);
		this.shiftRight(0);
		this.variables[0] = variable;
	}
	
	private void shiftLeft(int position) {
		for (int i = position + 1; i < this.variables.length; i++)
			this.variables[i-1] = this.variables[i];	
	}
	
	private void shiftRight(int position) {
		for (int i = this.variables.length - 1; i > position; i--)
			this.variables[i] = this.variables[i - 1];	
	}
}
