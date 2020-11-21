package edu.ucla.cs.wis.bigdatalog.compiler.variable;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;

public class CompilerVariableList {
	public static int globalVariableCount = 1;
	public static CompilerTypeList globalVariableList = null;
	public CompilerVariable[] variables;
	
	public CompilerVariableList(int size) {
		this.variables = new CompilerVariable[size];
	}
	
	public CompilerVariableList() {
		this(0);
	}
	
	public CompilerVariableList(CompilerTypeList compilerTypeList) {
		this.variables = new CompilerVariable[compilerTypeList.size()];
		for (int i = 0; i < compilerTypeList.size(); i++) {			
			if (!compilerTypeList.get(i).isVariable())
				throw new CompilerException("Cannot load VariableList.  Given CompilerTypeList must contain only variables.");
			
			this.variables[i] = (CompilerVariable)compilerTypeList.get(i);
		}
	}

	public void reset() {
		this.variables = new CompilerVariable[0];
	}
	
	public CompilerVariable getVariable(String name) {
		for (int i = 0; i < this.variables.length; i++) {
			if (this.variables[i].getVariableName().equals(name))
				return this.variables[i];
		}

		CompilerVariable variable = new CompilerVariable(name);
		this.append(variable);
				
		return variable;
	}
		
	public CompilerVariable getVariable(CompilerVariable variable) {
		return this.getVariable(variable.getVariableName());
	}

	public void add(CompilerVariable variable) {
		for (int i = 0; i < this.variables.length; i++)
			if (this.variables[i].equals(variable))
				return;
				
		this.append(variable);
	}

	public CompilerVariableList copy() {
		CompilerVariableList newList = new CompilerVariableList(this.variables.length);
		for (int i = 0; i < this.variables.length; i++)
			newList.set(i, this.variables[i].deepCopy());

		return newList;	  
	}
	
	public CompilerVariableList copy(CompilerVariableList variableList) {
		CompilerVariableList newList = new CompilerVariableList(this.variables.length);
		for (int i = 0; i < this.variables.length; i++)
			newList.set(i, this.variables[i].copy(variableList));

		return newList;
	}
	
	public CompilerVariableList copyList() {
		CompilerVariableList newList = new CompilerVariableList(this.variables.length);
		for (int i = 0; i < this.variables.length; i++)
			newList.set(i, this.variables[i]);

		return newList;
	}

	public void appendVariablesToList(CompilerTypeList list) {
		for (int i = 0; i < this.variables.length; i++)
			list.add(this.variables[i]);
	}
	
	public void appendArrayToVariables(CompilerVariable[] array) {
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

	public CompilerTypeList toCompilerTypeList() {
		CompilerTypeList ctl = new CompilerTypeList();
		
		for (int i = 0; i < this.variables.length; i++)
			ctl.add(this.variables[i]);
		
		return ctl;
	}

	public void appendList(CompilerVariableList variableList) {
		for (int i = 0; i < variableList.size(); i++)
			this.append(variableList.get(i));
	}
	
	public void appendListUnique(CompilerVariableList variableList) {
		for (int i = 0; i < variableList.size(); i++)
			if (!this.contains(variableList.get(i)))
				this.append(variableList.get(i));		
	}
	
	// put the argument list in front of this list
	public void prependList(CompilerVariableList variableList) {
		if (variableList == null)
			return;
		for (int i = variableList.size() - 1; i >= 0; i--)
			this.prepend(variableList.get(i));
	}
	
	public boolean contains(CompilerVariable variable) {
		for (int i = 0; i < this.variables.length; i++)
			if (this.variables[i].equals(variable))
				return true;
		return false;
	}
	
	public void clear() {
		this.variables = new CompilerVariable[0];
	}
	
	public int size() {
		return this.variables.length;
	}
	
	public CompilerVariable get(int position) {
		return this.variables[position];
	}
	
	public CompilerVariable remove(int position) {
		CompilerVariable variable = this.variables[position];
		this.shiftLeft(position);
		this.shrink(1);
		return variable;
	}

	public void set(int position, CompilerVariable variable) {
		this.variables[position] = variable;
	}
	
	public boolean isEmpty() {
		return (this.variables.length == 0);
	}
	
	public int getPosition(CompilerVariable variable) {
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
		CompilerVariable[] temp = new CompilerVariable[this.variables.length + numberToGrowBy];
		for (int i = 0; i < this.variables.length; i++)
			temp[i] = this.variables[i];
	
		this.variables = temp;
	}
	
	private void shrink(int numberToShrinkBy) {
		if ((this.variables.length - numberToShrinkBy) < 0) {
			this.variables = new CompilerVariable[0];
		} else {
			// always the tail
			CompilerVariable[] temp = new CompilerVariable[this.variables.length - numberToShrinkBy];
			for (int i = 0; i < this.variables.length - numberToShrinkBy; i++)
				temp[i] = this.variables[i];
		
			this.variables = temp;
		}
	}
	
	private void append(CompilerVariable variable) {
		this.grow(1);
		this.variables[this.variables.length - 1] = variable;
	}

	private void prepend(CompilerVariable variable) {
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
