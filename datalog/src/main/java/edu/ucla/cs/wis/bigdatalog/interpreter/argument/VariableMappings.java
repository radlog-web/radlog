package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;

public class VariableMappings {
	public List<CompilerVariable> compilerVariables;
	public List<Variable> variables;
		
	public VariableMappings() {
		this.compilerVariables = new ArrayList<>();
		this.variables = new ArrayList<>();
	}
	
	public int getNumberOfVariables() { return this.compilerVariables.size(); }
		
	public Variable getVariable(CompilerVariable compilerVariable) {
		int i;
		
		for (i = 0; i < this.compilerVariables.size(); i++)
			if (this.compilerVariables.get(i) == compilerVariable)
				return this.variables.get(i);
		
		for (i = 0; i < this.compilerVariables.size(); i++)
			if (this.compilerVariables.get(i).getVariableName().equals(compilerVariable.getVariableName()))
				break;
		
		if (i >= this.compilerVariables.size())
			return null;
		
		return this.variables.get(i);
	}
	
	public CompilerVariable getCompilerVariable(Variable variable) {
		int i;
		for (i = 0; i < this.variables.size(); i++)
			if (this.variables.get(i) == variable)
				break;
		
		if (i >= this.compilerVariables.size())
			return null;
		
		return this.compilerVariables.get(i);
	}
	
	public CompilerVariable getCompilerVariableByIndex(int index) { return this.compilerVariables.get(index); }
	
	public Variable getVariableByIndex(int index) { return this.variables.get(index); }
	
	public Variable getVariableByName(String variableName) {
		for (Variable var : this.variables)
			if (var.getName().equals(variableName))
				return var;
		return null;
	}
	
	public void put(CompilerVariable compilerVariable, Variable variable) {
		this.compilerVariables.add(compilerVariable);
		this.variables.add(variable);
	}
	
	public void merge(VariableMappings variableMappings) {
		for (CompilerVariable cv : variableMappings.compilerVariables)
			//if (!this.compilerVariables.contains(cv))
				this.compilerVariables.add(cv);
		
		for(Variable v : variableMappings.variables)
			//if (!this.variables.contains(v))
				this.variables.add(v);
	}

	public void clear() {
		this.compilerVariables.clear();
		this.variables.clear();
	}
	
	public VariableMappings copy() {
		VariableMappings copy = new VariableMappings();
		for (CompilerVariable cv : this.compilerVariables)
			copy.compilerVariables.add(cv);
		
		for (Variable v : this.variables)
			copy.variables.add(v);
		
		return copy;
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("Compiler Variables:\n");
		for (int i = 0; i < this.compilerVariables.size(); i++) {
			if (i > 0)
				output.append(", ");
			output.append(this.compilerVariables.get(i).toString());
		}
		
		output.append("\nVariables:\n");
		for (int i = 0; i < this.variables.size(); i++) {
			if (i > 0)
				output.append(", ");
			output.append(this.variables.get(i).toString());
		}
		return output.toString();
	}
}
