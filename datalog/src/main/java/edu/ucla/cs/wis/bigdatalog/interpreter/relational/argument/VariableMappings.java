package edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;

public class VariableMappings {

	HashMap<CompilerVariable, Variable> mappings;
	
	private VariableMappings(HashMap<CompilerVariable, Variable> mappings) {
		this.mappings = mappings;
	}
	
	public VariableMappings() {
		this.mappings = new LinkedHashMap<>();
	}
	
	public int size() { return this.mappings.size(); }
	
	public Variable get(CompilerVariable compilerVariable) {
		if (this.mappings.containsKey(compilerVariable))
			return this.mappings.get(compilerVariable);
		return null;					
	}
	
	public CompilerVariable get(Variable variable) {
		for (Map.Entry<CompilerVariable, Variable> entry : this.mappings.entrySet())
			if (entry.getValue().equals(variable))
				return entry.getKey();
		return null;
	}
	
	public Argument getByName(String name) {
		for (Map.Entry<CompilerVariable, Variable> entry : this.mappings.entrySet())
			if (entry.getKey().getVariableName().equals(name))
				return entry.getValue();
		return null;
	}
	
	public void put(CompilerVariable compilerVariable, Variable planArgument) {
		if (!this.mappings.containsKey(compilerVariable))
			this.mappings.put(compilerVariable, planArgument);
	}
	
	public void merge(VariableMappings other) {
		for (Map.Entry<CompilerVariable, Variable> entry : other.mappings.entrySet())
			this.put(entry.getKey(),  entry.getValue());
	}
	
	public VariableMappings diff(VariableMappings other) {
		VariableMappings diff = new VariableMappings();
		for (Map.Entry<CompilerVariable, Variable> entry : this.mappings.entrySet())
			if (!other.mappings.containsKey(entry.getKey()))
				diff.put(entry.getKey(),  entry.getValue());
		return diff;
	}
	
	public VariableMappings copy() {
		VariableMappings copy = new VariableMappings();
		for (Map.Entry<CompilerVariable, Variable> entry : this.mappings.entrySet()) 
			copy.put(entry.getKey(),  entry.getValue());
	
		return copy;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		int counter = 0;
		for (Map.Entry<CompilerVariable, Variable> entry : this.mappings.entrySet()) {
			if (counter > 0)
				output.append("\n");
			output.append(entry.getKey().toString());
			output.append(" -> ");
			output.append(entry.getValue().toString());
			counter++;
		}
		
		return output.toString();
	}
	
	public void clear() {
		this.mappings.clear();
	}
}
