package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.util.ArrayList;
import java.util.Collection;

public class ArgumentList 
	extends ArrayList<Argument> {
	private static final long serialVersionUID = 1L;

	public ArgumentList() {}

	public ArgumentList(int initialCapacity) {
		super(initialCapacity);
	}

	public ArgumentList(Collection<Argument> c) {
		super(c);
	}
	
	public VariableList getVariables() {
		VariableList variableList = new VariableList();
		for (Argument argument : this) {
			if (argument instanceof Variable)
				variableList.add((Variable)argument);
		}
		return variableList;
	}

}
