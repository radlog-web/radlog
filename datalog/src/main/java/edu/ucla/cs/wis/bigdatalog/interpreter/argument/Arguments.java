package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

public interface Arguments {

	int size();
	
	boolean add(Argument argument);
	
	Argument get(int index);
	
	boolean contains(Argument argument);
	
	Argument set(int index, Argument argument);
}
