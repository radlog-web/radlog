package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.Expression;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;

public abstract class ComparisonNode 
	extends OrNode {
	
	public ComparisonNode(String name, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(name, args, binding, freeVariables);
	}

	@Override
	public boolean initialize() { return true; }

	@Override
	public void partialCleanUp() {
		this.cleanUp();
	}

	@Override
	public void deleteRelationsAndCursors() { }

	@Override
	public String toString() { return toStringNode(); }
	
	public boolean hasArithmeticArguments() {		
		for (int i = 0; i < 2; i++)
			if (this.getArgument(i) instanceof Expression)
				return true;
				
		return false;
	}
}
