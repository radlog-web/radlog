package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;

public abstract class MemberNode 
	extends OrNode {
	
	public MemberNode(String name, NodeArguments args, Binding binding, VariableList freeVariables) {
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
	public String toString() {
		return toStringNode();
	}
}
