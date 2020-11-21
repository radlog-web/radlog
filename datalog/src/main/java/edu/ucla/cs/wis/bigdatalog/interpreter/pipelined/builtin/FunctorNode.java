package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbComplex;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;

public abstract class FunctorNode 
	extends OrNode {
	protected DbComplex complexObject;
	protected DbTypeBase functor;
	protected DbTypeBase argumentList;

	public FunctorNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super(BuiltInPredicate.FUNCTOR_PREDICATE_NAME, args, binding, freeVariables);
		this.complexObject = null;
		this.functor = null;
		this.argumentList = null;
	}

	public boolean initialize() { return true; }

	public void deleteRelationsAndCursors() {  }

	public void cleanUp() {
		//this.unsetFreeVariables();
		this.freeVariableList.makeFree();
		this.baseNodeCleanUp();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}

	public String toString() {
		return toStringNode();
	}
}