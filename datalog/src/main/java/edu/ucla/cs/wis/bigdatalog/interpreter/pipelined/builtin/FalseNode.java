package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class FalseNode extends OrNode {
	
	public FalseNode() {
		super(BuiltInPredicate.FALSE_PREDICATE_NAME, new NodeArguments(), new Binding(0), null);
	}

	public void cleanUp() {
		this.baseNodeCleanUp();
	}

	public boolean initialize() { return true; }

	public void deleteRelationsAndCursors() { 	}

	public void partialCleanUp() {
		this.cleanUp();
	}

	public Status getTuple() {
		Status status = Status.BACKTRACK_SUCCESS;

		this.traceGetTupleEntry();
		this.traceGetTupleExit(status);

		return status;
	}

	public String toString() {
		return toStringNode();
		//return "false";
	}
	
	@Override
	public FalseNode copy(ProgramContext programContext) {
		FalseNode copy = new FalseNode();
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
}