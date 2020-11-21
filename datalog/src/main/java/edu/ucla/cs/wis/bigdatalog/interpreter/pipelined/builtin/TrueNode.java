package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class TrueNode 
	extends OrNode {
	
	public TrueNode() {
		super(BuiltInPredicate.TRUE_PREDICATE_NAME, new NodeArguments(), new Binding(0), null);
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
		Status status;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			this.isEntry = false;
			status = Status.SUCCESS;
	    } else {
	    	this.isEntry = true;
	    	status = Status.FAIL;
	    }

		this.traceGetTupleExit(status);

		return status;
	}

	public String toString() { 
		return toStringNode();
		//return "true";
	}
	
	@Override
	public TrueNode copy(ProgramContext programContext) {
		TrueNode copy = new TrueNode();
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
}
