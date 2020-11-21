package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class NegationOrNode extends OrNode {
	
	protected OrNode literalToBeNegated;
	
	public NegationOrNode(OrNode literalToBeNegated) {
		super(BuiltInPredicate.NEGATION_PREDICATE_NAME, new NodeArguments(), new Binding(0), new VariableList());
		this.literalToBeNegated = literalToBeNegated;
	}
	
	public OrNode getLiteralToBeNegated() { return this.literalToBeNegated; }

	@Override
	public void cleanUp() {
		this.baseNodeCleanUp();
		this.literalToBeNegated.cleanUp();
	}

	@Override
	public boolean initialize() {
		return this.literalToBeNegated.initialize();
	}
	
	@Override
	public void deleteRelationsAndCursors() {
		this.literalToBeNegated.deleteRelationsAndCursors();
	}
	
	@Override
	public void partialCleanUp() {
		this.baseNodeCleanUp();
		this.literalToBeNegated.partialCleanUp();
	}

	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			if (this.literalToBeNegated.getTuple() == Status.SUCCESS) {
				this.literalToBeNegated.partialCleanUp();
				status = Status.ENTRY_FAIL;
			} else {
				this.isEntry = false;
				status = Status.SUCCESS;
			}
	    } else {
	    	this.isEntry = true;
	    	this.literalToBeNegated.partialCleanUp();
	    	status = Status.FAIL;
	    }

		this.traceGetTupleExit(status);

		return status;
	}

	@Override
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append(this.toStringNode());
		retval.append(this.literalToBeNegated.toStringTree());
		return retval.toString();
	}
	
	@Override
	public void getVariables(VariableList variableList) {
		this.literalToBeNegated.getVariables(variableList);
	}
	
	@Override
	public NegationOrNode copy(ProgramContext programContext) {
		NegationOrNode copy = new NegationOrNode(this.literalToBeNegated.copy(programContext));
		programContext.getNodeMapping().put(this, copy);
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.literalToBeNegated.attachContext(deALSContext);
	}
}
