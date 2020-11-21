package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;

public class IfThenElseNode 
	extends OrNode {
	
	private boolean isIfThenElse;
	
	enum IfThenElseMode {
		IFTE_THEN_MODE, 
		IFTE_ELSE_MODE;
	}
	
	protected IfThenElseMode ifThenElseMode;
		
	public IfThenElseNode() {
		super(BuiltInPredicate.IFTHENELSE_PREDICATE_NAME, new NodeArguments(), new Binding(0), new VariableList());
	}
	
	@Override
	public boolean initialize() { 
		//this.isIfThenElse = (this.children.size() == 3);
		this.isIfThenElse = (this.getNumberOfChildren() == 3);
		return super.initialize();
	}

	//protected boolean isIfThenElse() { return (this.children.size() == 3);}

	public void partialCleanUp() {
		this.baseNodeCleanUp();
		this.orNodePartialCleanUp();
	}

	public String toString() {
		return toStringNode();
	}

	public void getVariables(VariableList variableList) {
		this.getConditionalNode().getVariables(variableList);
		this.getConditionalNode().getBodyVariables(variableList);
		this.getThenNode().getVariables(variableList);
		this.getThenNode().getBodyVariables(variableList);

		//if (this.isIfThenElse()) {
		if (this.isIfThenElse) {
			this.getElseNode().getVariables(variableList);
			this.getElseNode().getBodyVariables(variableList);
	    }
	}

	@Override
	public Status getTuple() {
		Status status = Status.FAIL;

		this.traceGetTupleEntry();

		if (this.isEntry) {
			Status conditionalStatus = this.getConditionalNode().getTuple();

	      // If this "If-Then-Else" node has updates within it,
	      // then we do not backtrack into the "Conditional" node.
	      // In this case upon a successful execution of the
	      // "Conditional" node, we get a status of FAIL.
	      // This puts us in the IFTE_THEN_MODE.
	      // A failure on the "Conditional" node is indicated by
	      // a return status of ENTRY_FAIL. This puts us in the
	      // IFTE_ELSE_MODE.

			if ((conditionalStatus == Status.SUCCESS)) 
				/*||(this.hasUpdate && (conditionalStatus != Status.ENTRY_FAIL))*/
				this.ifThenElseMode = IfThenElseMode.IFTE_THEN_MODE;
			else
				this.ifThenElseMode = IfThenElseMode.IFTE_ELSE_MODE;
	    }

		if (this.ifThenElseMode == IfThenElseMode.IFTE_THEN_MODE) {
			status = this.getThenTuple();
		} else {
			//if (this.getElseNode() != null)
			if (this.isIfThenElse)
				status = this.getElseTuple();
			else if (this.isEntry)
				status = Status.SUCCESS;
			else
				status = Status.FAIL;
	    }

		this.isEntry = (status != Status.SUCCESS);
		/*
		if (status != Status.SUCCESS)
			this.isEntry = true;
		else
			this.isEntry = false;
		 */
		this.traceGetTupleExit(status);

		return status;
	}

	public Status getThenTuple() {
		Status status;
	  // If there are updates within this node, then
	  // do *NOT* backtrack into the "Conditional" node
	  // since it has been "exhausted" of its tuples.

		do {
			status = this.getThenNode().getTuple();
	    } while ((status != Status.SUCCESS)
		     && (this.getConditionalNode().getTuple() == Status.SUCCESS));

		return status;
	}

	public Status getElseTuple() { return this.getElseNode().getTuple(); }
	
	public AndNode getConditionalNode() { return this.getChild(0); }

	public AndNode getThenNode() { return this.getChild(1); }

	public AndNode getElseNode() {
		if (this.isIfThenElse)
			return this.getChild(2);
		
		return null;
	}
	
	@Override
	public IfThenElseNode copy(ProgramContext programContext) {
		IfThenElseNode copy = new IfThenElseNode();
		copy.ifThenElseMode = this.ifThenElseMode;
		
		programContext.getNodeMapping().put(this, copy);
		
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
				 
		return copy;
	}
}
