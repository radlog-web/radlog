package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeStack;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.AggregateRelationNode;

public class OrNode 
	extends Node<AndNode> {
	protected final boolean 	hasAllArgumentsBound;
	protected VariableList	 	freeVariableList;
	
	protected static NodeStack<AggregateRelationNode> readAggregateNodes = new NodeStack<>();
		
	public OrNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName, args, binding);
		this.hasAllArgumentsBound = binding.allBound();		
		this.freeVariableList = freeVariables;
	}
	
	public VariableList getFreeVariableList() {
		return this.freeVariableList;
	}
	
	public void cleanUp() {
		this.baseNodeCleanUp();
		for (AndNode andNode : this.getChildren())
			andNode.cleanUp();
	}

	public boolean initialize() { 
		for (AndNode andNode : this.getChildren())
			if (!andNode.initialize())
				return false;
		
		return true; 
	}

	public void deleteRelationsAndCursors() {
		for (AndNode andNode : this.getChildren())
			andNode.deleteRelationsAndCursors();
	}

	public void partialCleanUp() {
		this.baseNodeCleanUp();
		this.orNodePartialCleanUp();
	}

	public Status getTuple() {
		this.traceGetTupleEntry();

		Status status = this.getOrNodeTuple();

		this.traceGetTupleExit(status);

		return status;
	}
	
	public Status getOrNodeTuple() {
		Status status = Status.FAIL;
		
		if (!this.isEntry && this.hasAllArgumentsBound) {
			this.partialCleanUp();
		} else {
			while (this.currentChildIndex < this.children.length && status != Status.SUCCESS) {
				depth++;
				status = this.children[this.currentChildIndex++].getTuple();
				depth--;
			}
			
			if (DEBUG && this.deALSContext.isDerivationTrackingEnabled() && (this.currentChildIndex > 0)) {
				StringBuilder retval = new StringBuilder();
				for (int i = 0; i < depth; i++) retval.append(" ");
				retval.append(this.getChild(this.currentChildIndex-1).toStringWithAssignments());
				this.logDerivationTracking(retval.toString());
			}
			
			if (status != Status.SUCCESS) {
				this.currentChildIndex = 0;

				// It was like this but this is wrong because if this or node is not
				// on entry and yet one of the indexed rule fails on entry, the status
				// will inherit the status of the entry-failed rule. As a result, this
				// or node will get entry fail instead of normal fail - KL Ong /10/15/92
				if (this.isEntry)
					status = Status.ENTRY_FAIL;

				this.isEntry = true;
			} else {
				this.isEntry = false;
				// If we succeeded, we undo the increment of the rule index
				this.currentChildIndex--;	
			}
	    }

		return status;
	}

	protected void orNodePartialCleanUp() {
		for (AndNode andNode : this.getChildren())
			andNode.partialCleanUp();
	}

	public void overWriteFreeVariableList(VariableList newFreeVariableList) {	
		this.freeVariableList = newFreeVariableList;
	}

	@Override
	public OrNode copy(ProgramContext programContext) {
		OrNode copy = new OrNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		copy.xyPredicateType = this.xyPredicateType;
		
		// copy all children
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
		
		return copy;
	}
}
