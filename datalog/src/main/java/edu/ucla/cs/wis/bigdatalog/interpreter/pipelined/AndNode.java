package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYPredicateType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.XYRecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.XYStageVariableNode;

public class AndNode 
	extends Node<OrNode> {

	protected int[] 	backtrackMap;
	protected int 		ruleBacktrackPoint;
	
	public AndNode(String predicateName, NodeArguments args, Binding binding) {
		super(predicateName, args, binding);
		this.backtrackMap = null;
		this.ruleBacktrackPoint = -2;
	}
	
	public void getBodyVariables(VariableList variableList) {
		if (this.hasChildren())
			for (OrNode node : this.getChildren())
				node.getVariables(variableList);
	}

	// here for ChoiceNode to use
	protected void cleanUpLiteralsInRange(int leftLiteral, int rightLiteral) {
		for (int i = rightLiteral; i > leftLiteral; i--)
			this.children[i].partialCleanUp();
	}

	protected Status getAndNodeTuple() {		
		Status	status;
		int 	numberOfChildren = this.children.length;
		int 	index;

		if (!this.isEntry && (this.ruleBacktrackPoint != this.currentChildIndex)) {
			// If we are here, then we are backtracking into this rule.  And the last literal 
			// of this rule does not contribute any variables to the head.
			for (int i = (numberOfChildren - 1); i > this.ruleBacktrackPoint; i--)
				this.children[i].partialCleanUp();

			// If this.ruleBacktrackPoint is equal to -1, no literals can contribute
			// any new values and thus, we should fail out of this and node, K.L.Ong 2/28/93
			if (this.ruleBacktrackPoint == -1) {
				this.currentChildIndex = 0;
				return Status.FAIL;
			}
			index = this.ruleBacktrackPoint;
		} else {
			index = this.currentChildIndex;
		}

		while (index < numberOfChildren) {
			status = this.children[index].getTuple();
			
			if (DEBUG && this.deALSContext.isDerivationTrackingEnabled()) {
				StringBuilder output = new StringBuilder();
				for (int i = 0; i < depth; i++) output.append(" ");

				output.append(this.getChild(index).toStringWithAssignments());
				output.append(" " + status.name());
				this.logDerivationTracking("{}", output.toString());
				//logger.trace(output.toString());
			}
			
			//Node.numberOfPredicateEvaluations++;
			
			if (status == Status.SUCCESS) {
				index++;
			} else if ((status == Status.BACKTRACK_SUCCESS) 
					|| (status == Status.FAIL)) {
				status = Status.FAIL;
				if (index == 0) {
					this.currentChildIndex = 0;
					return status;
				}
				// cause a backtrack 
				index--;
			} else {
				if (index == 0) {
					this.currentChildIndex = 0;
					return status;
				}

				for (int i = (index - 1); i > this.backtrackMap[index]; i--)
					this.children[i].partialCleanUp();

				if (this.backtrackMap[index] == -1) {
					this.currentChildIndex = 0;
					return status;
				}		 				
				index = this.backtrackMap[index];
			}
		}
		
		// go to last literal
		this.currentChildIndex = numberOfChildren - 1;
	
		return Status.SUCCESS;
	}

	public Status getTuple() {
		this.traceGetTupleEntry();

		Status status = this.getAndNodeTuple();

		this.isEntry = !(status == Status.SUCCESS);

		this.traceGetTupleExit(status);

		return status;
	}

	public void cleanUp() {
		this.baseNodeCleanUp();

		for (OrNode node : this.getChildren())
			node.cleanUp();
	}

	public boolean initialize() {
		if (this.backtrackMap != null && (this.ruleBacktrackPoint != -2)) {
			for (OrNode node : this.getChildren())
				if (!node.initialize())
					return false;			
		} else {
			return false;
		}

		return true;
	}

	public void deleteRelationsAndCursors() {
		for (OrNode node : this.getChildren())
			node.deleteRelationsAndCursors();
	}

	public void partialCleanUp() {
		this.baseNodeCleanUp();

		for (OrNode node : this.getChildren())
			node.partialCleanUp();
	}

	public String toString() {
		return toStringNode();
	}

	public boolean containXYNode() {
		for (OrNode node : this.getChildren())
			if (node instanceof XYRecursiveOrNode || node instanceof XYStageVariableNode)
				return true;

		return false;
	}
	
	public int getRuleBacktrackPoint() { return this.ruleBacktrackPoint; }

	public void setRuleBacktrackPoint(int ruleBacktrackPoint) {
		this.ruleBacktrackPoint = ruleBacktrackPoint;
	}

	public int[] getBacktrackMap() { return this.backtrackMap; }
	
	public void setBacktrackMap(int[] backtrackMap) {
		this.backtrackMap = backtrackMap;
	}

	public String toStringWithAssignments() {
		StringBuilder output = new StringBuilder();
		if ((this.predicateName.equals(BuiltInPredicate.EQUALITY_PREDICATE_NAME)  
				|| this.predicateName.equals(BuiltInPredicate.INEQUALITY_PREDICATE_NAME) 
				|| this.predicateName.equals(BuiltInPredicate.LESS_THAN_OR_EQUAL_PREDICATE_NAME) 
				|| this.predicateName.equals(BuiltInPredicate.GREATER_THAN_OR_EQUAL_PREDICATE_NAME) 
				|| this.predicateName.equals(BuiltInPredicate.LESS_THAN_PREDICATE_NAME)  
				|| this.predicateName.equals(BuiltInPredicate.GREATER_THAN_PREDICATE_NAME))
				&& (this.arguments.size() == 2)) {
			output.append(this.getArgument(0).toString());
			output.append(" ");
			output.append(this.predicateName.toString());
			output.append("_");
			output.append(this.bindingPattern.toString());
			output.append(" ");
			output.append(this.getArgument(1).toString());
	    } else {
	    	/* mark it if it's an "I+1" type Node in Y rule of XY Clique --HW */
	    	if (this.xyPredicateType == XYPredicateType.NEW)
	    		output.append("NEW-XY_");
	    	if (this.xyPredicateType == XYPredicateType.OLD)
	    		output.append("OLD-XY_");

	    	output.append(this.predicateName);
	    	output.append("(");	    	

	    	if (this.arguments.size() > 0) {
	    		for (int i = 0; i < this.arguments.size(); i++) {
	    			if (i > 0)
	    				output.append(", "); 
	    			output.append(this.getArgument(i).toString());
	    		}
	    	}
	    	output.append(")");
	    }
		
		output.append(" <- ");
		
		for (int i = 0; i < this.getNumberOfChildren(); i++) {
			if (i > 0)
				output.append(", ");
			output.append(this.getChild(i).getPredicateName() + "(" + this.getChild(i).getArguments().toStringValues() + ")");
		}

		return output.toString();
	}
		
	@Override
	public AndNode copy(ProgramContext programContext) {		
		AndNode copy = new AndNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy());
		
		programContext.getNodeMapping().put(this, copy);
		
		copy.backtrackMap = this.backtrackMap;
		copy.ruleBacktrackPoint = this.ruleBacktrackPoint;
		copy.xyPredicateType = this.xyPredicateType;
		
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
				
		return copy;
	}
}
