package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;

public class MutualCliqueNode 
	extends CliqueNode 
	implements IMutualClique {
	
	public MutualCliqueNode(String predicateName, NodeArguments args, Binding binding,
			VariableList freeVariables, String recursiveRelationName) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, true);
	}

	@Override
	public void cleanUp() {
		if (!this.isCleanedUp) {
			this.cliqueCleanUp();
			this.isCleanedUp = true;
		}
	}

	public void partialCleanUp() { }

	@Override
	public Status generateTuple() {
		if (DEBUG)
			this.logTrace("Entering generateTuple for {}", this.toStringNode());
		
		Status status = Status.FAIL;
		
		this.isCleanedUp = false;

		if (this.currentRuleType == CliqueRuleType.EXIT_RULE) {
			status = this.getExitTuple();

			if (status != Status.SUCCESS)
				this.currentRuleType = CliqueRuleType.RECURSIVE_RULE;
		}

		if (this.currentRuleType == CliqueRuleType.RECURSIVE_RULE)
			status = this.getRecursiveTuple();

		if (DEBUG)			
			this.logTrace("Exiting generateTuple for {} with status = {}", this.toStringNode(), status);

		return status;
	}

	@Override
	public int getAnyTuple(Cursor readCursor, CliqueBaseNode clique, Tuple tuple) {
		if (DEBUG)
			this.logTrace("Entering getAnyTuple for {}", this.toStringNode());
				
		int getStatus = readCursor.getTuple(tuple); 
		if (getStatus == 0) {
			// If we are here, then it implies that we have reached the end of the
			// local recursive relation of clique. Let us see if we can generate
			// a new tuple by prodding the clique to generate a new tuple. 
			while (true) {
				if (clique.generateTuple() != Status.SUCCESS)
					break;
					
				getStatus = readCursor.getTuple(tuple);
				if (getStatus > 0)
					break;
			}
						
			// If we didn't generate any tuple and the getAnyTuple request was originally directed
			// at a subclique (this would be true if (this != clique)), then we try to
			// generate local tuples hoping that this would generate tuples in the subclique "clique".
			if (this != clique) {
				Status status = Status.SUCCESS;
				while ((getStatus == 0) && (status == Status.SUCCESS)) {
					// It does not matter if the generateTuple on the parent clique will succeed or not
					// because we might generate some tuples for the mutual clique
					status = this.generateTuple();

					while (true) {
						getStatus = readCursor.getTuple(tuple);
						if (getStatus > 0)
							break;
						if (clique.generateTuple() != Status.SUCCESS)
							break;								
					}
				}
			}
		}

		if (DEBUG) {			
			if (getStatus > 0)
				this.logTrace("Exiting getAnyTuple for {} with tuple {} with SUCCESS (status = {})", 
						this.toStringNode(), tuple, getStatus);
			else
				this.logTrace("Exiting getAnyTuple for {} with FAIL (status = {})", this.toStringNode(), getStatus);			
		}

		return getStatus;
	}

	protected boolean isFixedpointReached() {
		this.logError("MutualCliqueNode.isFixedpointReached() should not be called on Mutual Cliques");
		throw new InterpreterException("MutualCliqueNode.isFixedpointReached() should not be called on Mutual Cliques");
	}

	@Override
	public FixpointCursor getFixpointCursor() { return this.fixpointCursor; }
	
	@Override
	public MutualCliqueNode copy(ProgramContext programContext) {
		if (programContext.getCliqueMapping().containsKey(this))
			return (MutualCliqueNode) programContext.getCliqueMapping().get(this);
		
		MutualCliqueNode copy = new MutualCliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName));
		
		programContext.getCliqueMapping().put(this, copy);
		
		copy.exitRulesOrNode = this.exitRulesOrNode.copy(programContext);
		copy.recursiveRulesOrNode = this.recursiveRulesOrNode.copy(programContext);
		
		copy.evaluationType = this.evaluationType;
		copy.stage = this.stage;
		
		for (IMutualClique mc : this.mutualCliqueList)
			copy.mutualCliqueList.add((IMutualClique) programContext.getCliqueMapping().get(mc));
		
		for (RecursiveLiteral rl : this.recursiveLiteralList)
			copy.recursiveLiteralList.add((RecursiveLiteral) programContext.getNodeMapping().get(rl));
		
		return copy;
	}
}
