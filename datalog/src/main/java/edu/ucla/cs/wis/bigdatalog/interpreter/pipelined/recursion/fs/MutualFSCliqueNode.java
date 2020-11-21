package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs;

import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueRuleType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.IMutualClique;

public class MutualFSCliqueNode 
	extends FSCliqueNode 
	implements IMutualClique {
	
	public MutualFSCliqueNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, String recursiveRelationName, List<ArgumentType> argumentTypeAdornment) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, true, argumentTypeAdornment);
		this.evaluationType = EvaluationType.SemiNaive;
	}
	
	@Override
	public void cleanUp() {
		if (!this.isCleanedUp) {
			this.cliqueCleanUp();
			this.isCleanedUp = true;
		}
	}

	@Override
	public void partialCleanUp() { 
		// do nothing as this is a mutual clique
	}

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

		Status 	status = Status.SUCCESS;
		int getStatus = readCursor.getTuple(tuple);
		
		if (getStatus == 0) {
			// If we are here, then it implies that we have reached the end of the
			// local recursive relation of clique. Let us see if we can generate
			// a new tuple by prodding the clique to generate a new tuple. 
			
			// APS added 8/7/2013 - when updating tuple in local relation, we cannot get next tuple from readCursor.getTuple() 
			// if it is a SimpleCursor, which it needs to be in order to read the tuple that was just generated.
			// Therefore, we need to avoid the cursor and directly select the tuple just updated.
			while (clique.generateTuple() == Status.SUCCESS) {
				if (this.updatedTuple != null) {
					tuple.setValues(this.updatedTuple);
					this.updatedTuple = null;
					getStatus = 1;
					break;
				} else if (this.addedTuple != null) {
					tuple.setValues(this.addedTuple);
					this.addedTuple = null;
					readCursor.moveNext();
					getStatus = 1;
					break;
				} else {
					if (readCursor instanceof IndexCursor)
						((IndexCursor)readCursor).refresh();
									
					getStatus = readCursor.getTuple(tuple);
					if (getStatus > 0)
						break;
				}
			}
						
			// If we didn't generate any tuple and the getRmTuple request was originally directed
			// at a subclique (this would be true if (this != clique_ptr)), then we try to
			// generate local tuples hoping that this would generate tuples in the subclique "clique".
			
			if (this != clique) {
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

	@Override
	public boolean isFixedpointReached() {
		this.logError("MutualFSCliqueNode.isFixedpointReached() should not be called on Mutual FS Cliques");
		throw new InterpreterException("MutualFSCliqueNode.isFixedpointReached() should not be called on Mutual FS Cliques");
	}
	
	@Override
	public FixpointCursor getFixpointCursor() { return this.fixpointCursor; }
/*
	@Override
	public MutualFSCliqueNode copy(ProgramContext programContext) {
		if (programContext.getCliqueMapping().containsKey(this))
			return (MutualFSCliqueNode) programContext.getCliqueMapping().get(this);
		
		MutualFSCliqueNode copy = (MutualFSCliqueNode) MutualFSCliqueNode.createMutualFSCliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName),
				Arrays.asList(this.argumentTypeAdornment));
		/MutualFSCliqueNode copy = new MutualFSCliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName),
				Arrays.asList(this.argumentTypeAdornment));
		/
		programContext.getCliqueMapping().put(this, copy);
		
		copy.exitRulesOrNode = this.exitRulesOrNode.copy(programContext);
		copy.recursiveRulesOrNode = this.recursiveRulesOrNode.copy(programContext);
		copy.stage = this.stage;
		
		for (IMutualClique mc : this.mutualCliqueList)
			copy.mutualCliqueList.add((IMutualClique) programContext.getCliqueMapping().get(mc));
				
		for (RecursiveLiteral rl : this.recursiveLiteralList)
			copy.recursiveLiteralList.add((RecursiveLiteral) programContext.getNodeMapping().get(rl));		
		
		return copy;
	}*/
	
	public static IMutualClique createMutualFSCliqueNode(String predicateName, NodeArguments arguments, Binding binding, 
			VariableList freeVariableList, String recursiveRelationName, List<ArgumentType> argumentTypes, 
			EvaluationType evaluationType) {
		//EvaluationType evaluationType = EvaluationType.getEvaluationType(deALSConfiguration.getProperty("deals.interpreter.fsclique.evaluationtype").toLowerCase()); 
		switch (evaluationType) {
			case MonotonicSemiNaive:
				return new MutualMSNCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, argumentTypes);
			case EagerMonotonic:
				return new MutualEMSNCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, argumentTypes);
			default:
				return new MutualFSCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, argumentTypes);
		}		
	}
	
}