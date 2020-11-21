package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs;

import java.util.List;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleRowPageLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// Class for monotonic semi-naive evaluation of fs aggregates
// recursive relation - heap tuplestore
// add/update of recursive relation
public class MSNCliqueNode 
	extends FSCliqueNode {

	protected final boolean			isSingleAggregate;
	protected final int 			valueOffset;
	protected AddressedTupleStore	tupleStore;
	protected SecondaryIndex<?>		writeIndex;
	protected AddressedTuple 		previousTuple;

	public MSNCliqueNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables,
			String recursiveRelationName, boolean isSharable, List<ArgumentType> argumentTypeAdornment) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, isSharable, argumentTypeAdornment);
		this.evaluationType = EvaluationType.MonotonicSemiNaive;
		
		int numberOfAggregates = 0;
		
		for (int i = 0; i < this.argumentTypeAdornment.length; i++)
			if (this.argumentTypeAdornment[i] == ArgumentType.FSAGGREGATE)
				numberOfAggregates++;
				
		this.isSingleAggregate = (numberOfAggregates == 1);
		
		// APS 12/7/2013 - only doing to do directmemory updating if single aggregate
		if (this.isSingleAggregate) {
			DataType[] schema =  this.getSchema();
			int valueOffset = 0;
			for (int i = 0; i < this.keyColumns.length; i++) 
				valueOffset += schema[i].getNumberOfBytes();
			
			this.valueOffset = valueOffset;
		} else {
			this.valueOffset = 0;
		}
	}

	@Override
	public boolean initialize() {
		if (!this.initialized) {
			if (!super.initialize())
				return false;
		
			this.tupleStore = (AddressedTupleStore) this.recursiveRelation.getTupleStore();
			this.writeIndex = this.database.getIndexManager().createSecondaryIndex(this.recursiveRelation, this.keyColumns);
			this.recursiveRelation.addSecondaryIndex(this.writeIndex);
			this.recursiveRelation.removeUniqueIndex();
			this.previousTuple = (AddressedTuple) this.recursiveRelation.getEmptyTuple();
		}
		
		return true;
	}
	
	@Override
	protected boolean addTuple() {
		if (DEBUG)
			this.logTrace("Entering addTuple");
		
		boolean	status	= false;
		for (int i = 0; i < this.arity; i++)
			this.returnedTuple.columns[i] = this.getArgumentAsDbType(i);

		if (DEBUG) {
			if (this.arguments.size() > 0) 
				this.logDebug("////////// [addTuple: {}] //////////", this.returnedTuple);			
		}
		
		this.numberOfRecursiveFactsDerived++;
		this.numberOfGeneratedFactsThisIteration++;
		
		// APS added 8/7/2013 - update tuple if incrementing
		int retval = this.addUpdateTuple(this.returnedTuple);
		if (retval > 0) {
			status = true;
			if (DEBUG) {
				if (retval == 1)
					this.logDebug("{}{} added to the recursive relation", this.recursiveRelationName, this.returnedTuple);
				else
					this.logDebug("{}{} updated in the recursive relation", this.recursiveRelationName, this.returnedTuple);					
			}
			
			this.addUpdatedThisStage++;
		}			
		
		if (DEBUG) {
			if (!status)
				this.logDebug("Tuple {} not added", this.returnedTuple.toString());
						
			this.logTrace("Exiting addTuple with status = {}", status);
		}

		return status;
	}
	
	// recursive relation must have an index to use this method
	private int addUpdateTuple(Tuple newTuple) {
		int retval = 2; // 1 for add, 2 for update, -1 for fail
		// PROCESS:
		// 1) Check if tuple is in index 
		//	Results:
		//  a) Matching tuple not in index
		//	b) Matching tuple in index but from earlier stage
		//	c) Matching tuple in index from this stage
		// 2) If 1.a or 1.b 
		//	a) add tuple and return status of add
		// else
		//  b)update tuple's non indexed columns
								
		int[] address = this.writeIndex.getSimilar(newTuple);
		int getStatus = 0;
		
		// there should only be one
		if (address != null && address.length == 1)
			getStatus = this.tupleStore.get(address[0], this.previousTuple);
		
		if (getStatus > 0) {
			// it is in this stage if its address in the tuplestore is after the last round's last tuple address
			if (this.isSingleAggregate) {
				TupleRowPageLeaf currentLeaf = ((TupleUnorderedHeapStore)this.tupleStore).getPage(this.previousTuple.address);
				int currentTupleAddress = ((TupleUnorderedHeapStore)this.tupleStore).getAddressInPage(this.previousTuple.address);

				newTuple.columns[this.keyColumns.length].getBytes(currentLeaf.getData().getData(), 
						(currentLeaf.getBytesPerTuple() * currentTupleAddress) + this.valueOffset);
				((AddressedTuple)newTuple).address = this.previousTuple.address;
			}
			this.updatedTupleCount++;
			
			this.updatedTuple = newTuple;
			this.addedTuple = null;
			
			return retval;
		}

		// if we get here, we didn't find an old tuple to update
		this.updatedTuple = null;
		boolean status = (this.recursiveRelation.add(newTuple, true) != null);
				
		if (status) {
			retval = 1;
			this.addedTuple = newTuple;
			this.addedTupleCount++;
		} else {
			retval = -1;
		}
		
		return retval;
	}
	
	@Override
	protected int getIterationSize() {
		return this.writeIndex.getNumberOfEntries();
	}
	
	// APS added 4/12/2013
	@Override
	public void prepareForNextStage() {
		if (DEBUG)
			this.logTrace("Entering prepareForNextPhase");
		
		super.prepareForNextStage();

		this.writeIndex.clear();
		
		if (DEBUG)
			this.logTrace("Exiting prepareForNextPhase");
	}
	
	@Override
	public void cleanUp() {
		super.cleanUp();
		if (this.writeIndex == null)
			this.writeIndex = null;
	}
	/*
	@Override
	public MSNCliqueNode copy(ProgramContext programContext) {
		if (programContext.getCliqueMapping().containsKey(this))
			return (MSNCliqueNode) programContext.getCliqueMapping().get(this);
		
		MSNCliqueNode copy = new MSNCliqueNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				new String(this.recursiveRelationName),
				this.isSharable,
				Arrays.asList(this.argumentTypeAdornment));
		
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
}
