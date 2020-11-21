package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs;

import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.NaiveCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.ChangeTrackerCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.IntTreeChangeTrackerScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.LongTreeChangeTrackerScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly.BPlusTreeLongKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.IntTreeChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.LongTreeChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.FSCliqueComparisonReachedNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSCountKeyValueAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.LinearRecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.NonLinearRecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// Class for eager monotonic semi-naive evaluation of fs aggregates
// no recursive relation 
// reads aggregate relation from fs aggregates - BAT (B+Tree aggregator) tuplestore
// no modifying aggregate relation
public class EMSNCliqueNode 
	extends FSCliqueNode {
	public boolean isNewTuple;
	protected boolean isUsedForComparison;
	protected FSCliqueComparisonReachedNode tracker;
	protected DbSet keysPassingComparison;
	protected DbSet keysToExclude;
	
	public EMSNCliqueNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables,
			String recursiveRelationName, boolean isSharable, List<ArgumentType> argumentTypeAdornment) {
		super(predicateName, args, binding, freeVariables, recursiveRelationName, isSharable, argumentTypeAdornment);
		this.evaluationType = EvaluationType.EagerMonotonic;
	}
	
	public void setupForComparison(FSCliqueComparisonReachedNode tracker) {
		this.isUsedForComparison = true;
		this.tracker = tracker;
	}
	
	@Override
	public boolean initialize() {
		boolean status = false;

		if (!this.initialized) {
			if (this.recursiveRelationName != null) {
				this.initializeBookkeepingInfo();
				this.fsAggregates = this.getFSAggregates();
				
				if ((this.exitRulesOrNode != null) 
						&& (status = this.exitRulesOrNode.initialize())
						&& (this.recursiveRulesOrNode != null) 
						&& (status = this.recursiveRulesOrNode.initialize())) {						 
					this.initialized = true;
				}
							
				String relationName = AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX + this.getPredicateName().substring(0, this.getPredicateName().lastIndexOf("_"));
				int numberOfColumns = this.arity; 
				
				// fscnt + emsn requires fooling the clique into using a table with extra +1 arity
				for (FSAggregateRelationNode fsAggr : this.fsAggregates) {
					if (fsAggr.matchesRelationName(relationName)) {
						if (/*(fsAggr instanceof FSCountSingleKeyValueAggregateRelationNode)
								|| */(fsAggr instanceof FSCountKeyValueAggregateRelationNode)
								/*|| (fsAggr instanceof FSCountSingleKeyValueFastAggregateRelationNode2)*/) {
							numberOfColumns++;
							break;
						}
					}/* else if (fsAggr instanceof FSManyAggregateRelationNode) {
							FSAggregateType[] aggregateTypes = ((FSManyAggregateRelationNode)fsAggr).getFSAggregateTypes();
							for (FSAggregateType aggregateType : aggregateTypes) {
								if (aggregateType == FSAggregateType.FSCNT)
									numberOfColumns++;
							}							
						}
					}*/
				}

				this.aggregateRelation = (DerivedRelation) this.database.getRelationManager().getRelation(relationName, numberOfColumns);

				// APS 2/14/2014
				// if there are recursive literals, see if we have a linear recursion for this clique.  If not, try nonlinear.
				// in both cases, grab the cursor from the first one found that matches this clique's name
				if (this.recursiveLiteralList != null && this.recursiveLiteralList.size() > 0) {
					for (RecursiveLiteral literal : this.recursiveLiteralList) {
						if (literal instanceof LinearRecursiveLiteral) {
							if (literal.getRecursiveRelationName().equals(this.aggregateRelation.getName())) {
								if (literal.getCursor() instanceof FixpointCursor) {
									this.fixpointCursor = (FixpointCursor) literal.getCursor();
									break;
								}
							}
						}
					}
					
					if (this.fixpointCursor == null) {
						for (RecursiveLiteral literal : this.recursiveLiteralList) {
							if (literal instanceof NonLinearRecursiveLiteral) {
								if (literal.getRecursiveRelationName().equals(this.aggregateRelation.getName())) {
									if (literal.getCursor() instanceof FixpointCursor) {
										this.fixpointCursor = (FixpointCursor) literal.getCursor();
										break;
									}
								}
							}
						}
					}
				}
				
				if (this.fixpointCursor == null)
					this.fixpointCursor = this.database.getCursorManager().createChangeTrackerCursor(this.aggregateRelation, this.deALSContext.getConfiguration());
				
				((ChangeTrackerCursor<?>)this.fixpointCursor).initializeTracking(this.deALSContext.getConfiguration());
				this.returnedTuple = this.aggregateRelation.getEmptyTuple();
				if (this.returnedTuple.getArity() > this.arity) {
					DataType[] schema = this.getSchema();
					Tuple tuple = new Tuple(schema.length);
					for (int i = 0; i < schema.length; i++)
						tuple.columns[i] = DbTypeBase.loadFrom(schema[i], 0);
					this.returnedTuple = tuple;
				}
			} else {
				throw new InterpreterException("Can not initialize clique.  No relation " + this.predicateName);
			}
			
			if (this.tracker != null) {
				this.tracker.initialize();
				this.keysPassingComparison = this.tracker.getTracker();
				DataType[] keyTypes = new DataType[this.arity - 1];
				for (int i = 0; i < keyTypes.length; i++)
					keyTypes[i] = this.arguments.get(i).getDataType();
								
				this.keysToExclude = this.typeManager.createSet(keyTypes);	
			}
			
		} else {
			status = true;
		}

		return status;
	}
	
	@Override
	protected boolean addTuple() {
		if (DEBUG)
			this.logTrace("Entering addTuple");
		
		for (int i = 0; i < this.arity; i++)
			this.returnedTuple.columns[i] = this.getArgumentAsDbType(i);

		if (DEBUG) {
			if (this.arguments.size() > 0) 
				this.logDebug("////////// [addTuple: {}] //////////", this.returnedTuple);			
		}
		
		this.numberOfRecursiveFactsDerived++;
		this.numberOfGeneratedFactsThisIteration++;
		
		if (this.isNewTuple) {
			this.addedTuple = this.returnedTuple;
			this.addedTupleCount++;

			if (DEBUG)
				this.logDebug("{}{} added to the recursive relation", this.getPredicateName(), this.returnedTuple);
		} else {
			this.updatedTuple = this.returnedTuple;
			this.updatedTupleCount++;
			if (DEBUG)
				this.logDebug("{}{} updated in the recursive relation", this.getPredicateName(), this.returnedTuple);
		}

		this.addUpdatedThisStage++;
		
		if (DEBUG)
			this.logTrace("Exiting addTuple");

		return true;
	}
	
	@Override
	protected int getIterationSize() {
		return ((ChangeTrackerCursor<?>)this.fixpointCursor).getSizeOfDelta();
	}
		
	// APS added 4/12/2013
	@Override
	public void prepareForNextStage() {
		if (DEBUG)
			this.logTrace("Entering prepareForNextPhase");

		super.prepareForNextStage();
		
		// the idea is that need to only check within the stage if a tuple can be updated
		// so clear the index each iteration
		if (this.recursiveLiteralList != null && this.recursiveLiteralList.size() > 0) {
			for (RecursiveLiteral literal : this.recursiveLiteralList) {
				if (literal instanceof LinearRecursiveLiteral) {
					if (literal.getRecursiveRelationName().equals(this.aggregateRelation.getName()))
						if (literal.getCursor() instanceof ChangeTrackerCursor)
							((ChangeTrackerCursor<?>)literal.getCursor()).beginNextStage(this.deALSContext, this.stage);					
				} else if (literal instanceof NonLinearRecursiveLiteral) {
					if (literal.getRecursiveRelationName().equals(this.aggregateRelation.getName()))
						if (literal.getCursor() instanceof ChangeTrackerCursor)
							((ChangeTrackerCursor<?>)literal.getCursor()).beginNextStage(this.deALSContext, this.stage);					
				}
			}
		} else {
			if (this.fixpointCursor instanceof ChangeTrackerCursor)
				((ChangeTrackerCursor<?>)this.fixpointCursor).beginNextStage(this.deALSContext, this.stage);
		}

		if (DEBUG)
			this.logTrace("Exiting prepareForNextPhase");
	}
	
	@Override
	protected boolean isFixedpointReached() {
		if (this.isUsedForComparison)
			this.prepareComparisonForNextStage();
		
		return super.isFixedpointReached();		
	}
	
	protected void prepareComparisonForNextStage() {
		boolean print = false;
		// remove keys from the change tracker that have #1 passed comparison and #2 afterwards have been used in recursion (intersect)
		// #1 is the set from the tracker node (called A)
		// #2 is the set of keys used in recursion and have passed A (called B)
		// with deltaS' (C)
		// C' = C - (B + A) where C' is the new tree placed in the change tracker node
		if (this.fixpointCursor instanceof LongTreeChangeTrackerScanCursor) {		
			LongTreeChangeTrackerScanCursor cursor = (LongTreeChangeTrackerScanCursor)this.fixpointCursor;
			LongTreeChangeTracker deltaSPrime = (LongTreeChangeTracker) cursor.getStore().getModifiedKeys(this.stage);
			if (deltaSPrime == null) return;
			
			LongTreeChangeTracker deltaS = (LongTreeChangeTracker) cursor.getStore().getModifiedKeys(this.stage - 1);
			if (deltaS == null) return;
			
			if (this.keysPassingComparison.getTree().isEmpty()) return;

			BPlusTreeLongKeysOnly deltaSTree = deltaS.getTree();
			BPlusTreeLongKeysOnly deltaSPrimeTree = deltaSPrime.getTree();
			BPlusTreeLongKeysOnly keysToExcludeTree = (BPlusTreeLongKeysOnly)this.keysToExclude.getTree();
			
			// B + A
			BPlusTreeLongKeysOnly newKeysToExclude = deltaSTree.intersect((BPlusTreeLongKeysOnly) this.keysPassingComparison.getTree(), print);
			
			keysToExcludeTree.merge(newKeysToExclude, print);
			
			// C' = C - (B + A)
			BPlusTreeLongKeysOnly newTree = deltaSPrimeTree.difference(keysToExcludeTree, print);
			if (newTree != null)
				deltaSPrime.setTree(newTree);
						
			// reset for next iteration
			this.keysPassingComparison.clear();
		} else if (this.fixpointCursor instanceof IntTreeChangeTrackerScanCursor) {
			IntTreeChangeTrackerScanCursor cursor = (IntTreeChangeTrackerScanCursor)this.fixpointCursor;
			IntTreeChangeTracker deltaSPrime = (IntTreeChangeTracker) cursor.getStore().getModifiedKeys(this.stage);
			if (deltaSPrime == null) return;
			
			IntTreeChangeTracker deltaS = (IntTreeChangeTracker) cursor.getStore().getModifiedKeys(this.stage - 1);
			if (deltaS == null) return;
			
			if (this.keysPassingComparison.getTree().isEmpty()) return;

			BPlusTreeIntKeysOnly deltaSTree = deltaS.getTree();
			BPlusTreeIntKeysOnly deltaSPrimeTree = deltaSPrime.getTree();
			BPlusTreeIntKeysOnly keysToExcludeTree = (BPlusTreeIntKeysOnly)this.keysToExclude.getTree();
			
			// B + A
			BPlusTreeIntKeysOnly newKeysToExclude = deltaSTree.intersect((BPlusTreeIntKeysOnly) this.keysPassingComparison.getTree(), print);
			
			keysToExcludeTree.merge(newKeysToExclude, print);
			
			// C' = C - (B + A)
			BPlusTreeIntKeysOnly newTree = deltaSPrimeTree.difference(keysToExcludeTree, print);
			if (newTree != null)
				deltaSPrime.setTree(newTree);
						
			// reset for next iteration
			this.keysPassingComparison.clear();			
		}
	}
		
	@Override
	protected void prepareStatCountersForNextStage(int stage) {
		if (DEBUG && this.deALSContext.isInfoEnabled()) {
			if (this.fixpointCursor instanceof NaiveCursor) {
				this.logInfo("Stage {} [{} - {}] added: {}  updated: {} [total this stage: {}] time: {} + # of delta tuples:{}", 
						stage, 
						((NaiveCursor)this.fixpointCursor).baseTupleAddress, 
						((NaiveCursor)this.fixpointCursor).endTupleAddress, 
						this.addedTupleCount, 
						this.updatedTupleCount, 
						this.addUpdatedThisStage, 
						(System.currentTimeMillis() - time), 
						this.numberOfDeltaFactsByIteration);
			} else {
				this.logInfo("Stage {} [{}] added: {}  updated: {} [total this stage: {}] time: {} # of delta tuples:{}", 
						stage, 
						this.aggregateRelation.getTupleStore().getNumberOfTuples(),
						this.addedTupleCount,
						this.updatedTupleCount,
						this.addUpdatedThisStage,
						(System.currentTimeMillis() - time),
						this.numberOfDeltaFactsByIteration);
			}

			this.logInfo("***************************Stage {} Complete***************************", stage);				
		}
		this.time = System.currentTimeMillis();
		this.addUpdatedThisStage = 0;
		this.addedTupleCount = 0;
		this.updatedTupleCount = 0;
	}
	
	@Override
	public void cleanUp() {
		super.cleanUp();
		
		if (this.aggregateRelation != null)
			this.aggregateRelation.cleanUp();
	}
	/*
	@Override
	public EMSNCliqueNode copy(ProgramContext programContext) {
		if (programContext.getCliqueMapping().containsKey(this))
			return (EMSNCliqueNode) programContext.getCliqueMapping().get(this);
		
		EMSNCliqueNode copy = new EMSNCliqueNode(new String(this.predicateName), 
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
