package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleRowPageLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.Utilities;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// Read aggregates use this class to produce an aggregate tuple from the underlying predicates
// Write aggregates use this class to update the running aggregate value for a key
// 1 Heap or 1 B+Tree tuplestore used
public class AggregateRelationNode 
	extends RelationNode {	
	public enum ValueType {
		Key, New, Old, OldAsNil
	}	

	protected boolean 				isReadAggregate;
	protected boolean 				hasOld;
	protected boolean 				hasNew;	
	protected int 						numberOfAggregateColumns;  
	protected int 						numberOfKeyColumns;
	protected boolean 				allKeyColumnsBound;
	protected AggregateRelationNode 	counterpart;
	protected Tuple	 				newValue;
	protected Tuple					oldValue;
	protected TupleRowPageLeaf			currentLeaf;	
	protected int						currentTupleAddress;
	protected TupleStoreBase<?>		tupleStore;
	protected AggregateInfo[]			aggregateInfos;
	protected int						numberOfDistinctAggregates;
	protected AggregateStoreType		aggregateStoreType;
	protected DbTypeBase[] 			keyValues;
	protected Cursor<?>				selectionCursor;
	protected Cursor<?>				scanCursor;
	protected Database					database;

	public AggregateRelationNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, boolean isRead, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables);
		this.isReadAggregate = isRead;
		
		this.aggregateInfos = new AggregateInfo[0];
		this.aggregateStoreType = aggregateStoreType;
	}
	
	public boolean isReadAggregate() { return this.isReadAggregate; }
	
	public void addAggregateInfo(AggregateInfo aggregateInfo) {
		AggregateInfo[] temp = new AggregateInfo[this.aggregateInfos.length + 1];
		for (int i = 0; i < this.aggregateInfos.length; i++)
			temp[i] = this.aggregateInfos[i];
		
		this.aggregateInfos = temp;			
		this.aggregateInfos[this.aggregateInfos.length - 1] = aggregateInfo;
		
		if (aggregateInfo.aggregateType == AggregateFunctionType.COUNT_DISTINCT)
			this.numberOfDistinctAggregates++;
	}
	
	@Override
	public boolean initialize() {
		if (this.isReadAggregate){
			readAggregateNodes.push(this);
			return this.getChild(0).initialize();   
		}
		
		// this is only for write aggregate nodes
		this.counterpart = readAggregateNodes.pop();
		this.counterpart.counterpart = this;
		
		this.numberOfAggregateColumns = this.counterpart.arity - 1 - this.arity;
		this.numberOfKeyColumns = this.arity - this.numberOfAggregateColumns;
		this.counterpart.numberOfAggregateColumns = this.numberOfAggregateColumns;
		this.counterpart.numberOfKeyColumns = this.numberOfKeyColumns;

		// check if all keys are bound
		this.counterpart.allKeyColumnsBound = true;
		for (int i = 0; i < this.numberOfKeyColumns; i++) {
			if (this.counterpart.getBinding(i) == BindingType.FREE) {
				this.counterpart.allKeyColumnsBound = false;
				break;
			}
		}
		
		for (int i = 0; i < this.counterpart.aggregateInfos.length; i++)
			this.addAggregateInfo(this.counterpart.aggregateInfos[i]);

		int keyColumns[] = new int[0];
		// can only add an index if we have key columns
		if (this.numberOfKeyColumns > 0) {
			keyColumns = new int[this.numberOfKeyColumns];
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				keyColumns[i] = i;	
		}
		
		// we're going to add an extra column for when the aggregation is done, we so we can reuse aggregate relations w/out recomputation
		DataType[] schema = new DataType[this.arity];
		DataType[] partialSchema = this.getSchema();
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			schema[i] = partialSchema[i];
		
		int count = 0;
		for (int i = this.numberOfKeyColumns; i < this.numberOfKeyColumns + this.numberOfAggregateColumns; i++) {
			if (this.isDistinctAggregate(count++))
				schema[i] = DataType.SET;
			else
				schema[i] = partialSchema[i];
		}

		//boolean useBPlusTree = this.useBPlusTree;
		if (keyColumns.length == 0)
			if (this.aggregateStoreType == AggregateStoreType.BPlusTree)
				this.aggregateStoreType = AggregateStoreType.Heap;
		
		if (this.aggregateStoreType == AggregateStoreType.BPlusTree) {
			TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.BPlusTree, keyColumns);
			tsc.setUniqueValue(true);
			this.relation = this.relationManager.createDerivedRelation(this.predicateName, schema, tsc, false);
		} else {
			TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.UnorderedHeap);
			this.relation = this.relationManager.createDerivedRelation(this.predicateName, schema, tsc, false);

			if (this.numberOfKeyColumns > 0)
				this.relation.addSecondaryIndex(keyColumns);
		}
					
		this.counterpart.relation = this.relation;
		this.counterpart.tupleStore = this.relation.getTupleStore();
		this.tupleStore = this.relation.getTupleStore();
		this.oldValue = this.tupleStore.getEmptyTuple();
		this.counterpart.oldValue = this.oldValue;
		this.newValue = this.tupleStore.getEmptyTuple();

		if (keyColumns.length > 0)
			this.counterpart.selectionCursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
				
		this.counterpart.scanCursor = this.database.getCursorManager().createScanCursor(this.relation);				 
				
		return true;
	}

	public void cleanUpData() {
		if (this.relation != null)
			this.relation.cleanUp();
	}
	
	public void deleteRelationsAndCursors() {
		if (this.relation != null) {
			this.relation.removeAllTuples();
			this.relation.commit();

			this.relationManager.deleteDerivedRelation((DerivedRelation)this.relation);
			this.relation = null;
		}

		this.database.getCursorManager().destroyCursor(this.scanCursor);
		this.scanCursor = null;
		this.database.getCursorManager().destroyCursor(this.selectionCursor);
		this.selectionCursor = null;
		
		super.deleteRelationsAndCursors();

		if (this.hasChildren())
			for (AndNode andNode : this.getChildren())
				andNode.deleteRelationsAndCursors();
	}

	public void partialCleanUp() {
		this.cleanUp();
	}

	@Override
	public String toString() { return toStringNode(); }
	
	public void getVariables(VariableList variableList) {
		for (int i = this.getNumberOfChildren() - 1; i >= 0; i--) {
			this.getChild(i).getVariables(variableList);
			this.getChild(i).getBodyVariables(variableList);
		}		
		Utilities.getVariables(this.getArguments(), variableList);
	}
	
	// get the old value for the key
	protected boolean getOldValue(boolean reset, boolean wasEntry) {
		int status = 0;
		// with no binding, we use the scan cursor
		if (this.numberOfKeyColumns == 0) {
			this.cursor = this.scanCursor;
			if (reset)
				this.cursor.reset();

			status = this.cursor.getTuple(this.oldValue);
		} else {
			boolean keyNotBound = false;
			if (wasEntry) {				
				// if key columns unbound, we have to use scan cursor
				for (int i = 0; i < this.numberOfKeyColumns; i++) {
					if (this.getArgument(i) instanceof Variable) {
						// APS 1/2/2014 - dereference to find out if value is set
						// variable will be returned when it not assigned anything
						Argument value = ((Variable)this.getArgument(i)).deepDereference();
						if (value == null || value instanceof Variable) {
							keyNotBound = true;
							break;
						}
					} else {
						if (!this.getArgument(i).isBound()) {
							keyNotBound = true;
							break;
						}
					}
				}
				
				// no bindings, so get scancursor
				if (keyNotBound) {
					this.cursor = this.scanCursor;
					this.keyValues = new DbTypeBase[this.numberOfKeyColumns];
				} else {
					this.cursor = this.selectionCursor;
				}
			}

			if (reset) {
				if (this.cursor instanceof SelectionCursor) {						
					this.keyValues = new DbTypeBase[this.numberOfKeyColumns];
					for (int i = 0; i < this.numberOfKeyColumns; i++)
						this.keyValues[i] = this.getArgumentAsDbType(i);
					
					((SelectionCursor<?>)this.cursor).reset(this.keyValues);
				} else {
					this.cursor.reset();
				}
			}

			if (this.cursor instanceof SelectionCursor) {
				if (this.aggregateStoreType == AggregateStoreType.BPlusTree) {
					if (reset) {
						status = ((TupleBPlusTreeUniqueStore)this.tupleStore).getTuple(this.keyValues, this.oldValue);
					}
				} else {					
					status = this.cursor.getTuple(this.oldValue);
				}
				
			} else {
				status = this.cursor.getTuple(this.oldValue);
			}
		}

		return (status > 0);
	}
	
	protected boolean setArgumentValues(ValueType type) {
		Argument argument;
		boolean isMatch = true;
 	
		switch (type) {
		case Key:
			for (int i = 0; i < this.numberOfKeyColumns && isMatch; i++)
				isMatch = this.getArgument(i).match(this.oldValue.getColumn(i));
			break;
		case New:
			for (int i = 0; i < this.numberOfAggregateColumns && isMatch; i++)
				isMatch = this.getArgument(this.numberOfKeyColumns + i).match(this.newValue.getColumn(this.numberOfKeyColumns + i));	
			break;
		case Old:
			for (int i = 0; i < this.numberOfAggregateColumns && isMatch; i++) {
				argument = this.getArgument(this.numberOfKeyColumns + this.numberOfAggregateColumns + i);
				if (this.isDistinctAggregate(i)) {
					DbSet set = (DbSet) this.oldValue.getColumn(i + this.numberOfKeyColumns);
					isMatch = argument.match(DbInteger.create(set.getNumberOfEntries()));
				} else {
					isMatch = argument.match(this.oldValue.getColumn(i + this.numberOfKeyColumns));
				}
			}
			break;
		case OldAsNil:
			for (int i = 0; i < this.numberOfAggregateColumns && isMatch; i++)
				isMatch = this.getArgument(this.numberOfKeyColumns 
						+ this.numberOfAggregateColumns + i).match(DbList.create());
			
			break;
		}

		return isMatch;
	}
	
	protected Status getReadTuple() {
		Status status = Status.FAIL;
		
		DbTypeBase returnArgumentValue = null;    
		Argument returnArgument = this.getArgument(this.numberOfKeyColumns + this.numberOfAggregateColumns * 2);
		
		// free aggregate values for assignment later
		for (int i = (this.numberOfKeyColumns + this.numberOfAggregateColumns); i < this.arity; i++)
			((Variable)this.getArgument(i)).makeFree();

		boolean retry = true;
		while (retry) {
			retry = false;

			boolean wasEntry = this.isEntry;
			if (this.isEntry) {
				this.hasNew = (this.getOrNodeTuple() == Status.SUCCESS);
								
				this.hasOld = this.getOldValue(true, wasEntry);
						
				if (this.hasNew) {						
					if (this.hasOld) {
						returnArgumentValue = DbInteger.create(3);
						if (this.setArgumentValues(ValueType.Old))
							status = Status.SUCCESS;
						this.isEntry = false;
					} else {
						returnArgumentValue = DbInteger.create(2);
						if (this.setArgumentValues(ValueType.OldAsNil))
							status = Status.SUCCESS;
						this.isEntry = true;
					}
				} else {
					if (this.hasOld) {
						returnArgumentValue = DbInteger.create(1);
						this.freeVariableList.makeFree();

						if (this.setArgumentValues(ValueType.Old) && this.setArgumentValues(ValueType.Key))
							status = Status.SUCCESS;
						
						this.aggregationCompleteForTuple();
												
						this.isEntry = false;
					} else {
						status = Status.FAIL;
						this.isEntry = true;
					}
				}
			} else {
				this.hasOld = this.getOldValue(false, wasEntry);
				
				if (!this.hasOld) {
					if (this.hasNew)
						retry = true;
					else
						status = Status.FAIL;
					
					this.isEntry = true;
				} else {
					if (this.hasNew) {
						returnArgumentValue = DbInteger.create(3);
						// set args: we only need to set old value, because new/key value are same as the previous tuple returned
						if (this.setArgumentValues(ValueType.Old))
							status = Status.SUCCESS;
					} else {
						returnArgumentValue = DbInteger.create(1);
						// we are returning final tuples from table, so the keys can be different, we need to set them all.

						this.freeVariableList.makeFree();
						if (this.setArgumentValues(ValueType.Old) && this.setArgumentValues(ValueType.Key))
							status = Status.SUCCESS;
						
						this.aggregationCompleteForTuple();
						this.isEntry = false;
					}
				}
			}
		}
		
		if (status == Status.FAIL)
			this.freeVariableList.makeFree();

		if (status == Status.SUCCESS)
			returnArgument.match(returnArgumentValue);
		
		return status;
	}
	
	protected void aggregationCompleteForTuple() {
		// APS 6/12/2014 - remove when done with tuple
		this.relation.remove(this.oldValue);
		
		// reset the bplustree cursor since it scans left to right in sequential order
		// heapstore cursors go by address instead
		if (this.aggregateStoreType == AggregateStoreType.BPlusTree)
			this.cursor.reset();
	}
	
	protected Status getWriteTuple() {
		Status status;
		// With one readAccess, we might enter writeAccess several times,
		// depending on how many tuples are returned from the multi rule
		if (this.isEntry) {
			if (this.counterpart.hasOld) {
				int counter = 0;				
				for (int i = this.numberOfKeyColumns; i < this.arity; i++) {
					if (this.isDistinctAggregate(counter)) {
						DbSet set = (DbSet) this.counterpart.oldValue.getColumn(i);
						set.put(this.getArgumentAsDbType(i));
					} else {
						this.counterpart.oldValue.columns[i] = this.getArgumentAsDbType(i);
					}
					counter++;
				}
				((DerivedRelation)this.relation).update(this.counterpart.oldValue, this.counterpart.oldValue);
				//((UpdateableCursor)this.counterpart.cursor).update(this.counterpart.oldValue);			
			} else {
				if (this.numberOfDistinctAggregates > 0) {
					for (int i = 0; i < this.numberOfKeyColumns; i++) 
						this.newValue.columns[i] = this.getArgumentAsDbType(i);

					int counter = 0;
					for (int i = this.numberOfKeyColumns; i < this.arity; i++) {
						if (this.isDistinctAggregate(counter++)) {
							DbTypeBase val = this.getArgumentAsDbType(i);
							// type used in bplustree is from value assigned in argument
							DbSet set = this.typeManager.createSet(val.getDataType());							
							set.put(val);
							this.newValue.columns[i] = set;
						} else {
							this.newValue.columns[i] = this.getArgumentAsDbType(i);
						}
					}
				} else {
					for (int i = 0; i < this.arity; i++) 
						this.newValue.columns[i] = this.getArgumentAsDbType(i);
				}

				((DerivedRelation)this.relation).add(this.newValue, true);
			}
						
			this.isEntry = false;
			status = Status.SUCCESS;			
		} else {
			status = Status.ENTRY_FAIL;
			this.cleanUp();
		}

		return status;
	}

	@Override
	public Status getTuple() {
		this.traceGetTupleEntry();

		Status status = this.isReadAggregate ? this.getReadTuple() : this.getWriteTuple();

		this.traceGetTupleExit(status);

		return status;
	}
	
	protected boolean isDistinctAggregate(int index) {
		return (this.aggregateInfos.length > index 
				&& this.aggregateInfos[index].aggregateType == AggregateFunctionType.COUNT_DISTINCT);
	}
	
	@Override
	public AggregateRelationNode copy(ProgramContext programContext) {		
		AggregateRelationNode copy = new AggregateRelationNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList),
				this.isReadAggregate,
				this.aggregateStoreType);
		
		copy.executionMode = this.executionMode;
		
		programContext.getNodeMapping().put(this, copy);

		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
		
		return copy;
	}
	
	@Override
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.database = deALSContext.getDatabase();
	}
}
