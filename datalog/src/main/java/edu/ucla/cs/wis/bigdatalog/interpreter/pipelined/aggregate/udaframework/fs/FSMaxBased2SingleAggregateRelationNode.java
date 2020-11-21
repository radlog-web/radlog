package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import java.util.Iterator;
import java.util.LinkedHashMap;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// 1 Heap tuplestore used
public class FSMaxBased2SingleAggregateRelationNode 
	extends FSAggregateRelationNodeBase {

	protected final FSAggregateType 			aggregateType;
	protected FSMaxBased2SingleAggregateRelationNode   counterpart;
	protected int						valueOffset;	
	protected int						stage;
	protected DbTypeBase[] 				currentBinding;
	protected DbTypeBase				previousKey;
	protected boolean					sendMode;
	protected LinkedHashMap<Integer, Tuple> sendList;
	protected Iterator<Tuple>	sendCursor;
	protected TupleUnorderedHeapStore	tupleStore;

	public FSMaxBased2SingleAggregateRelationNode(String predicateName, NodeArguments args, 
			Binding binding, VariableList freeVariables, boolean isRead, AggregateStoreType aggregateStoreType, 
			FSAggregateType aggregateType) {
		super(predicateName, args, binding, freeVariables, isRead, aggregateStoreType);
		this.aggregateType = aggregateType;
	}

	@Override
	public boolean initialize() {
		if (this.isReadAggregate){
			readAggregateNodes.push(this);
			this.stage = 0;
			return this.getChild(0).initialize();
		}

		this.counterpart = (FSMaxBased2SingleAggregateRelationNode)readAggregateNodes.pop();
		this.counterpart.counterpart = this;

		// fsmax aggregates require only 1 aggregate value column
		this.numberOfAggregateColumns = 1;

		// we subtract one - the aggregate value to write
		this.numberOfKeyColumns = this.arity - 2;
		this.counterpart.numberOfAggregateColumns = 1;
		this.counterpart.numberOfKeyColumns = this.numberOfKeyColumns;

		int[] keyColumns = new int[this.numberOfKeyColumns];
		// check if all keys are bound
		this.counterpart.allKeyColumnsBound = true;
		for (int i = 0; i < this.numberOfKeyColumns; i++) {
			if (this.counterpart.getBinding(i) == BindingType.FREE) {
				this.counterpart.allKeyColumnsBound = false;
				break;
			}
		}

		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;

		// we need 1 column for each key, 1 column for fsmax, 1 + size of 1st argument in functor for fscnt
		int relationArity = this.numberOfKeyColumns + 1;

		// see if relation already created
		String relationName = this.getRelationName();
		DataType[] schema = new DataType[relationArity];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			schema[i] = this.getArgument(i).getDataType();

		schema[relationArity - 1] = this.getArgument(this.numberOfKeyColumns).getDataType();

		this.valueOffset = 0; 
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.valueOffset += schema[i].getNumberOfBytes();

		this.relation = this.relationManager.createDerivedRelation(relationName, schema, 
				new TupleStoreConfiguration(TupleStoreType.UnorderedHeap), false);

		// inform read aggregate relation so they access the same relation
		this.counterpart.setRelation(this.relation);
		this.counterpart.tupleStore = (TupleUnorderedHeapStore)this.relation.getTupleStore();

		if (this.numberOfKeyColumns > 0)
			this.relation.addSecondaryIndex(keyColumns);

		this.counterpart.setCursor(this.database.getCursorManager().createCursor(this.relation, keyColumns));
		this.oldValue = this.relation.getEmptyTuple();
		this.counterpart.oldValue = this.oldValue;
		this.stage = 0;

		this.sendList = new LinkedHashMap<>();
		this.counterpart.sendList = this.sendList;

		return true;
	}

	public void incrementStage() { 
		this.stage++;
		this.sendCursor = null;
		this.sendList.clear();
	}

	// get the old value for the key
	protected boolean getOldValue(boolean reset, boolean wasEntry) {	
		// with no binding, we use the scan cursor
		if (this.numberOfKeyColumns == 0) {
			if (reset)
				this.sendCursor = this.sendList.values().iterator();
			
			if (this.sendCursor.hasNext())
				this.oldValue = this.sendCursor.next();
			else
				this.hasOld = false;
		} else {
			if (reset) {
				DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					boundValues[i] = this.getArgumentAsDbType(i);

				((SelectionCursor<AddressedTuple>)this.cursor).reset(boundValues);				
			}

			// if we have a new tuple and we have a binding we can use to search on, use the index
			this.hasOld = (this.cursor.getTuple(this.oldValue) > 0);
			if (this.hasOld && !this.hasNew)
				this.hasNew = true;			
		}

		if (this.hasOld) {
			this.currentLeaf = this.tupleStore.getPage(((AddressedTuple)this.oldValue).address);
			this.counterpart.currentLeaf = this.currentLeaf;
			this.currentTupleAddress = this.tupleStore.getAddressInPage(((AddressedTuple)this.oldValue).address);
			this.counterpart.currentTupleAddress = this.currentTupleAddress;
			return true;
		}
		this.currentLeaf = null;
		this.counterpart.currentLeaf = null;
		this.currentTupleAddress = -1;
		this.counterpart.currentTupleAddress = -1;
		return false;
	}

	protected boolean setArgumentValues(FSValueType type) {
		boolean isMatch = true;

		switch (type) {
		case OldFS:
			isMatch = this.getArgument(this.numberOfKeyColumns + 1).match(this.oldValue.getColumn(this.numberOfKeyColumns));				
			break;
		case OldFSAsMin:
			isMatch = this.getArgument(this.numberOfKeyColumns + 1).match(this.getMinValue());
			break;
		case OldFSAsMax:
			isMatch = this.getArgument(this.numberOfKeyColumns + 1).match(this.getMaxValue());
			break;
		}

		return isMatch;
	}

	protected Status getReadTuple() {
		Status status = Status.FAIL;  

		// free aggregate values for assignment later
		for (int i = (this.numberOfKeyColumns + this.numberOfAggregateColumns); i < this.arity; i++)
			((Variable)this.getArgument(i)).makeFree();

		boolean retry = true;
		while (retry) {
			retry = false;
			boolean wasEntry = this.isEntry;
			if (this.isEntry) {
				this.hasNew = (this.getOrNodeTuple() == Status.SUCCESS);
				
				this.hasOld = this.getOldValue(this.hasNew, wasEntry);

				if (this.hasNew) {		
					if (this.hasOld) {
						if (this.setArgumentValues(ValueType.Old))
							status = Status.SUCCESS;
						this.isEntry = false;
					} else {
						if (this.aggregateType == FSAggregateType.FSMAX) {
							if (this.setArgumentValues(FSValueType.OldFSAsMin))
								status = Status.SUCCESS;
						} else {
							if (this.setArgumentValues(FSValueType.OldFSAsMax))
								status = Status.SUCCESS;
						}
						this.isEntry = true;
					}
				} else {
					if (this.hasOld) {
						if (this.setArgumentValues(ValueType.Old) && this.setArgumentValues(ValueType.Key))
							status = Status.SUCCESS;

						this.isEntry = false;
						this.setLastTupleModified(false);
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
					status = Status.SUCCESS;
					if (this.hasNew) {
						// set args: we only need to set old value, because new/key value are same as the previous tuple returned
						this.setArgumentValues(ValueType.Old);
					} else {												
						// we are returning final tuples from table, so the keys can be different, we need to set them all.
						this.freeVariableList.makeFree();
						this.setArgumentValues(ValueType.Old);
						this.setArgumentValues(ValueType.Key);
						this.setLastTupleModified(false);
					}
				}
			}
		}

		return status;
	}

	protected Status getWriteTuple() {
		Status status;
		// With one readAccess, we might enter writeAccess several times,
		// depending on how many tuples are returned from the multi rule
		if (this.isEntry) {
			AddressedTuple newTuple = null;
			if (this.counterpart.hasOld) {
				DbTypeBase newValue = this.getArgumentAsDbType(this.numberOfKeyColumns);				
				newValue.getBytes(this.currentLeaf.getData().getData(), 
						(this.currentLeaf.getBytesPerTuple() * this.currentTupleAddress) + this.valueOffset);
				this.counterpart.oldValue.columns[this.numberOfKeyColumns] = newValue;
				this.sendList.put(((AddressedTuple)this.counterpart.oldValue).address, this.counterpart.oldValue);
			} else {
				AddressedTuple newValue = new AddressedTuple(this.arity);

				for (int i = 0; i < this.relation.getArity(); i++)
					newValue.setColumn(i, this.getArgumentAsDbType(i));

				((DerivedRelation)this.relation).add(newValue, true);
				this.sendList.put(newValue.address, newValue);
			}
			
			// the last column in the tuple is put into the AggrValue variable
			//this.aggregateValueVariable.match(dbTypeObject);

			this.isEntry = true;
			status = Status.FAIL;
		} else {
			status = Status.ENTRY_FAIL;
			this.cleanUp();
		}
		
		return status;
	}
}
