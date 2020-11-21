package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//1 Heap tuplestore used
public class FSSingleAggregateRelationNode 
	extends FSAggregateRelationNodeBase  {
	public FSSingleAggregateRelationNode   	counterpart;
	protected int							valueOffset;
	protected TupleStoreBase<?>				tupleStore;
	private final FSAggregateType 			aggregateType;
	private byte[] 							currentValue;
	protected int							numberOfTuplesDerived;
		
	public FSSingleAggregateRelationNode(String predicateName, NodeArguments args, 
			Binding binding, VariableList freeVariables, boolean isRead, 
			AggregateStoreType aggregateStoreType, FSAggregateType aggregateType) {
		super(predicateName, args, binding, freeVariables, isRead, aggregateStoreType);
		this.aggregateType = aggregateType;
	}

	@Override
	public boolean initialize() {
		/* APS 2/6/2014 - read will be initialized before write
		 * but write will set the counterpart pointers, since it is last
		 * read will create the relation, write will find it and use it */
		/*READ & WRITE NEED THIS*/
		this.numberOfKeyColumns = this.arity - 2;
		this.numberOfAggregateColumns = 1;
		this.aggregateValueVariable = (Variable)this.getArgument(this.arity - 1);
		
		int keyColumns[] = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;
		
		DataType[] schema = this.determineSchemaMain(keyColumns);

		// if the last column is an integer, see if we need to promote it to a long
		// this is necessary if the TypeAssigner recognizes a mixed fs aggregate situation
		if (schema[schema.length - 1] == DataType.INT)
			if (this.aggregateValueVariable.getDataType() == this.deALSContext.getConfiguration().getCountDataType())
				schema[schema.length - 1] = this.deALSContext.getConfiguration().getCountDataType();		
		
		if (this.aggregateStoreType == AggregateStoreType.BPlusTree) {
			this.relation = this.relationManager.createAggregateRelationBPlusTree(this.getRelationName(), schema, keyColumns);
		} else {
			this.relation = this.relationManager.createAggregateRelationHeap(this.getRelationName(), schema, keyColumns);
		}

		if (this.isReadAggregate){
			/*ONLY READ NEEDS THIS*/
			readAggregateNodes.push(this); 	
			
			// check if all keys are bound
			this.allKeyColumnsBound = true;
			for (int i = 0; i < this.numberOfKeyColumns; i++) {
				if (this.getBinding(i) == BindingType.FREE) {
					this.allKeyColumnsBound = false;
					break;
				}
			}
			
			this.tupleStore = this.relation.getTupleStore();
			if (this.aggregateStoreType != aggregateStoreType.BPlusTree)
				this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
						
			if (keyColumns.length == 0)
				this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
			
			return this.getChild(0).initialize();
		}
		
		/*ONLY WRITE NEEDS THIS*/
		this.counterpart = (FSSingleAggregateRelationNode)readAggregateNodes.pop();
		this.counterpart.counterpart = this;
		this.counterpart.oldValue = this.relation.getEmptyTuple();
		this.oldValue = this.counterpart.oldValue;
		this.newValue = this.relation.getEmptyTuple();
		
		this.valueOffset = 0; 
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.valueOffset += schema[i].getNumberOfBytes();

		return true;
	}
		
	protected Status getReadTuple() {
		Status status = Status.FAIL;
		this.hasOld =  false;
		// if we have a new value for to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		if (this.getOrNodeTuple() == Status.SUCCESS) {
			this.numberOfRecursiveFactsDerived++;
			if (this.clique != null)
				this.clique.numberOfGeneratedFactsThisIteration++;

			if (this.numberOfKeyColumns > 0) {
				if (this.aggregateStoreType == AggregateStoreType.BPlusTree) {
					DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
					for (int i = 0; i < this.numberOfKeyColumns; i++)
						boundValues[i] = this.getArgumentAsDbType(i);
					
					this.hasOld = (((BPlusTreeTupleStore)this.tupleStore).getTuple(boundValues, this.oldValue) > 0);
				} else {
					DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
					for (int i = 0; i < this.numberOfKeyColumns; i++)
						boundValues[i] = this.getArgumentAsDbType(i);
					
					((SelectionCursor)this.cursor).reset(boundValues);
					this.hasOld = (this.cursor.getTuple(this.oldValue) > 0);
				}
			} else {
				this.cursor.reset();
				this.hasOld = (this.cursor.getTuple(this.oldValue) > 0);
			}

			this.aggregateValueVariable.makeFree();
			
			if (this.hasOld) {
				if (this.aggregateStoreType == AggregateStoreType.Heap) {
					this.counterpart.currentLeaf = ((TupleUnorderedHeapStore)this.tupleStore).getPage(((AddressedTuple)this.oldValue).address);
					this.counterpart.currentTupleAddress = ((TupleUnorderedHeapStore)this.tupleStore).getAddressInPage(((AddressedTuple)this.oldValue).address);
				}

				this.aggregateValueVariable.setValue(this.oldValue.getColumn(this.numberOfKeyColumns));
				status = Status.SUCCESS;
			} else {
				this.hasOld = false;
				this.counterpart.currentLeaf = null;
				this.counterpart.currentTupleAddress = -1;
	
				if (this.aggregateType == FSAggregateType.FSMAX) {
					if (this.setArgumentValues(FSValueType.OldFSAsMin))
						status = Status.SUCCESS;
				} else {
					if (this.setArgumentValues(FSValueType.OldFSAsMax))
						status = Status.SUCCESS;
				}
			}	
		} else {
			// if no new value, we fail
			status = Status.FAIL;
		}

		return status;
	}

	protected Status getWriteTuple() {
		// if we reached here, we have a new max for the keys
		Status status;
		// With one getReadTuple call, we might enter getWriteTuple several times,
		// depending on how many tuples are returned from multi rule
		if (this.isEntry) {
			if (this.counterpart.hasOld) {
				DbTypeBase newValue = this.getArgumentAsDbType(this.numberOfKeyColumns);
				if (this.aggregateStoreType == AggregateStoreType.BPlusTree) {
					newValue.getBytes(this.currentValue, 0);
				} else {
					newValue.getBytes(this.currentLeaf.getData().getData(), 
							(this.currentLeaf.getBytesPerTuple() * this.currentTupleAddress) + this.valueOffset);
				}

				this.aggregateValueVariable.match(newValue);
				this.counterpart.hasOld = false;
				this.setLastTupleModified(false);
			} else {
				DbTypeBase dbTypeObject = null;
				for (int i = 0; i < this.relation.getArity(); i++) {
					dbTypeObject = this.getArgumentAsDbType(i);
					this.newValue.columns[i] = dbTypeObject;
				}

				// we just checked for an old tuple in getReadTuple() and got nothing
				this.relation.add(this.newValue, true); 
				// the last column in the tuple is put into the AggrValue variable
				this.aggregateValueVariable.match(dbTypeObject);
				this.setLastTupleModified(true);
			}
			
			// subtract 1 here since FSCliqueNode will add 1
			this.numberOfRecursiveFactsDerived--;
			if (this.clique != null)
				this.clique.numberOfGeneratedFactsThisIteration--;
			
			this.isEntry = false;
			status = Status.SUCCESS;
		} else {
			status = Status.FAIL;
			this.cleanUp();
		}
		
		return status;
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
}
