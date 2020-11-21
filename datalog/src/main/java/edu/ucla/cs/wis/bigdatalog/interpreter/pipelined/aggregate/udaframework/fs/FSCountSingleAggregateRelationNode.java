package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// 1 Heap tuplestore only
public class FSCountSingleAggregateRelationNode 
	extends FSAggregateRelationNodeBase {
	
	protected FSCountSingleAggregateRelationNode counterpart;
	protected int					numberOfJoinKeys;
	protected IndexCursor			detailsCursor;
	protected int					valueOffset;
	protected DataType				valueDataType;
	
	public FSCountSingleAggregateRelationNode(String predicateName, NodeArguments args, 
			Binding binding, VariableList freeVariables, boolean isRead, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, isRead, aggregateStoreType);
	}
	
	@Override
	public boolean initialize() {
		/*READ & WRITE NEED THIS*/
		// we subtract two, because 1 columns for MultiValue and for AggrValue
		this.numberOfKeyColumns = this.arity - 2;
				
		// in this version, we assume 1 aggregate column
		// fscnt aggregates requires 1 column for the total summed multiplicity (last) and n columns for each join key in fscnt<()>
		this.numberOfAggregateColumns = 1;
		// FSCNT will be using a functor
		InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.arity - 2);
		if (func.getArgument(0) instanceof InterpreterFunctor)
			this.numberOfJoinKeys = ((InterpreterFunctor)func.getArgument(0)).getArguments().size();
		else
			this.numberOfJoinKeys = 1;
		this.aggregateValueVariable = (Variable)this.getArgument(this.arity - 1);
		
		int keyColumns[] = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;
		
		DataType[] schema = this.determineSchemaDetail(keyColumns, this.numberOfJoinKeys);
		
		this.relation = this.relationManager.createAggregateRelationHeap(this.getRelationName(), schema, keyColumns);
		this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
		
		this.valueDataType = schema[schema.length - 1];
		
		int[] detailsIndexedColumns = new int[this.numberOfKeyColumns + this.numberOfJoinKeys];
		
		for (int i = 0; i < (this.numberOfKeyColumns + this.numberOfJoinKeys); i++)
			detailsIndexedColumns[i] = i;
		
		this.relation.addSecondaryIndex(detailsIndexedColumns);
		
		if (this.isReadAggregate) {
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
			
			this.detailsCursor = (IndexCursor)this.database.getCursorManager().createCursor(this.relation, detailsIndexedColumns);
			
			return this.getChild(0).initialize();
		}
		/*ONLY WRITE NEEDS THIS*/
		this.counterpart = (FSCountSingleAggregateRelationNode)readAggregateNodes.pop();
		this.counterpart.counterpart = this;
				
		this.oldValue = this.cursor.getEmptyTuple();
		this.counterpart.oldValue = this.oldValue;
		this.newValue = this.cursor.getEmptyTuple();				
				
		this.valueOffset = 0;
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.valueOffset += schema[i].getNumberOfBytes();
							
		for (int i = this.numberOfKeyColumns; i < (this.numberOfKeyColumns + this.numberOfJoinKeys); i++)
			this.valueOffset += schema[i].getNumberOfBytes();
						
		return true;
	}
	
	protected Status getReadTuple() {
		Status status = Status.FAIL;
		boolean wasEntry = this.isEntry;

		// if we have a new value for to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		if (this.getOrNodeTuple() == Status.SUCCESS) {
			if (this.numberOfKeyColumns > 0) {
				DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns + this.numberOfJoinKeys];
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					boundValues[i] = this.getArgumentAsDbType(i);

				// should only be 1 functor holding the join keys
				if (this.getArgument(this.numberOfKeyColumns) instanceof InterpreterFunctor) {
					InterpreterFunctor functor = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
					if (functor.getArgument(0) instanceof InterpreterFunctor)
						functor = (InterpreterFunctor)functor.getArgument(0);
					
					for (int i = 0; i < this.numberOfJoinKeys; i++)
						boundValues[this.numberOfKeyColumns + i] = functor.getArgument(i).toDbType(this.typeManager);
				}
				
				((SelectionCursor<AddressedTuple>)this.detailsCursor).reset(boundValues);
			}

			this.hasOld = (this.detailsCursor.getTuple((AddressedTuple) this.oldValue) > 0);
			
			// if we're not in a recursive situation and we have already aggregated this value before,
			// we must clear the old max value - if we delete the detail value, it looks like a new one
			// and we re-aggregate for the previous total
			if (wasEntry && !this.isInClique()) {
				if (this.hasOld) {
					this.relation.remove(this.oldValue);
					this.hasOld = false;
				}
			}

			this.aggregateValueVariable.makeFree();
			
			if (this.hasOld) {
				this.counterpart.currentLeaf = ((TupleUnorderedHeapStore)this.relation.getTupleStore()).getPage(((AddressedTuple)this.oldValue).address);
				this.counterpart.currentTupleAddress = ((TupleUnorderedHeapStore)this.relation.getTupleStore()).getAddressInPage(((AddressedTuple)this.oldValue).address);

				if (this.setArgumentValues(FSValueType.OldFS))
					status = Status.SUCCESS;				
			} else {
				this.counterpart.currentLeaf = null;
				this.counterpart.currentTupleAddress = -1;

				if (this.setArgumentValues(FSValueType.OldFSAsZero))
					status = Status.SUCCESS;
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
				// we are going to update the previous old value from the detailsRelation
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbTypeBase newMultiplicity = func.getArgument(1).toDbType(this.typeManager);
 				
				newMultiplicity.getBytes(this.currentLeaf.getData().getData(), (this.currentLeaf.getBytesPerTuple() * this.currentTupleAddress) + this.valueOffset);

				this.counterpart.hasOld = false;
			} else { 
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.newValue.columns[i] = this.getArgumentAsDbType(i);
				
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
				DbNumericType value = (DbNumericType) func.getArgument(1).toDbType(this.typeManager);
				
				this.newValue.columns[this.numberOfKeyColumns] = key;
				
				if (this.valueDataType == value.getDataType())
					this.newValue.columns[this.numberOfKeyColumns + 1] = value;
				else
					this.newValue.columns[this.numberOfKeyColumns + 1] = value.convertTo(this.valueDataType);
				
				this.relation.add(this.newValue, true);
			}
			
			this.aggregateValueVariable.setValue(this.getAggregateValue());
			this.isEntry = false;
			status = Status.SUCCESS;
		} else {
			status = Status.FAIL;
			this.cleanUp();
		}

		return status;
	}
	
	private DbTypeBase getAggregateValue() {
		// PROCESS:
		// 1) Search for tuple in summaryRelation. Result:
		//	a) not in summaryRelation
		//	b) in summaryRelation
		// 2) if a), then insert new tuple into summaryRelation with 'multiplicity'
		//	else b), then update existing tuple from summaryRelation by incrementing count by 'multiplicity'
		// 3) return value

		DbNumericType value;
		// last column holds the multiplicity
		int multiplicityColumnPosition = this.relation.getArity() - 1;		
		DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
		
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			boundValues[i] = this.getArgumentAsDbType(i);
		
		if (this.cursor instanceof SelectionCursor)
			((SelectionCursor<AddressedTuple>)this.cursor).reset(boundValues);
		else
			this.cursor.reset();
						
		// aggregate all tuples to get combined count - it is in the last column
		if (this.isResultInteger)
			value = this.typeManager.castToCountDataType(0);
		else
			value = DbInteger.create(0).convertTo(this.newValue.getColumn(multiplicityColumnPosition).getDataType());
		
	while (this.cursor.getTuple(this.newValue) > 0)
			value = value.add((DbNumericType) this.newValue.getColumn(multiplicityColumnPosition));
		
		return value;
	}
}
