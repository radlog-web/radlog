package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// 1 Heap tuplestore only
public class FSCountSingleKeyValueAggregateRelationNode 
	extends FSAggregateRelationNodeBase {

	protected FSCountSingleKeyValueAggregateRelationNode counterpart;
	protected int					numberOfJoinKeys;
	protected int						valueOffset;
	protected DbKeyValueStore			currentKeyValue;
	private DataType					valueDataType;

	public FSCountSingleKeyValueAggregateRelationNode(String predicateName, NodeArguments args, 
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
		
		// create a relation to store the results.  
		// Example: (a, b, aggr, [[a, 1], [c, 1]])
		int relationArity = this.numberOfKeyColumns + 1 + 1;
		
		DataType[] schema = new DataType[relationArity];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			schema[i] = this.getArgument(i).getDataType();
			
		if (this.getArgument(this.numberOfKeyColumns) instanceof InterpreterFunctor) {
			InterpreterFunctor functor = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
			if (functor.getArgument(0) instanceof InterpreterFunctor) 
				functor = (InterpreterFunctor)functor.getArgument(0);
			schema[relationArity - 2] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;
			this.valueDataType = schema[relationArity - 2]; 
			schema[relationArity - 1] = DataType.KEYVALUESTORE;
		} else {
			throw new InterpreterException("Functor not where expected");
		}
		
		this.relation = this.relationManager.createAggregateRelationHeap(this.getRelationName(), schema, keyColumns);
		this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
		
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
			
			return this.getChild(0).initialize();
		}
		
		/*ONLY WRITE NEEDS THIS*/
		this.counterpart = (FSCountSingleKeyValueAggregateRelationNode)readAggregateNodes.pop();
		this.counterpart.counterpart = this;
		this.oldValue = this.relation.getEmptyTuple();
		this.counterpart.oldValue = this.oldValue;
		this.newValue = this.relation.getEmptyTuple();
		this.counterpart.newValue = this.newValue;
		
		this.valueOffset = 0;
		for (int i = 0; i < (this.numberOfKeyColumns); i++)
			this.valueOffset += schema[i].getNumberOfBytes();			
				
		return true;
	}
	
	protected Status getReadTuple() {
		Status status = Status.FAIL;
		boolean wasEntry = this.isEntry;

		// if we have a new value for to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		if (this.getOrNodeTuple() == Status.SUCCESS) {
			this.numberOfRecursiveFactsDerived++;
			if (this.clique != null)
				this.clique.numberOfGeneratedFactsThisIteration++;

			if (this.numberOfKeyColumns > 0) {
				DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns + this.numberOfJoinKeys];
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					boundValues[i] = this.getArgumentAsDbType(i);
				
				((SelectionCursor<AddressedTuple>)this.cursor).reset(boundValues);
			}
			
			this.hasOld = (this.cursor.getTuple(this.oldValue) > 0);
			
			// if we're not in a recursive situation and we have already aggregated this value before,
			// we must clear the old max value - if we delete the detail value, it looks like a new one
			// and we re-aggregate for the previous total
			if (wasEntry && !this.isInClique()) {
				if (this.hasOld) {
					InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
					DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
					DbKeyValueStore values = (DbKeyValueStore)this.oldValue.columns[this.numberOfKeyColumns+1];
					DbNumericType oldValue = (DbNumericType) values.get(key);
					values.remove(key);
					
					// if we remove the only item in the list, delete the tuple
					if (values.getNumberOfEntries() == 0) {
						this.relation.remove(this.oldValue);
						this.oldValue = null;	
					} else {
						DbTypeBase newValue;
						for (int i = 0; i < this.newValue.columns.length; i++)
							this.newValue.columns[i] = this.oldValue.columns[i];
						
						if (oldValue != null) {
							// get new total value after withdrawing the item we just removed
							//if (this.isResultInteger) {
							newValue = ((DbNumericType) this.oldValue.columns[this.numberOfKeyColumns]).subtract(oldValue);
							//} else {
							//	newValue = DbFloat.create(this.oldValue.columns[this.numberOfKeyColumns].getFloatValue() - oldValue.getFloatValue());
							//}
							this.newValue.columns[this.numberOfKeyColumns] = newValue;
							// set oldValue so when its used later, it has the correct value
							this.oldValue.columns[this.numberOfKeyColumns] = newValue;
							((DerivedRelation)this.relation).update(this.oldValue, this.newValue);
						}
					}
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
				this.counterpart.currentKeyValue = null;

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
			DbTypeBase newTotalValue = null;
			
			if (this.counterpart.hasOld) {				
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
				DbNumericType newPartialValue = (DbNumericType) func.getArgument(1).toDbType(this.typeManager);

				DbNumericType oldPartialValue = (DbNumericType) this.currentKeyValue.put(key, newPartialValue);

				DbNumericType oldTotalValue = (DbNumericType) this.counterpart.oldValue.getColumn(this.numberOfKeyColumns);
				//if (this.isResultInteger) {
					if (oldPartialValue == null)
						newTotalValue = oldTotalValue.add(newPartialValue);
					else
						newTotalValue = oldTotalValue.add((newPartialValue).subtract(oldPartialValue));					
				/*} else {
					if (oldPartialValue == null)
						newTotalValue = DbFloat.create(oldTotalValue.getFloatValue() + newPartialValue.getFloatValue());
					else
						newTotalValue = DbFloat.create(oldTotalValue.getFloatValue() + (newPartialValue.getFloatValue() - oldPartialValue.getFloatValue()));					
				}*/
				
				newTotalValue.getBytes(this.currentLeaf.getData().getData(), (this.currentLeaf.getBytesPerTuple() * this.currentTupleAddress) + this.valueOffset);

				this.counterpart.hasOld = false;
				this.setLastTupleModified(false);
			} else {
				// write all bound columns to tuple
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.newValue.columns[i] = this.getArgumentAsDbType(i);
				
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
				DbNumericType newPartialValue = (DbNumericType) func.getArgument(1).toDbType(this.typeManager);

				// add keyvaluetype to hold join key and multiplicty
				DbKeyValueStore keyValueStore = this.typeManager.createKeyValueStore(key.getDataType(), newPartialValue.getDataType());
				
				keyValueStore.put(key, newPartialValue);
				if (this.valueDataType != newPartialValue.getDataType())
					newPartialValue = newPartialValue.convertTo(this.valueDataType);
					
				this.newValue.columns[this.numberOfKeyColumns] = newPartialValue;
				this.newValue.columns[this.numberOfKeyColumns + 1] = keyValueStore;
				newTotalValue = newPartialValue;
				
				this.relation.add(this.newValue, true);
				this.setLastTupleModified(true);
			}			
				
			// subtract 1 here since FSCliqueNode will add 1
			this.numberOfRecursiveFactsDerived--;
			if (this.clique != null)
				this.clique.numberOfGeneratedFactsThisIteration--;
			
			this.aggregateValueVariable.setValue(newTotalValue);
					
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
				// the key for the join is in the fscnt<> aggregate argument
				InterpreterFunctor argumentWithKey = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbTypeBase key = argumentWithKey.getArgument(0).toDbType(this.typeManager);
				
				this.counterpart.currentKeyValue = (DbKeyValueStore)this.oldValue.getColumn(this.numberOfKeyColumns+1);
				DbTypeBase dbType = this.counterpart.currentKeyValue.get(key);
				
				if (dbType == null)
					dbType = this.typeManager.castToCountDataType(0);

				isMatch = this.aggregateValueVariable.match(dbType);
			break;
			
			case OldFSAsZero:
				isMatch = this.aggregateValueVariable.match(this.typeManager.castToCountDataType(0));
				break;						
		}

		return isMatch;
	}
}
