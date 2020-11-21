package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//1 Heap or 1 Bplustree or 1 B+Tree Aggregator (BAT) Tuplestore used
public class FSCountKeyValueAggregateRelationNode 
	extends FSAggregateRelationNode {	
	protected int					numberOfJoinKeys;
	protected InterpreterFunctor 	aggregateTerm;
	protected DbKeyValueStore		currentKeyValue;
	protected DbTypeBase[]			searchKey;

	public FSCountKeyValueAggregateRelationNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, AggregateInfo[] fsAggregateInfos, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, fsAggregateInfos, aggregateStoreType);
	}
	
	@Override
	public boolean initialize() {
		// we subtract two, because 1 columns for MultiValue and for AggrValue
		this.numberOfKeyColumns = this.arity - 2;
		
		// in this version, we assume 1 aggregate column
		// fscnt aggregates requires 1 column for the total summed multiplicity (last) and n columns for each join key in fscnt<()>
		this.numberOfAggregateColumns = 1;
		
		// FSCNT will be using a functor
		this.aggregateTerm = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
		if (this.aggregateTerm.getArgument(0) instanceof InterpreterFunctor) {
			this.aggregateTerm = (InterpreterFunctor)this.aggregateTerm.getArgument(0);
			this.numberOfJoinKeys = this.aggregateTerm.getArguments().size();
		} else {
			this.numberOfJoinKeys = 1;
		}
				
		this.aggregateValueVariable = (Variable)this.getArgument(this.arity - 1);

		// check if all keys are bound
		this.allKeyColumnsBound = true;
		for (int i = 0; i < this.numberOfKeyColumns; i++) {
			if (this.getBinding(i) == BindingType.FREE) {
				this.allKeyColumnsBound = false;
				break;
			}
		}
		
		int[] keyColumns = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;
		// create a relation to store the results. Example: 
		// (a, b, aggr, [[a, 1], [c, 1]])
		int relationArity = this.numberOfKeyColumns + 1 + 1;
		
		DataType[] schema = new DataType[relationArity];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			schema[i] = this.getArgument(i).getDataType();
						
		schema[relationArity - 2] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;
		schema[relationArity - 1] = DataType.KEYVALUESTORE;
		
		String relationName = this.getPredicateName().substring(0, this.predicateName.lastIndexOf("_"));
		switch (this.aggregateStoreType) {
			case Heap:
				this.relation = this.relationManager.createAggregateRelationHeap(relationName, schema, keyColumns, this.isChangeTrackingStore());
				this.oldTuple = this.relation.getEmptyTuple();
				this.newTuple = this.relation.getEmptyTuple();
				break;
			case BPlusTree:
				this.relation = this.relationManager.createAggregateRelationBPlusTree(relationName, schema, keyColumns, this.isChangeTrackingStore());
				this.oldTuple = this.relation.getEmptyTuple();
				this.newTuple = this.relation.getEmptyTuple();
				break;
			case Aggregator:
				TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.Aggregation, keyColumns);
				tsc.setUniqueValue(true);
				tsc.setAggregateInfos(this.aggregateInfos);						
				this.relation = this.relationManager.createAggregateRelationAggregator(relationName, schema, tsc);
				this.capturedTuple = this.relation.getEmptyTuple();
				break;
		}

		this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
		
		this.searchKey = new DbTypeBase[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.searchKey[i] = DbTypeBase.loadFrom(schema[i], 0);
			
		return this.getChild(0).initialize();
	}
	
	@Override
	public Status getTuple() {
		this.traceGetTupleEntry();

		Status status = Status.FAIL;
		
		// if materialize execution mode:
		//  1) materialize relation
		//  2) switch mode to retrieve
		//  3) retrieve tuple from relation
		// 	4) when no more tuples to retrieve, switch mode to derive
		// else (pipelined)
		//  1) derive and return tuple until exhaustion
		if (this.executionMode == ExecutionMode.Pipelined) {
			if (this.aggregateStoreType == AggregateStoreType.Aggregator)
				status = this.doGetTupleAggregator();
			else
				status = this.doGetTuple();			
		} else {
			if (!this.isMaterialized) {
				if (this.aggregateStoreType == AggregateStoreType.Aggregator)
					status = this.doGetTupleAggregatorMO();
				else
					status = this.doGetTupleMO();

				/*if (this.returnResults) {
					this.changesCursor = CursorManager.createChangeTrackerCursor(this.relation);
					this.changesCursor.initializeWithKeys();
				}*/
			}
			
			//if (this.returnResults)
			//	status = this.doRetrieveTuple();

			// after a failure, we can materialize again
			if (status == Status.FAIL)
				this.isMaterialized = false;
		}
		
		this.traceGetTupleExit(status);

		return status;
	}
	
	@Override
	public Status doGetTuple() {
		Status status = Status.FAIL;
		boolean hasOld = false;
		int numberOfTuplesDerived = 0;

		this.aggregateValueVariable.makeFree();
		
		// if we have a new value to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		while (this.getOrNodeTuple() == Status.SUCCESS) {
			numberOfTuplesDerived++;
			if (this.numberOfKeyColumns == 0) {
				this.cursor.reset();
			} else {
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.searchKey[i] = this.getArgumentAsDbType(i);

				((SelectionCursor<?>)this.cursor).reset(this.searchKey);
			}
			
			hasOld = (this.cursor.getTuple(this.oldTuple) > 0);
			
			// if we're not in a recursive situation and we have already aggregated this value before,
			// we must clear the old max value - if we delete the detail value, it looks like a new one
			// and we re-aggregate for the previous total
			if (!this.isInClique()) {
				if (hasOld) {
					DbTypeBase key = this.aggregateTerm.getArgument(0).toDbType(this.typeManager);
					DbKeyValueStore values = (DbKeyValueStore)this.oldTuple.columns[this.numberOfKeyColumns+1];
					DbNumericType oldValue = (DbNumericType) values.get(key);
					values.remove(key);
					
					// if we remove the only item in the list, delete the tuple
					if (values.getNumberOfEntries() == 0) {
						this.relation.remove(this.oldTuple);
						hasOld = false;
					} else {
						DbTypeBase newValue;
						for (int i = 0; i < this.newTuple.columns.length - 1; i++)
							this.newTuple.columns[i] = this.oldTuple.columns[i];
												
						if (oldValue != null) {
							// get new total value after withdrawing the item we just removed
							//if (this.isResultInteger)
							newValue = ((DbNumericType)this.oldTuple.columns[this.numberOfKeyColumns]).subtract(oldValue);
							//else
							//	newValue = DbFloat.create(this.oldTuple.columns[this.numberOfKeyColumns].getFloatValue() - oldValue.getFloatValue());
							
							this.newTuple.columns[this.numberOfKeyColumns] = newValue;
							this.oldTuple.columns[this.numberOfKeyColumns] = newValue;
							((DerivedRelation)this.relation).update(this.oldTuple, this.newTuple);
						}
					}
				}
			}
			
			DbNumericType newTotalValue = null;
			DbNumericType oldTotalValue = null;
			DbTypeBase key = this.aggregateTerm.getArgument(0).toDbType(this.typeManager);
			DbNumericType newPartialValue = (DbNumericType) this.aggregateTerm.getArgument(1).toDbType(this.typeManager);
			DbKeyValueStore keyValueColumn = null;
			
			if (hasOld) {
				keyValueColumn = ((DbKeyValueStore)this.oldTuple.getColumn(this.numberOfKeyColumns + 1));
				oldTotalValue = (DbNumericType) this.oldTuple.getColumn(this.numberOfKeyColumns);
				
				DbNumericType oldPartialValue = (DbNumericType) keyValueColumn.get(key);
				if (oldPartialValue != null)
					if (!newPartialValue.greaterThan(oldPartialValue))
						continue;

				// since it is higher than the previous value, set the value for the key
				keyValueColumn.put(key, newPartialValue);
				
				if (oldPartialValue == null)
					newTotalValue = oldTotalValue.add(newPartialValue);
				else
					newTotalValue = oldTotalValue.add(newPartialValue.subtract(oldPartialValue));					

				if (newTotalValue.lessThan(this.typeManager.castToCountDataType(0)))
					throw new DatabaseException("Overflow!");
							
				this.oldTuple.columns[this.numberOfKeyColumns] = newTotalValue;				
				((DerivedRelation)this.relation).update(this.oldTuple);
				
				this.setNewOrUpdatedTuple(false);
			} else {				
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.newTuple.columns[i] = this.getArgumentAsDbType(i);
								
				DbKeyValueStore keyValue = this.typeManager.createKeyValueStore(key.getDataType(), newPartialValue.getDataType());
				
				keyValue.put(key, newPartialValue);
				this.newTuple.columns[this.numberOfKeyColumns] = newPartialValue;
				this.newTuple.columns[this.numberOfKeyColumns + 1] = keyValue;
				newTotalValue = newPartialValue;
				
				this.relation.add(this.newTuple, true);				
				this.setNewOrUpdatedTuple(true);
			}
						
			this.aggregateValueVariable.setValue(newTotalValue);
			status = Status.SUCCESS;
			break;
		}
		
		if (numberOfTuplesDerived > 1) {			
			this.numberOfRecursiveFactsDerived += (numberOfTuplesDerived - 1);
			// subtract 1 since fscliquenode4 will add one for the tuple produced here
			if (this.clique != null)
				this.clique.numberOfGeneratedFactsThisIteration += (numberOfTuplesDerived - 1);
		}
		return status;
	}
	
	public Status doGetTupleMO() {
		boolean hasOld = false;
		int numberOfTuplesDerived = 0;

		this.aggregateValueVariable.makeFree();
	
		// if we have a new value to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		while (this.getOrNodeTuple() == Status.SUCCESS) {
			numberOfTuplesDerived++;
			if (this.numberOfKeyColumns == 0) {
				this.cursor.reset();
			} else {
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.searchKey[i] = this.getArgumentAsDbType(i);

				((SelectionCursor<?>)this.cursor).reset(this.searchKey);
			}
			
			hasOld = (this.cursor.getTuple(this.oldTuple) > 0);
			
			// if we're not in a recursive situation and we have already aggregated this value before,
			// we must clear the old max value - if we delete the detail value, it looks like a new one
			// and we re-aggregate for the previous total
			if (!this.isInClique()) {
				if (hasOld) {
					DbTypeBase key = this.aggregateTerm.getArgument(0).toDbType(this.typeManager);
					DbKeyValueStore values = (DbKeyValueStore)this.oldTuple.columns[this.numberOfKeyColumns+1];
					DbNumericType oldValue = (DbNumericType) values.get(key);
					values.remove(key);
					
					// if we remove the only item in the list, delete the tuple
					if (values.getNumberOfEntries() == 0) {
						this.relation.remove(this.oldTuple);
						hasOld = false;
					} else {
						DbTypeBase newValue;
						for (int i = 0; i < this.newTuple.columns.length - 1; i++)
							this.newTuple.columns[i] = this.oldTuple.columns[i];
												
						if (oldValue != null) {
							// get new total value after withdrawing the item we just removed
							//if (this.isResultInteger)
							newValue = ((DbNumericType)this.oldTuple.columns[this.numberOfKeyColumns]).subtract(oldValue);
							//else
							//	newValue = DbFloat.create(this.oldTuple.columns[this.numberOfKeyColumns].getFloatValue() - oldValue.getFloatValue());
							
							this.newTuple.columns[this.numberOfKeyColumns] = newValue;
							this.oldTuple.columns[this.numberOfKeyColumns] = newValue;
							((DerivedRelation)this.relation).update(this.oldTuple, this.newTuple);
						}
					}
				}
			}
			
			DbNumericType newTotalValue = null;
			DbNumericType oldTotalValue = null;
			DbTypeBase key = this.aggregateTerm.getArgument(0).toDbType(this.typeManager);
			DbNumericType newPartialValue = (DbNumericType) this.aggregateTerm.getArgument(1).toDbType(this.typeManager);
			DbKeyValueStore keyValueColumn = null;
			
			if (hasOld) {
				keyValueColumn = ((DbKeyValueStore)this.oldTuple.getColumn(this.numberOfKeyColumns + 1));
				oldTotalValue = (DbNumericType) this.oldTuple.getColumn(this.numberOfKeyColumns);
				
				DbNumericType oldPartialValue = (DbNumericType) keyValueColumn.get(key);
				if (oldPartialValue != null)
					if (!newPartialValue.greaterThan(oldPartialValue))
						continue;

				// since it is higher than the previous value, set the value for the key
				keyValueColumn.put(key, newPartialValue);
				
				if (oldPartialValue == null)
					newTotalValue = oldTotalValue.add(newPartialValue);
				else
					newTotalValue = oldTotalValue.add(newPartialValue.subtract(oldPartialValue));					
				
				if (newTotalValue.lessThan(this.typeManager.castToCountDataType(0)))
					throw new DatabaseException("Overflow!");
			
				this.oldTuple.columns[this.numberOfKeyColumns] = newTotalValue;				
				((DerivedRelation)this.relation).update(this.oldTuple);
			} else {			
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.newTuple.columns[i] = this.getArgumentAsDbType(i);
								
				DbKeyValueStore keyValue = this.typeManager.createKeyValueStore(key.getDataType(), newPartialValue.getDataType());
				
				keyValue.put(key, newPartialValue);
				this.newTuple.columns[this.numberOfKeyColumns] = newPartialValue;
				this.newTuple.columns[this.numberOfKeyColumns + 1] = keyValue;
				newTotalValue = newPartialValue;
				
				this.relation.add(this.newTuple, true);				
			}
		}

		this.numberOfRecursiveFactsDerived += numberOfTuplesDerived;
		if (this.clique != null)
			this.clique.numberOfGeneratedFactsThisIteration = numberOfTuplesDerived;

		return Status.FAIL;
	}
	
	protected Status doGetTupleAggregator() {
		int numberOfTuplesDerived = 0;
		Status status = Status.FAIL;
		
		// if we have a new value to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		while (this.getOrNodeTuple() == Status.SUCCESS) {
			numberOfTuplesDerived++;
			//TODO - add back in the isInClique() clique
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
			
			this.capturedTuple.columns[this.numberOfKeyColumns] = this.aggregateTerm.getArgument(0).toDbType(this.typeManager);			
			this.capturedTuple.columns[this.numberOfKeyColumns + 1] = this.aggregateTerm.getArgument(1).toDbType(this.typeManager);
			
			// aggregator in the BPlusTree will make change if not same value
			((AggregateRelation)this.relation).put(this.capturedTuple, this.aggregatorResult);
			
			if (this.aggregatorResult.status == AggregatorInsertStatus.FAIL)
				continue;
						
			if (this.aggregatorResult.status == AggregatorInsertStatus.NO_CHANGE)
				if (this.isInClique())
					continue;
			
			if (this.aggregatorResult.newTotalValue.lessThan(this.typeManager.castToCountDataType(0)))
				throw new DatabaseException("Overflow!");

			this.aggregateValueVariable.setValue(this.aggregatorResult.newTotalValue);
			status = Status.SUCCESS;
			
			this.setNewOrUpdatedTuple(this.aggregatorResult.status == AggregatorInsertStatus.NEW);

			break;
		}
		
		if (numberOfTuplesDerived > 1) {
			this.numberOfRecursiveFactsDerived += (numberOfTuplesDerived - 1);
			// subtract 1 since emsncliquenode4 will add one for the tuple produced here
			if (this.clique != null)
				this.clique.numberOfGeneratedFactsThisIteration += (numberOfTuplesDerived - 1);
		}
		
		return status;
	}
	
	protected Status doGetTupleAggregatorMO() {
		int numberOfTuplesDerived = 0;
		while (this.getOrNodeTuple() == Status.SUCCESS) {
			numberOfTuplesDerived++;
			//TODO - add back in the isInClique() clique
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
			
			this.capturedTuple.columns[this.numberOfKeyColumns] = this.aggregateTerm.getArgument(0).toDbType(this.typeManager);			
			this.capturedTuple.columns[this.numberOfKeyColumns + 1] = this.aggregateTerm.getArgument(1).toDbType(this.typeManager);
			
			((AggregateRelation)this.relation).put(this.capturedTuple, this.aggregatorResult);
			
			if (this.aggregatorResult.newTotalValue.lessThan(this.typeManager.castToCountDataType(0)))
				throw new DatabaseException("Overflow!");
		}
		
		// we must do this set here because addTuple() in emsnfscliquenode will never be entered
		this.numberOfRecursiveFactsDerived += numberOfTuplesDerived;
		if (this.clique != null)
			this.clique.numberOfGeneratedFactsThisIteration = numberOfTuplesDerived;
		
		return Status.FAIL;
	}
	
	@Override
	public FSCountKeyValueAggregateRelationNode copy(ProgramContext programContext) {
		FSCountKeyValueAggregateRelationNode copy = new FSCountKeyValueAggregateRelationNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList),
				programContext.copyAggregateInfos(this.aggregateInfos), 
				this.aggregateStoreType);
		
		copy.executionMode = this.executionMode;
		copy.clique = (CliqueNode) programContext.getCliqueMapping().get(this.clique);
		
		programContext.getNodeMapping().put(this, copy);
				
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
		
		return copy;
	}
}
