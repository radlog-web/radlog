package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//1 Heap or 1 B+Tree Tuplestore used
public class FSMinMaxAggregateRelationNode 
	extends FSAggregateRelationNode  {

	protected DbTypeBase[]			searchKey;
	protected DataType[] 			schema;
	protected int[] 				keyColumns;
	//private ChangeTrackerCursor<?> changesCursor;
	
	public FSMinMaxAggregateRelationNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, AggregateInfo[] aggregateInfos, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, aggregateInfos, aggregateStoreType);
	}

	@Override
	public boolean initialize() {
		this.numberOfKeyColumns = this.arity - 2;
		this.numberOfAggregateColumns = 1;
		this.aggregateValueVariable = (Variable)this.getArgument(this.arity - 1);

		// check if all keys are bound
		this.allKeyColumnsBound = true;
		for (int i = 0; i < this.numberOfKeyColumns; i++) {
			if (this.getBinding(i) == BindingType.FREE) {
				this.allKeyColumnsBound = false;
				break;
			}
		}
		
		this.keyColumns = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.keyColumns[i] = i;
		
		this.schema = this.determineSchemaMain(keyColumns);

		// if the last column is an integer, see if we need to promote it to a long
		// this is necessary if the TypeAssigner recognizes a mixed fs aggregate situation
		if (this.schema[this.schema.length - 1] == DataType.INT)
			if (this.aggregateValueVariable.getDataType() == this.deALSContext.getConfiguration().getCountDataType())
				this.schema[this.schema.length - 1] = this.deALSContext.getConfiguration().getCountDataType();		
		
		String relationName = this.getPredicateName().substring(0, this.predicateName.lastIndexOf("_"));
		
		switch (this.aggregateStoreType) {
			case Heap:
				this.relation = this.relationManager.createAggregateRelationHeap(relationName, this.schema, this.keyColumns, this.isChangeTrackingStore());
				this.oldTuple = this.relation.getEmptyTuple();
				this.newTuple = this.relation.getEmptyTuple();
				break;	
			case BPlusTree:
				this.relation = this.relationManager.createAggregateRelationBPlusTree(relationName, this.schema, this.keyColumns, this.isChangeTrackingStore());
				this.oldTuple = this.relation.getEmptyTuple();
				this.newTuple = this.relation.getEmptyTuple();
				break;
			case Aggregator:
				TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.Aggregation, this.keyColumns);
				tsc.setUniqueValue(true);
				tsc.setAggregateInfos(this.aggregateInfos);	
				
				this.relation = this.relationManager.createAggregateRelationAggregator(relationName, this.schema, tsc);
				this.capturedTuple = this.relation.getEmptyTuple();
				this.newTuple = this.relation.getEmptyTuple();
				break;
		}
		
		this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);

		this.searchKey = new DbTypeBase[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.searchKey[i] = DbTypeBase.loadFrom(schema[i], 0);

		return this.getChild(0).initialize();
	}
	
	@Override
	public void cleanUpData() {
		this.isMaterialized = false;
		super.cleanUpData();
	}
	
	/*
	@Override
	public Status getTuple() {
		if (this.executionMode == ExecutionMode.Materialized
				&& this.isMaterialized)
			return this.retrieveTuple(this.isEntry);

		if (this.aggregateStoreType == AggregateStoreType.Aggregator)
			return this.doDeriveTupleAggregator();
		
		return this.doDeriveTuple();
	}*/
	
	@Override
	public Status getTuple() {
		Status status = Status.FAIL;

		this.traceGetTupleEntry();		
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
		/*
		System.out.println(this.relation.getTupleStore().getNumberOfTuples());
		
		Cursor<Tuple> cursor = (Cursor<Tuple>) this.database.getCursorManager().createScanCursor(this.relation);
		Tuple t = cursor.getEmptyTuple();
		int i = 0;
		while (cursor.getTuple(t) == 1) {
			System.out.println(t);
			i++;
		}
		System.out.println(i);*/
		this.traceGetTupleExit(status);

		return status;
	}

	protected Status doGetTuple() {
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
			
			DbTypeBase newValue = this.getArgumentAsDbType(this.numberOfKeyColumns);
			if (hasOld) {
				DbTypeBase oldValue = this.oldTuple.getColumn(this.numberOfKeyColumns);
				if (this.aggregateInfos[0].aggregateType == AggregateFunctionType.FSMAX) {
					if (!newValue.greaterThan(oldValue))
						continue;
				} else {
					if (!newValue.lessThan(oldValue))
						continue;
				}
			}
			
			if (hasOld) {
				this.oldTuple.columns[this.numberOfKeyColumns] = newValue;
				((DerivedRelation)this.relation).update(this.oldTuple);
			} else {
				for (int i = 0; i < this.numberOfKeyColumns; i++) 
					this.newTuple.columns[i] = this.getArgumentAsDbType(i);

				this.newTuple.columns[this.numberOfKeyColumns] = newValue;
			
				((DerivedRelation)this.relation).add(this.newTuple, true);
			}
			
			this.aggregateValueVariable.setValue(newValue);
			status = Status.SUCCESS;
			
			this.setNewOrUpdatedTuple(!hasOld);

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
	
	protected Status doGetTupleAggregator() {
		Status status = Status.FAIL;
		// if we have a new value to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		int numberOfTuplesDerived = 0;
		while (this.getOrNodeTuple() == Status.SUCCESS) {
			numberOfTuplesDerived++;
			for (int i = 0; i < this.capturedTuple.columns.length; i++)
				this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);

			// aggregator will make no change if same value 
			((AggregateRelation)this.relation).put(this.capturedTuple, this.aggregatorResult);
			if ((this.aggregatorResult.status == AggregatorInsertStatus.NO_CHANGE)
					|| (this.aggregatorResult.status == AggregatorInsertStatus.FAIL))
				continue;
			
			this.aggregateValueVariable.setValue(this.capturedTuple.columns[this.numberOfKeyColumns]);
			status = Status.SUCCESS;
		
			this.setNewOrUpdatedTuple(this.aggregatorResult.status == AggregatorInsertStatus.NEW);
			
			break;
		}
		
		if (numberOfTuplesDerived > 1) {
			this.numberOfRecursiveFactsDerived += (numberOfTuplesDerived - 1);
			if (this.clique != null)	
				this.clique.numberOfGeneratedFactsThisIteration += (numberOfTuplesDerived - 1);
		}
		
		return status;
	}
	
	protected Status doGetTupleMO() {
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

			DbTypeBase newValue = this.getArgumentAsDbType(this.numberOfKeyColumns);
			if (hasOld) {
				DbTypeBase oldValue = this.oldTuple.getColumn(this.numberOfKeyColumns);
				if (this.aggregateInfos[0].aggregateType == AggregateFunctionType.FSMAX) {
					if (!newValue.greaterThan(oldValue))
						continue;					
				} else {
					if (!newValue.lessThan(oldValue)) 
						continue;
				}				
			}

			if (hasOld) {
				this.oldTuple.columns[this.numberOfKeyColumns] = newValue;
				((DerivedRelation)this.relation).update(this.oldTuple);
			} else {
				for (int i = 0; i < this.numberOfKeyColumns; i++) 
					this.newTuple.columns[i] = this.getArgumentAsDbType(i);

				this.newTuple.columns[this.numberOfKeyColumns] = newValue;

				((DerivedRelation)this.relation).add(this.newTuple, true);
			}
		}
		
		this.isMaterialized = true;
		
		// done deriving, so now produce the tuples
		this.numberOfRecursiveFactsDerived += numberOfTuplesDerived;
		if (this.clique != null)
			this.clique.numberOfGeneratedFactsThisIteration = numberOfTuplesDerived;			
				
		return Status.FAIL;
	}
	
	protected Status doGetTupleAggregatorMO() {
		int numberOfTuplesDerived = 0;
		this.aggregateValueVariable.makeFree();

		while (this.getOrNodeTuple() == Status.SUCCESS) {
			numberOfTuplesDerived++;
			for (int i = 0; i < this.capturedTuple.columns.length; i++) 
				this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);
 
			((AggregateRelation)this.relation).put(this.capturedTuple, this.aggregatorResult);
		}
		
		this.isMaterialized = true;

		// we must do this set here because addTuple() in emsnfscliquenode will never be entered
		this.numberOfRecursiveFactsDerived += numberOfTuplesDerived;
		if (this.clique != null)
			this.clique.numberOfGeneratedFactsThisIteration = numberOfTuplesDerived;
		
		return Status.FAIL;
	}
	/*
	private Status doRetrieveTuple() {
		Status status = Status.FAIL;
		
		while (this.changesCursor.getTuple(this.newTuple) > 0) {
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				if (!this.arguments.innerArguments[i].isConstant())
					((Variable)this.arguments.innerArguments[i]).value = this.newTuple.columns[i];
			
			this.aggregateValueVariable.value = this.newTuple.columns[this.numberOfKeyColumns];

			status = Status.SUCCESS;
			this.setNewOrUpdatedTuple(true);
				
			// we must subtract 1 for each tuple addTuple() will process so as to not double count
			this.numberOfRecursiveTuplesDerived -= 1;
			if (this.clique != null)
				this.clique.numberOfDerivedTuplesThisIteration -= 1;
			
			break;
		}
		
		if (status != Status.SUCCESS){			
			status = Status.FAIL;
			this.isMaterialized = false;
		} 
			
		return status;
	}*/
	
	public void initializeRelation() {
		String relationName = this.getPredicateName().substring(0, this.predicateName.lastIndexOf("_"));
		TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.Aggregation, this.keyColumns);
		tsc.setUniqueValue(true);
		tsc.setAggregateInfos(this.aggregateInfos);	
				
		this.relation = this.relationManager.createAggregateRelationAggregator(relationName, this.schema, tsc);
	}
	
	@Override
	public FSMinMaxAggregateRelationNode copy(ProgramContext programContext) {
		FSMinMaxAggregateRelationNode copy = new FSMinMaxAggregateRelationNode(new String(this.predicateName), 
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
