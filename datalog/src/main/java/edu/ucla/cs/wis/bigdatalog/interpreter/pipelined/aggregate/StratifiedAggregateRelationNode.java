package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate;

import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbAverage;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//1 Heap or 1 B+Tree of 1 BAT tuplestore used
public class StratifiedAggregateRelationNode 
	extends AggregateRelationNode {

	protected boolean 			hasOld;
	protected boolean 			hasNew;	
	protected Tuple 				newTuple;
	protected Tuple 				oldTuple;
	protected Cursor<?>			selectionCursor;
	protected Cursor<?>			scanCursor;
	protected DbTypeBase[] 		searchKey;
	protected DbTypeBase[] 		aggregateValues;
	protected List<StratifiedAggregateRelationNode> otherInstances;
		
	public StratifiedAggregateRelationNode(String predicateName, NodeArguments args, 
			Binding binding, VariableList freeVariables, AggregateInfo[] aggregateInfos, AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, aggregateInfos, aggregateStoreType);
	}

	public void addOtherInstance(StratifiedAggregateRelationNode instance) {
		if (this.otherInstances == null)
			this.otherInstances = new LinkedList<>(); 
		
		if (!this.otherInstances.contains(instance))
			this.otherInstances.add(instance);
	}
	
	@Override
	public boolean initialize() {
		this.numberOfAggregateColumns = this.aggregateInfos.length;
		this.numberOfKeyColumns = this.arity - this.numberOfAggregateColumns;

		// check if all keys are bound
		this.allKeyColumnsBound = true;
		for (int i = 0; i < this.numberOfKeyColumns; i++) {
			if (this.getBinding(i) == BindingType.FREE) {
				this.allKeyColumnsBound = false;
				break;
			}
		}

		int keyColumns[] = new int[0];
		// can only add an index if we have key columns
		if (this.numberOfKeyColumns > 0) {
			keyColumns = new int[this.numberOfKeyColumns];
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				keyColumns[i] = i;	
		}

		DataType[] schema = new DataType[this.arity];
		DataType[] partialSchema = this.getSchema();
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			schema[i] = partialSchema[i];
	
		int index = 0;
		for (int i = this.numberOfKeyColumns; i < this.numberOfKeyColumns + this.numberOfAggregateColumns; i++) {
			if (this.isDistinctAggregate(index)) {
				schema[i] = DataType.SET;
			} else {
				if (this.aggregateInfos[index].aggregateType == AggregateFunctionType.AVG)
					schema[i] = DataType.AVERAGE;
				else
					schema[i] = partialSchema[i];
			}
			index++;
		}

		// b-trees require keys
		if (keyColumns.length == 0)
			this.aggregateStoreType = AggregateStoreType.Heap;
		
		// aggregators only support a single aggregate function
		if ((this.aggregateStoreType == AggregateStoreType.Aggregator)
			&& (this.numberOfAggregateColumns > 1))
			this.aggregateStoreType = AggregateStoreType.BPlusTree;
		
		TupleStoreConfiguration tsc;
		
		String derivedRelationName = this.relationManager.getNextDerivedRelationName(this.predicateName, schema.length);
				
		switch (this.aggregateStoreType) {
			case Heap:
				tsc = new TupleStoreConfiguration(TupleStoreType.UnorderedHeap);
				this.relation = this.relationManager.createDerivedRelation(derivedRelationName, schema, tsc, false);
				this.oldTuple = this.relation.getEmptyTuple();
				this.newTuple = this.relation.getEmptyTuple();
				break;
			case BPlusTree:
				tsc = new TupleStoreConfiguration(TupleStoreType.BPlusTree, keyColumns);
				tsc.setUniqueValue(true);
				this.relation = this.relationManager.createDerivedRelation(derivedRelationName, schema, tsc, false);
				this.oldTuple = this.relation.getEmptyTuple();
				this.newTuple = this.relation.getEmptyTuple();
				break;
			case Aggregator:
				tsc = new TupleStoreConfiguration(TupleStoreType.Aggregation, keyColumns);
				tsc.setUniqueValue(true);
				tsc.setAggregateInfos(this.aggregateInfos);
				this.relation = this.relationManager.createAggregateRelationAggregator(derivedRelationName, schema, tsc);
				this.oldTuple = this.relation.getEmptyTuple();
				this.capturedTuple = this.relation.getEmptyTuple();
				break;
		}
		
		if (keyColumns.length > 0)
			this.selectionCursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
				
		this.scanCursor = this.database.getCursorManager().createScanCursor(this.relation);	
		this.searchKey = new DbTypeBase[this.numberOfKeyColumns];
		this.aggregateValues = new DbTypeBase[this.numberOfAggregateColumns];

		return this.getChild(0).initialize();  
	}
	
	public void cleanUpData() {
		this.isMaterialized = false;
		super.cleanUpData();
	}

	public void deleteRelationsAndCursors() {
		this.database.getCursorManager().destroyCursor(this.scanCursor);
		this.scanCursor = null;
		this.database.getCursorManager().destroyCursor(this.selectionCursor);
		this.selectionCursor = null;
		
		super.deleteRelationsAndCursors();
	}

	// get the old value for the key
	protected boolean getOldValue(boolean reset, boolean wasEntry) {	
		int status = 0;
		// with no binding, we use the scan cursor
		if (this.numberOfKeyColumns == 0) {
			this.cursor = this.scanCursor;
			if (reset)
				this.cursor.reset();
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
				if (keyNotBound)
					this.cursor = this.scanCursor;
				else
					this.cursor = this.selectionCursor;				
			}

			if (reset) {
				if (this.cursor instanceof SelectionCursor) {						
					for (int i = 0; i < this.numberOfKeyColumns; i++)
						this.searchKey[i] = this.getArgumentAsDbType(i);
					
					((SelectionCursor<?>)this.cursor).reset(this.searchKey);
				} else {
					this.cursor.reset();
				}
			}
		}
		
		status = this.cursor.getTuple(this.oldTuple);
		
		return (status > 0);
	}
	
	protected boolean setArgumentValues() {
		boolean isMatch = true;

		for (int i = 0; i < this.numberOfKeyColumns && isMatch; i++)
			isMatch = this.getArgument(i).match(this.oldTuple.getColumn(i));
		
		for (int i = 0; i < this.numberOfAggregateColumns && isMatch; i++) {
			switch (this.aggregateInfos[i].aggregateType) {
				case AVG:
					DbAverage average = (DbAverage)this.oldTuple.getColumn(i + this.numberOfKeyColumns);
					//DbComplex dbComplex = (DbComplex)this.oldTuple.getColumn(i + this.numberOfKeyColumns);
					//Pair<DbNumericType, DbNumericType> values = DataType.castToHigherType((DbNumericType)dbComplex.getArgument(0), (DbNumericType)dbComplex.getArgument(1));					
					//isMatch = this.getArgument(this.numberOfKeyColumns + i).match(this.expressions[i].evaluate(values.getFirst(), values.getSecond()));
					isMatch = this.getArgument(this.numberOfKeyColumns + i).match(average.computeAverage());
					break;
				case COUNT_DISTINCT:
					DbSet set = (DbSet)this.oldTuple.getColumn(this.numberOfKeyColumns + i); 
					isMatch = this.getArgument(this.numberOfKeyColumns + i).match(this.typeManager.castToCountDataType(set.getNumberOfEntries()));
					break;
				default:
					isMatch = this.getArgument(this.numberOfKeyColumns + i).match(this.oldTuple.getColumn(i + this.numberOfKeyColumns));
			}
		}

		return isMatch;
	}
	
	@Override
	public Status getTuple() {
		this.traceGetTupleEntry();

		Status status = Status.FAIL;
		
		if (this.isMaterialized)
			status = this.doRetrieveTuple(this.isEntry);
		else if (this.aggregateStoreType == AggregateStoreType.Aggregator)
			status = this.doDeriveTupleAggregator(); 
		else
			status = this.doDeriveTuple();

		this.traceGetTupleExit(status);

		return status;
	}
	
	private Status doDeriveTuple() {
		// free free (f) aggregate variables
		//for (int i = this.numberOfKeyColumns; i < this.arity; i++)
		//	((Variable)this.getArgument(i)).makeFree();
		if (this.isEntry)
			this.freeVariableList.makeFree();
		
		this.hasNew = false;
		boolean wasEntry = this.isEntry;
		this.isEntry = false;
		while (wasEntry && (this.getOrNodeTuple() == Status.SUCCESS)) {
			this.hasNew = true;

			this.hasOld = this.getOldValue(true, wasEntry);

			this.doAggregation();
			// NEW & OLD
			if (this.hasOld) {
				int counter = 0;
				for (int i = this.numberOfKeyColumns; i < this.arity; i++)
					this.oldTuple.columns[i] = this.aggregateValues[counter++];							
				
				((DerivedRelation)this.relation).update(this.oldTuple, this.oldTuple);	
			} else { // ONLY NEW
				for (int i = 0; i < this.numberOfKeyColumns; i++) 
					this.newTuple.columns[i] = this.getArgumentAsDbType(i);

				int counter = 0;
				for (int i = this.numberOfKeyColumns; i < this.arity; i++)
					this.newTuple.columns[i] = this.aggregateValues[counter++];

				((DerivedRelation)this.relation).add(this.newTuple, true);
			}
		}
		
		if (this.executionMode == ExecutionMode.Materialized) {
			this.isMaterialized = true;
			this.setMaterializedAllInstances();
		}
		
		return this.doRetrieveTuple(wasEntry);	
	}
	
	private Status doDeriveTupleAggregator() {
		// free aggregate variable
		//for (int i = this.numberOfKeyColumns; i < this.arity; i++)
		//	((Variable)this.getArgument(i)).makeFree();
		if (this.isEntry)
			this.freeVariableList.makeFree();

		AggregatorResult aggregatorResult = new AggregatorResult();
		boolean wasEntry = this.isEntry;
		this.isEntry = false;
		while (wasEntry && (this.getOrNodeTuple() == Status.SUCCESS)) {			
			for (int i = 0; i < this.numberOfKeyColumns; i++) 
				this.capturedTuple.columns[i] = this.getArgumentAsDbType(i);

			this.capturedTuple.columns[this.numberOfKeyColumns] 
					= this.getChild(0).getArgumentAsDbType(this.numberOfKeyColumns);

			((AggregateRelation) this.relation).put(this.capturedTuple, aggregatorResult);			
		}
		
		if (this.executionMode == ExecutionMode.Materialized) {
			this.isMaterialized = true;
			this.setMaterializedAllInstances();
		}
		
		return this.doRetrieveTuple(wasEntry);	
	}
	
	private void setMaterializedAllInstances() {
		if (this.otherInstances != null)
			for (StratifiedAggregateRelationNode instance : this.otherInstances) {
				instance.isMaterialized = true;
				instance.executionMode = ExecutionMode.Materialized;
			}
	}
	
	private Status doRetrieveTuple(boolean isEntry) {
		Status status = Status.FAIL;
		
		this.hasOld = this.getOldValue(isEntry, isEntry);
		if (this.hasOld) {
			this.freeVariableList.makeFree();
			if (this.setArgumentValues())
				status = Status.SUCCESS;

			// APS 9/10/2014
			// if pipelined execution, we remove tuple from relation once its completed
			// if materialized execution, we leave tuple in relation so it can be retrieved again later
			if (this.executionMode == ExecutionMode.Pipelined) {
				this.relation.remove(this.oldTuple);

				// we need to reset the cursor for bplustrees
				if (this.aggregateStoreType != AggregateStoreType.Heap)
					this.cursor.reset();
			}

			this.isEntry = false;
		} else {
			status = Status.FAIL;
			this.isEntry = true;
		}
		
		if (status == Status.FAIL)
			this.freeVariableList.makeFree();
		
		return status;
	}
	
	private void doAggregation() {
		if (!this.hasOld) {
			this.doAggregationNoOld();
			return;
		}
			
		for (int i = 0; i < this.aggregateInfos.length; i++) {
			DbTypeBase newValue = this.getChild(0).getArgumentAsDbType(this.numberOfKeyColumns + i);
			
			if (this.aggregateInfos[i].aggregateType == AggregateFunctionType.COUNT_DISTINCT) {
				DbSet set = (DbSet)this.oldTuple.getColumn(this.numberOfKeyColumns + i);
				set.put(newValue);
				this.aggregateValues[i] = set;
			} else {
				DbNumericType oldValue = (DbNumericType) this.oldTuple.getColumn(this.numberOfKeyColumns + i);
			
				switch (this.aggregateInfos[i].aggregateType) {
					case MAX:
						this.aggregateValues[i] = newValue.greaterThan(oldValue) ? newValue : oldValue;
						break;
					case MIN:
						this.aggregateValues[i] = newValue.lessThan(oldValue) ? newValue : oldValue;
						break;
					case COUNT:
						this.aggregateValues[i] = oldValue.add(DbInteger.create(1).convertTo(oldValue.getDataType()));
						break;
					case AVG:						
						this.aggregateValues[i] = ((DbAverage)oldValue).accrue((DbNumericType) newValue);
						break;
					case SUM:
						// the oldvalue will have the datatype from the relation, so cast to it
						this.aggregateValues[i] = this.expressions[i].evaluate(((DbNumericType) newValue).convertTo(oldValue.getDataType()), oldValue);						
						break;
				}
			}
		}
	}
	
	private void doAggregationNoOld() {
		DbTypeBase newTupleValue;
		for (int i = 0; i < this.aggregateInfos.length; i++) {
			newTupleValue = this.getChild(0).getArgumentAsDbType(i + this.numberOfKeyColumns);
			switch (this.aggregateInfos[i].aggregateType) {
				case MAX:
				case MIN:
					this.aggregateValues[i] = newTupleValue;
					break;
				case SUM:
					this.aggregateValues[i] = newTupleValue;
					break;
				case COUNT:
					this.aggregateValues[i] = this.typeManager.castToCountDataType(1);
					break;
				case AVG:
					this.aggregateValues[i] = DbAverage.create((DbNumericType) newTupleValue);
					break;
				case COUNT_DISTINCT:
					DbSet set = this.typeManager.createSet(newTupleValue.getDataType());							
					set.put(newTupleValue);
					this.aggregateValues[i] = set;
					break;
			}
		}
	}
	
	@Override
	public StratifiedAggregateRelationNode copy(ProgramContext programContext) {		
		StratifiedAggregateRelationNode copy = new StratifiedAggregateRelationNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList), 
				programContext.copyAggregateInfos(this.aggregateInfos),
				this.aggregateStoreType);
		
		copy.aggregateStoreType = this.aggregateStoreType;
		copy.executionMode = this.executionMode;
		copy.xyPredicateType = this.xyPredicateType;
		
		programContext.getNodeMapping().put(this, copy);
		
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
		
		return copy;
	}
}
