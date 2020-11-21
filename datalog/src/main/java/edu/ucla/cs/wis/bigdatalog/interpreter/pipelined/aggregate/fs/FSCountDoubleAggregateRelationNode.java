package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// 2 Heap tuplestore used
public class FSCountDoubleAggregateRelationNode 
	extends FSAggregateRelationNode {

	protected final boolean 			useDeltaMaintenance;
	protected final boolean 			isMixedAggregate;
	protected int						numberOfJoinKeys;
	protected InterpreterFunctor 		aggregateTerm;
	protected int 						multiplicityColumnPosition;
	
	protected DbTypeBase[]				mainRelationSearchKey;
	protected DerivedRelation			detailsRelation;
	protected SelectionCursor<Tuple>	detailsRelationCursor;
	protected SelectionCursor<Tuple>	detailsRelationCursor2;
	protected DbTypeBase[]				detailsRelationSearchKey; 

	protected Tuple						mainOldValue;
	protected Tuple						mainNewValue;

	public FSCountDoubleAggregateRelationNode(String predicateName, NodeArguments args, Binding binding, 
			VariableList freeVariables, AggregateInfo[] fsAggregateInfos, AggregateStoreType aggregateStoreType,
			boolean useDeltaMaintenance, boolean isMixedAggregate) {
		super(predicateName, args, binding, freeVariables, fsAggregateInfos, aggregateStoreType);
		this.isMixedAggregate = isMixedAggregate;
		// we do not use deltamaintenance with mixed aggregates
		if (this.isMixedAggregate)
			this.useDeltaMaintenance = false;
		else
			this.useDeltaMaintenance = useDeltaMaintenance;		
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
		if (this.aggregateTerm.getArgument(0) instanceof InterpreterFunctor)
			this.numberOfJoinKeys = ((InterpreterFunctor)this.aggregateTerm.getArgument(0)).getArguments().size();
		else
			this.numberOfJoinKeys = 1;

		this.aggregateValueVariable = (Variable)this.getArgument(this.arity - 1);

		int[] keyColumns = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;		

		DataType[] schema = this.determineSchemaMain(keyColumns);
		schema[this.numberOfKeyColumns] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;

		String relationName = this.getPredicateName().substring(0, this.predicateName.lastIndexOf("_"));
		
		if (this.aggregateStoreType == AggregateStoreType.BPlusTree)
			this.relation = this.relationManager.createAggregateRelationBPlusTree(relationName, schema, keyColumns, this.isChangeTrackingStore());
		else
			this.relation = this.relationManager.createAggregateRelationHeap(relationName, schema, keyColumns, this.isChangeTrackingStore());
		
		this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);

		// initialize search key object
		this.mainRelationSearchKey = new DbTypeBase[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.mainRelationSearchKey[i] = DbTypeBase.loadFrom(schema[i], 0);
		
		this.multiplicityColumnPosition = this.relation.getArity() - 1;
		
		int[] detailsIndexedColumns = new int[this.numberOfKeyColumns + this.numberOfJoinKeys];
		for (int i = 0; i < (this.numberOfKeyColumns + this.numberOfJoinKeys); i++)
			detailsIndexedColumns[i] = i;

		// create a relation to store the aggregated summary results
		/* Example: if the detailRelation holds:
		 * (a, b, a, 1)
		 * (a, b, c, 1)
		 * 	then mainRelation holds:
		 * (a, b, 2) 
		 * the intent is for faster access and less computation */
		int detailsRelationArity = this.numberOfKeyColumns + this.numberOfJoinKeys + 1;
		DataType[] detailsRelationSchema = new DataType[detailsRelationArity];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			detailsRelationSchema[i] = this.getArgument(i).getDataType();

		// functor will contain the aggregate details
		if (this.aggregateTerm.getArgument(0) instanceof InterpreterFunctor)
			this.aggregateTerm = (InterpreterFunctor)this.aggregateTerm.getArgument(0);

		for (int i = 0; i < this.numberOfJoinKeys; i++)
			detailsRelationSchema[this.numberOfKeyColumns + i] = this.aggregateTerm.getArgument(i).getDataType();

		detailsRelationSchema[detailsRelationArity - 1] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;
		
		if (this.aggregateStoreType == AggregateStoreType.BPlusTree) {
			this.detailsRelation = this.relationManager.createAggregateRelationBPlusTree(relationName, detailsRelationSchema, detailsIndexedColumns);
			if (!this.useDeltaMaintenance)
				this.detailsRelationCursor2 = (SelectionCursor)this.database.getCursorManager().createCursor(this.detailsRelation, keyColumns);
		} else {
			this.detailsRelation = this.relationManager.createAggregateRelationHeap(relationName, detailsRelationSchema, detailsIndexedColumns);
			if (!this.useDeltaMaintenance) {
				this.detailsRelation.addSecondaryIndex(keyColumns);
				this.detailsRelationCursor2 = (SelectionCursor)this.database.getCursorManager().createCursor(this.detailsRelation, keyColumns);
			}
		}
		
		this.detailsRelationCursor = (SelectionCursor)this.database.getCursorManager().createCursor(this.detailsRelation, detailsIndexedColumns);
		
		// initialize details search key object
		this.detailsRelationSearchKey = new DbTypeBase[this.numberOfKeyColumns + this.numberOfJoinKeys];
		for (int i = 0; i < this.numberOfKeyColumns + this.numberOfJoinKeys; i++)
			this.detailsRelationSearchKey[i] = DbTypeBase.loadFrom(detailsRelationSchema[i], 0);
						
		// check if all keys are bound
		this.allKeyColumnsBound = true;
		for (int i = 0; i < this.numberOfKeyColumns; i++) {
			if (this.getBinding(i) == BindingType.FREE) {
				this.allKeyColumnsBound = false;
				break;
			}
		}
				
		this.oldTuple = this.detailsRelation.getEmptyTuple();
		this.newTuple = this.detailsRelation.getEmptyTuple();
		
		this.mainNewValue = this.relation.getEmptyTuple();
		this.mainOldValue = this.relation.getEmptyTuple();

		return this.getChild(0).initialize();
	}

	public void cleanUpData() {
		super.cleanUpData();

		if (this.detailsRelation != null) {
			this.detailsRelation.removeAllTuples();
			this.detailsRelation.commit();
		}
	}

	public void deleteRelationsAndCursors() {
		super.deleteRelationsAndCursors();
		if (this.detailsRelation != null) {
			this.detailsRelation.removeAllTuples();
			this.detailsRelation.commit();
			this.detailsRelation.deleteSecondaryIndexes();

			this.relationManager.deleteDerivedRelation(this.detailsRelation);
			this.detailsRelation = null;
		}

		this.detailsRelationCursor = null;		
		this.detailsRelationCursor2 = null;
	}

	@Override
	public Status doGetTuple() {
		Status status = Status.FAIL;
		boolean hasOld = false;
		int numberOfTuplesDerived = 0;
		
		// if we have a new value to aggregate, try to aggregate with the old value
		// if no old value, that is fine, we just move forward with nil
		while (this.getOrNodeTuple() == Status.SUCCESS) {
			numberOfTuplesDerived++;
			if (this.numberOfKeyColumns == 0) {
				this.detailsRelationCursor.reset(null);
			} else {
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.detailsRelationSearchKey[i] = this.getArgumentAsDbType(i);
				
				for (int i = 0; i < this.numberOfJoinKeys; i++)
					this.detailsRelationSearchKey[this.numberOfKeyColumns + i] = this.aggregateTerm.getArgument(i).toDbType(this.typeManager);
					
				this.detailsRelationCursor.reset(this.detailsRelationSearchKey);
			}
			
			hasOld = (this.detailsRelationCursor.getTuple(this.oldTuple) > 0);
			
			// if we're not in a recursive situation and we have already aggregated this value before,
			// we must clear the old max value - if we delete the detail value, it looks like a new one
			// and we re-aggregate for the previous total
			if (!this.isInClique()) {
				if (this.useDeltaMaintenance) {				
					if (hasOld) {
						this.detailsRelation.remove(this.oldTuple);
						
						DbTypeBase value = null;						
						for (int i = 0; i < this.numberOfKeyColumns; i++)
							this.mainRelationSearchKey[i] = this.getArgumentAsDbType(i);

						((SelectionCursor<?>)this.cursor).reset(this.mainRelationSearchKey);
						
						if (this.cursor.getTuple(this.mainOldValue) > 0) {
							DbNumericType currentMultiplicity = (DbNumericType) this.mainOldValue.columns[this.multiplicityColumnPosition];
							value = currentMultiplicity.subtract((DbNumericType) this.oldTuple.columns[this.numberOfKeyColumns + this.numberOfJoinKeys]);
							if (value.lessThanOrEqualsTo(DbInteger.create(0)))
								value = null;

							/*if (this.isResultInteger) {
								value = currentMultiplicity.subtract((DbNumericType) this.oldTuple.columns[this.numberOfKeyColumns + this.numberOfJoinKeys]);
								if (value.lessThanOrEqualsTo(this.typeManager.getIntegerNumber(0)))
									value = null;
							} else {
								double newValue = currentMultiplicity.getFloatValue() 
										- this.oldTuple.columns[this.numberOfKeyColumns + this.numberOfJoinKeys].getFloatValue();
								if (newValue > 0)
									value = DbDouble.create(newValue);
							}*/
							
							// if value is not null, it is greater than 0.  0 we delete 
							if (value != null) {
								this.mainOldValue.columns[this.multiplicityColumnPosition] = value;
								((DerivedRelation)this.relation).update(this.mainOldValue);
							} else {
								this.relation.remove(this.mainOldValue);
							}
						}
						hasOld = false;
					}
				} else {
					if (hasOld) {
						this.detailsRelation.remove(this.oldTuple);
						hasOld = false;
					}
				}
			}
			
			DbNumericType newMaxValue = null;
			DbNumericType oldMultiplicity = null;
			if (hasOld) {
				newMaxValue = (DbNumericType) this.aggregateTerm.getArgument(1).toDbType(this.typeManager);
				oldMultiplicity = (DbNumericType) this.oldTuple.getColumn(this.numberOfKeyColumns + this.numberOfJoinKeys).copy();
				if (!newMaxValue.greaterThan(oldMultiplicity))
					continue;
										
				this.oldTuple.columns[this.numberOfKeyColumns + this.numberOfJoinKeys] = newMaxValue;	
				this.detailsRelation.update(this.oldTuple);
				hasOld = false;
			} else {				
				for (int i = 0; i < this.numberOfKeyColumns; i++) 
					this.newTuple.columns[i] = this.getArgumentAsDbType(i);

				DbTypeBase key = this.aggregateTerm.getArgument(0).toDbType(this.typeManager);
				newMaxValue = (DbNumericType) this.aggregateTerm.getArgument(1).toDbType(this.typeManager);
				
				this.newTuple.columns[this.numberOfKeyColumns] = key;
				this.newTuple.columns[this.numberOfKeyColumns + 1] = newMaxValue;
	
				this.detailsRelation.add(this.newTuple, true);
			}

			DbNumericType newAggregateMultiplicity = null;
			if (this.isMixedAggregate) {
				newAggregateMultiplicity = this.updateMultiplicityMixedAggregates(oldMultiplicity, newMaxValue);
			} else {
				if (this.useDeltaMaintenance)
					newAggregateMultiplicity = this.updateMultiplicityWithDeltaMaintenance(oldMultiplicity, newMaxValue);
				else
					newAggregateMultiplicity = this.updateMultiplicity(newMaxValue);
			}
			
			if (newAggregateMultiplicity == null)
				continue;
			
			this.aggregateValueVariable.setValue(newAggregateMultiplicity);
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
	
	private DbNumericType updateMultiplicity(DbNumericType newMultiplicity) {
		// PROCESS:
		// 1) Search for tuple in summaryRelation.
		// 2) if not in summaryRelation, then insert new tuple into summaryRelation with 'multiplicity'
		//	  else in summaryRelation, then update existing tuple from summaryRelation by aggregating all detail tuples 
		// 3) return value
		DbNumericType value = null;
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.mainRelationSearchKey[i] = this.getArgumentAsDbType(i);

		if (this.cursor instanceof SelectionCursor)
			((SelectionCursor<?>)this.cursor).reset(this.mainRelationSearchKey);
		else
			this.cursor.reset();

		// if no match is found, we insert a new tuple as this value is a new key we haven't seen before
		if (this.cursor.getTuple(this.mainOldValue) == 0) {
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				this.mainNewValue.columns[i] = this.mainRelationSearchKey[i];

			this.mainNewValue.columns[this.multiplicityColumnPosition] = newMultiplicity;
			this.relation.add(this.mainNewValue, true);
			value = newMultiplicity;
			this.setNewOrUpdatedTuple(true);
		} else {
			// special case - we have a summary relation value, but no details
			//   (page rank causes this problem)
			//   inserting the detail value is incorrect
			//   we can only increase the summary if the sum of the details increases
			//   it is usually ok, to just do delta maintenance, but not in these cases
			this.detailsRelationCursor2.reset(this.mainRelationSearchKey);

			int position = this.numberOfKeyColumns + this.numberOfJoinKeys;
			if (this.isResultInteger)
				value = this.typeManager.castToCountDataType(0);
			else 
				value = DbInteger.create(0).convertTo(this.oldTuple.getColumn(0).getDataType());
			
			while (this.detailsRelationCursor2.getTuple(this.oldTuple) > 0)
				value = value.add((DbNumericType) this.oldTuple.getColumn(position));

			/*} else {
				total = DbFloat.create(0);
				while (this.detailsRelationCursor2.getTuple(this.oldTuple) > 0)
					total += this.oldTuple.getColumn(position).getFloatValue();

				value = DbDouble.create(total);
			}*/
			
			// we still need new multiplicity to exceed the previous
			//if (!value.greaterThan(currentMultiplicity))
			//	return null;

			this.mainOldValue.columns[this.multiplicityColumnPosition] = value;
			((DerivedRelation)this.relation).update(this.mainOldValue);
			this.setNewOrUpdatedTuple(false);
		}							
		return value;
	}
	
	private DbNumericType updateMultiplicityWithDeltaMaintenance(DbNumericType oldMultiplicity, DbNumericType newMultiplicity) {
		// PROCESS:
		// 1) Search for tuple in summaryRelation.
		// 2) if not in summaryRelation, then insert new tuple into summaryRelation with 'multiplicity'
		//	  else in summaryRelation, then update existing tuple from summaryRelation by incrementing count by 'multiplicity'
		// 3) return value
		DbNumericType value = null;				
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.mainRelationSearchKey[i] = this.getArgumentAsDbType(i);

		if (this.cursor instanceof SelectionCursor)
			((SelectionCursor<?>)this.cursor).reset(this.mainRelationSearchKey);
		else
			this.cursor.reset();
		
		// if no match is found, we insert a new tuple as this value is a new key we haven't seen before
		if (this.cursor.getTuple(this.mainOldValue) == 0) {
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				this.mainNewValue.columns[i] = this.mainRelationSearchKey[i];
			
			this.mainNewValue.columns[this.multiplicityColumnPosition] = newMultiplicity;
			this.relation.add(this.mainNewValue, true);
			value = newMultiplicity;

			this.setNewOrUpdatedTuple(true);
		} else {
			DbNumericType currentMultiplicity = (DbNumericType) this.mainOldValue.columns[this.multiplicityColumnPosition];
			
			// get updated value
			if (oldMultiplicity == null)
				value = currentMultiplicity.add(newMultiplicity);
			else
				value = currentMultiplicity.add(newMultiplicity.subtract(oldMultiplicity));				
			
			this.mainOldValue.columns[this.multiplicityColumnPosition] = value;
			((DerivedRelation)this.relation).update(this.mainOldValue);			
			this.setNewOrUpdatedTuple(false);			
		}

		return value;
	}
	
	private DbNumericType updateMultiplicityMixedAggregates(DbNumericType oldMultiplicity, DbNumericType newMultiplicity) {
		// PROCESS:
		// 1) Search for tuple in summaryRelation.
		// 2) if not in summaryRelation, then insert new tuple into summaryRelation with 'multiplicity'
		//	  else in summaryRelation, then update existing tuple from summaryRelation by incrementing count by 'multiplicity'
		// 3) return value
		DbNumericType value = null;				
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.mainRelationSearchKey[i] = this.getArgumentAsDbType(i);

		if (this.cursor instanceof SelectionCursor)
			((SelectionCursor<?>)this.cursor).reset(this.mainRelationSearchKey);
		else
			this.cursor.reset();
		
		// if no match is found, we insert a new tuple as this value is a new key we haven't seen before
		if (this.cursor.getTuple(this.mainOldValue) == 0) {
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				this.mainNewValue.setColumn(i, this.mainRelationSearchKey[i]);
			
			this.mainNewValue.setColumn(multiplicityColumnPosition, newMultiplicity);
			this.relation.add(this.mainNewValue, true);
			value = newMultiplicity;
			
			this.setNewOrUpdatedTuple(true);
		} else {
			DbNumericType currentMultiplicity = (DbNumericType) this.mainOldValue.columns[this.multiplicityColumnPosition];
					
			this.detailsRelationCursor2.reset(this.mainRelationSearchKey);
			
			int position = this.numberOfKeyColumns + this.numberOfJoinKeys;
			boolean isNewAggregateMaxValue = false;
			
			if (this.isResultInteger)
				value = this.typeManager.castToCountDataType(0);
			else
				value = DbInteger.create(0).convertTo(this.oldTuple.getColumn(position).getDataType());
			
			while (this.detailsRelationCursor2.getTuple(this.oldTuple) > 0)
				value = value.add((DbNumericType) this.oldTuple.getColumn(position));
				
			if (value.greaterThan(currentMultiplicity)) 
				isNewAggregateMaxValue = true;
			
			// by now we've checked if we have an new aggregate max value by summing the detail records
			// if so, we add the delta of the detail (if there was an old value) to the aggregated value to get the new max
			// this might or might not be the total of the summed detail records
			// otherwise, we do not have a new max and cannot update the main relation
			if (isNewAggregateMaxValue) {
				// get updated value
				if (oldMultiplicity == null)
					value = currentMultiplicity.add(newMultiplicity);
				else
					value = currentMultiplicity.add(newMultiplicity.subtract(oldMultiplicity));
					
				this.mainOldValue.columns[this.multiplicityColumnPosition] = value;
				((DerivedRelation)this.relation).update(this.mainOldValue);
				this.setNewOrUpdatedTuple(false);	
			}
		}							
		return value;
	}
	
	@Override
	public FSCountDoubleAggregateRelationNode copy(ProgramContext programContext) {
		FSCountDoubleAggregateRelationNode copy = new FSCountDoubleAggregateRelationNode(new String(this.predicateName), 
				programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList),
				programContext.copyAggregateInfos(this.aggregateInfos), 
				this.aggregateStoreType, 
				this.useDeltaMaintenance, 
				this.isMixedAggregate);
		
		copy.executionMode = this.executionMode;
		copy.clique = (CliqueNode) programContext.getCliqueMapping().get(this.clique);
		
		programContext.getNodeMapping().put(this, copy);
				
		for (int i = 0; i < this.getNumberOfChildren(); i++)
			copy.addChild(this.getChild(i).copy(programContext));
		
		return copy;
	}
}
