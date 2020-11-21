package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleRowPageLeaf;
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

// 2 Heap tuplestores used
public class FSCountDoubleAggregateRelationNode 
	extends FSAggregateRelationNodeBase {
	
	protected final boolean 		useDeltaMaintenance;
	protected FSCountDoubleAggregateRelationNode counterpart;
	protected int					numberOfJoinKeys;
	protected DerivedRelation		detailsRelation;
	protected IndexCursor			detailsRelationCursor;
	protected IndexCursor			detailsRelationCursor2;

	protected int					detailRelationValueOffset;
	protected int					primaryRelationValueOffset;
	protected AddressedTuple		mainOldValue;
	protected AddressedTuple		mainNewValue;
	protected DataType				valueDataType;

	public FSCountDoubleAggregateRelationNode(String predicateName, NodeArguments args, 
			Binding binding, VariableList freeVariables, boolean isRead, boolean useDeltaMaintenance, 
			AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, isRead, aggregateStoreType);
		this.useDeltaMaintenance = useDeltaMaintenance;
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

		int[] keyColumns = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;		

		DataType[] schema = this.determineSchemaMain(keyColumns);
		schema[this.numberOfKeyColumns] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;
		this.valueDataType = schema[this.numberOfKeyColumns];

		this.relation = this.relationManager.createAggregateRelationHeap(this.getRelationName(), schema, keyColumns);
		this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
		
		int[] detailsIndexedColumns = new int[this.numberOfKeyColumns + this.numberOfJoinKeys];

		for (int i = 0; i < (this.numberOfKeyColumns + this.numberOfJoinKeys); i++)
			detailsIndexedColumns[i] = i;

		String relationName = this.getRelationName();
		String[] parts = relationName.split("_");
		StringBuilder detailsRelationName = new StringBuilder();
		for (int i = 0; i < parts.length; i++) {
			if ((i + 1) == parts.length)
				detailsRelationName.append("2");

			if (i > 0)
				detailsRelationName.append("_");
			detailsRelationName.append(parts[i]);
		}

		// create a relation to store the aggregated summary results
		/* Example: if the mainRelation holds:
		 * (a, b, a, 1)
		 * (a, b, c, 1)
		 * 	then summaryRelation holds:
		 * (a, b, 2)
		 * 
		 * the intention is for faster access and less calculations */		
		int detailsRelationArity = this.numberOfKeyColumns + this.numberOfJoinKeys + 1;
		DataType[] detailsRelationSchema = new DataType[detailsRelationArity];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			detailsRelationSchema[i] = this.getArgument(i).getDataType();

		// functor will contain the aggregate details
		if (this.getArgument(this.numberOfKeyColumns) instanceof InterpreterFunctor) {
			InterpreterFunctor functor = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
			if (functor.getArgument(0) instanceof InterpreterFunctor)
				functor = (InterpreterFunctor)functor.getArgument(0);

			int position = 0;
			for (int i = this.numberOfKeyColumns; i < (this.numberOfKeyColumns + this.numberOfJoinKeys); i++)
				detailsRelationSchema[i] = functor.getArgument(position++).getDataType();			
		}

		detailsRelationSchema[detailsRelationArity - 1] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;

		this.detailsRelation = this.relationManager.createAggregateRelationHeap(this.getRelationName(), detailsRelationSchema, detailsIndexedColumns);

		if (!this.useDeltaMaintenance)
			this.detailsRelation.addSecondaryIndex(keyColumns);

		this.detailsRelationCursor = (IndexCursor)this.database.getCursorManager().createCursor(this.detailsRelation, detailsIndexedColumns);

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
		this.counterpart = (FSCountDoubleAggregateRelationNode)readAggregateNodes.pop();
		this.counterpart.counterpart = this;
		this.oldValue = this.detailsRelationCursor.getEmptyTuple();
		this.counterpart.oldValue = this.oldValue;
		this.newValue = this.detailsRelationCursor.getEmptyTuple();	
				
		this.mainNewValue = (AddressedTuple) this.relation.getEmptyTuple();
		this.mainOldValue = (AddressedTuple) this.relation.getEmptyTuple();
		this.counterpart.mainOldValue = this.mainOldValue;
		this.counterpart.mainNewValue = this.mainNewValue;

		if (!this.useDeltaMaintenance)
			this.detailsRelationCursor2 = (IndexCursor)this.database.getCursorManager().createCursor(this.detailsRelation, keyColumns);

		this.detailRelationValueOffset = 0;
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			this.detailRelationValueOffset += schema[i].getNumberOfBytes();
		
		this.primaryRelationValueOffset = this.detailRelationValueOffset;	
			for (int i = this.numberOfKeyColumns; i < (this.numberOfKeyColumns + this.numberOfJoinKeys); i++)
			this.detailRelationValueOffset += detailsRelationSchema[i].getNumberOfBytes();

		return true;
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

		if (this.detailsRelationCursor != null)
			this.detailsRelationCursor = null;
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

				this.detailsRelationCursor.reset(boundValues);
			}

			this.hasOld = (this.detailsRelationCursor.getTuple((AddressedTuple) this.oldValue) > 0);

			// if we're not in a recursive situation and we have already aggregated this value before,
			// we must clear the old max value - if we delete the detail value, it looks like a new one
			// and we re-aggregate for the previous total
			if (wasEntry && !this.isInClique()) {
				if (this.useDeltaMaintenance) {				
					if (this.hasOld) {
						// if we remove from the details relation, we must decrement the total amount by what we are deleting
						// otherwise, we just increase the delta
						this.detailsRelation.remove(this.oldValue);
						
						DbTypeBase value = null;
						// last column holds the multiplicity
						int multiplicityColumnPosition = this.relation.getArity() - 1;		
						DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
						
						for (int i = 0; i < this.numberOfKeyColumns; i++)
							boundValues[i] = this.getArgumentAsDbType(i);

						((SelectionCursor<AddressedTuple>)this.cursor).reset(boundValues);					
						
						if (this.cursor.getTuple(this.mainOldValue) > 0) {
							DbNumericType currentMultiplicity = (DbNumericType) this.mainOldValue.columns[multiplicityColumnPosition];
							value = currentMultiplicity.subtract((DbNumericType) this.oldValue.columns[this.numberOfKeyColumns + this.numberOfJoinKeys]);
							if (value.lessThanOrEqualsTo(DbInteger.create(0)))
								value = null;
							/*if (this.isResultInteger) {
								value = currentMultiplicity.subtract((DbNumericType) this.oldValue.columns[this.numberOfKeyColumns + this.numberOfJoinKeys]);
								if (value.lessThanOrEqualsTo(DbInteger.create(0)))
									value = null;
							} else {
								double newValue = currentMultiplicity.getFloatValue() 
										- this.oldValue.columns[this.numberOfKeyColumns + this.numberOfJoinKeys].getFloatValue();
								if (newValue > 0)
									value = DbDouble.create(newValue);
							}*/
							
							// if value is not null, it is greater than 0.  0 we delete 
							if (value != null) {
								for (int i = 0; i < this.mainNewValue.columns.length - 1; i++)
									this.mainNewValue.columns[i] = this.mainOldValue.columns[i];
								this.mainNewValue.columns[multiplicityColumnPosition] = value;
						
								((DerivedRelation)this.relation).update(this.mainOldValue, this.mainNewValue);
							} else {
								this.relation.remove(this.mainOldValue);
							}
						}
						this.hasOld = false;
					}
				} else {
					if (this.hasOld) {
						this.detailsRelation.remove(this.oldValue);
						this.hasOld = false;
					}
				}
			}

			this.aggregateValueVariable.makeFree();

			if (this.hasOld) {
				this.counterpart.currentLeaf = ((TupleUnorderedHeapStore)this.detailsRelation.getTupleStore()).getPage(((AddressedTuple)this.oldValue).address);
				this.counterpart.currentTupleAddress = ((TupleUnorderedHeapStore)this.detailsRelation.getTupleStore()).getAddressInPage(((AddressedTuple)this.oldValue).address);

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
			DbNumericType newAggregateMultiplicity = null;
			DbNumericType newMultiplicity = null;
			DbNumericType oldMultiplicity = null;
			if (this.counterpart.hasOld) {
				// we are going to update the previous old value from the detailsRelation
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				newMultiplicity = (DbNumericType) func.getArgument(1).toDbType(this.typeManager);				
				if (this.useDeltaMaintenance)
					oldMultiplicity = (DbNumericType) this.counterpart.oldValue.getColumn(this.numberOfKeyColumns + this.numberOfJoinKeys);

				newMultiplicity.getBytes(this.currentLeaf.getData().getData(), 
						(this.currentLeaf.getBytesPerTuple() * this.currentTupleAddress) + this.detailRelationValueOffset);

				this.counterpart.hasOld = false;
			} else { 
				for (int i = 0; i < this.numberOfKeyColumns; i++) 
					this.newValue.columns[i] = this.getArgumentAsDbType(i);
				
				// get the multiValue
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
				newMultiplicity = (DbNumericType) func.getArgument(1).toDbType(this.typeManager);
			
				this.newValue.columns[this.numberOfKeyColumns] = key;
				
				if (this.valueDataType != newMultiplicity.getDataType())
					newMultiplicity = newMultiplicity.convertTo(this.valueDataType);
				
				this.newValue.columns[this.numberOfKeyColumns + 1] = newMultiplicity;			
				
				this.detailsRelation.add(this.newValue, true);
			}

			if (this.useDeltaMaintenance)
				newAggregateMultiplicity = this.updateMultiplicityWithDeltaMaintenance(oldMultiplicity, newMultiplicity);
			else
				newAggregateMultiplicity = this.updateMultiplicity(newMultiplicity);
			
			this.aggregateValueVariable.setValue(newAggregateMultiplicity);

			this.isEntry = false;
			status = Status.SUCCESS;
		} else {
			status = Status.FAIL;
			this.cleanUp();
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
		// last column holds the multiplicity
		int multiplicityColumnPosition = this.relation.getArity() - 1;		
		DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];

		for (int i = 0; i < this.numberOfKeyColumns; i++)
			boundValues[i] = this.getArgumentAsDbType(i);

		if (this.cursor instanceof SelectionCursor)
			((SelectionCursor<AddressedTuple>)this.cursor).reset(boundValues);
		else
			this.cursor.reset();

		// if no match is found, we insert a new tuple as this value is a new key we haven't seen before
		if (this.cursor.getTuple(this.mainOldValue) == 0) {
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				this.mainNewValue.columns[i] = boundValues[i];

			this.mainNewValue.columns[multiplicityColumnPosition] = newMultiplicity;
			this.relation.add(this.mainNewValue, true);
			value = newMultiplicity;
			this.setLastTupleModified(true);
		} else {
			DbTypeBase currentMultiplicity = this.mainOldValue.columns[multiplicityColumnPosition];

			// special case - we have a summary relation value, but no details
			//   (page rank causes this problem)
			//   inserting the detail value is incorrect
			//   we can only increase the summary if the sum of the details increases
			//   it is usually ok, to just do delta maintenance, but not in these cases
			this.detailsRelationCursor2.reset(boundValues);

			int position = this.numberOfKeyColumns + this.numberOfJoinKeys;
			if (this.isResultInteger) {
				DbNumericType total = this.typeManager.castToCountDataType(0);				
				while (this.detailsRelationCursor2.getTuple((AddressedTuple) this.oldValue) > 0)
					total = total.add((DbNumericType) this.oldValue.getColumn(position));

				value = total;
			} else {
				double total = 0.0;
				while (this.detailsRelationCursor2.getTuple((AddressedTuple) this.oldValue) > 0)
					System.out.println("total less than current multiplicity. total: " + total + " current multiplicity: " + currentMultiplicity);

				value = DbDouble.create(total);
			}

			this.mainOldValue.columns[this.mainOldValue.columns.length - 1] = value;
			((DerivedRelation)this.relation).update(this.mainOldValue, this.mainOldValue);
			//((UpdateableCursor)this.cursor).update(this.mainOldValue);

			this.setLastTupleModified(false);
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
		// last column holds the multiplicity
		int multiplicityColumnPosition = this.relation.getArity() - 1;		
		DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
		
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			boundValues[i] = this.getArgumentAsDbType(i);

		if (this.cursor instanceof SelectionCursor)
			((SelectionCursor<AddressedTuple>)this.cursor).reset(boundValues);
		else
			this.cursor.reset();
				
		// if no match is found, we insert a new tuple as this value is a new key we haven't seen before
		if (this.cursor.getTuple(this.mainOldValue) == 0) {			
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				this.mainNewValue.columns[i] = boundValues[i];
			
			this.mainNewValue.columns[multiplicityColumnPosition] = newMultiplicity;
			this.relation.add(this.mainNewValue, true);
			value = newMultiplicity;
			this.setLastTupleModified(true);
		} else {
			DbNumericType currentMultiplicity = (DbNumericType) this.mainOldValue.columns[multiplicityColumnPosition];
			
			// APS 11/19/2013 - for when we have only fscnt aggregates in 
			// the recursion we can do delta maintenance - the idea is if we have a larger 
			// part of a whole, we have a larger whole 
			// /*special case - we have a summary relation value, but no details
			//   (page rank causes this problem)
			//   inserting the detail value is incorrect
			//   we can only increase the summary if the sum of the details increases
			//   it is usually ok, to just do delta maintenance, but not in these cases*/
			
			// get updated value
			//if (this.isResultInteger) {
			if (oldMultiplicity == null)
				value = currentMultiplicity.add(newMultiplicity);
			else
				value = currentMultiplicity.add(newMultiplicity.subtract(oldMultiplicity));
			/*} else {
				if (oldMultiplicity == null) {
					value = DbDouble.create(currentMultiplicity.getFloatValue() + newMultiplicity.getFloatValue());
				} else {
					value = DbDouble.create(currentMultiplicity.getFloatValue() 
						+ (newMultiplicity.getFloatValue() - oldMultiplicity.getFloatValue()));
				}
			}*/
			
			TupleRowPageLeaf leaf = ((TupleUnorderedHeapStore)this.relation.getTupleStore()).getPage(this.mainOldValue.address);
			int address = ((TupleUnorderedHeapStore)this.relation.getTupleStore()).getAddressInPage(this.mainOldValue.address);
			value.getBytes(leaf.getData().getData(), (leaf.getBytesPerTuple() * address) + this.primaryRelationValueOffset);

			this.setLastTupleModified(false);
		}
		
		return value;
	}
}
