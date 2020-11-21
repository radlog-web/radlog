package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.IndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleRowPageLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
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

/* SPECIAL CLASS FOR MIXED (FSMAX, THEN FSCNT) FS AGGREGATES
 * use case:
 *  - we have a summary relation value, but no details (page rank causes this problem.
 *  - inserting the detail value from fsmax is incorrect.
 *  - we can only safely increase the summary value if the sum of the details increases
 *  - in this case, delta maintenance is not possible
 *  2 heap tuplestores used
 */
public class FSCountMixedAggregateRelationNode 
	extends FSAggregateRelationNodeBase {
	
	protected FSCountMixedAggregateRelationNode counterpart;
	protected int					numberOfJoinKeys;
	protected DerivedRelation detailsRelation;
	protected IndexCursor			detailsRelationCursor;
	protected IndexCursor			detailsRelationCursor2;
	
	protected int					detailRelationValueOffset;
	protected int					primaryRelationValueOffset;
	protected AddressedTuple		mainOldValue;
	protected AddressedTuple		mainNewValue;
	
	public FSCountMixedAggregateRelationNode(String predicateName, NodeArguments args, 
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
		
		int[] keyColumns = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;	
		
		DataType[] schema = this.determineSchemaMain(keyColumns);
		schema[this.numberOfKeyColumns] = this.isResultInteger ? this.deALSContext.getConfiguration().getCountDataType() : DataType.DOUBLE;
		
		this.relation = this.relationManager.createAggregateRelationHeap(this.getRelationName(), schema, keyColumns);

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
		/* Example: if the detailRelation holds:
		 * (a, b, a, 1)
		 * (a, b, c, 1)
		 * 	then mainRelation holds:
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
		this.counterpart = (FSCountMixedAggregateRelationNode)readAggregateNodes.pop();
		this.counterpart.counterpart = this;
		this.oldValue = this.detailsRelationCursor.getEmptyTuple();
		this.counterpart.oldValue = this.oldValue;
		this.newValue = this.detailsRelationCursor.getEmptyTuple();
		
		this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
		this.counterpart.cursor = this.cursor;
		
		this.mainNewValue = (AddressedTuple) this.relation.getEmptyTuple();
		this.mainOldValue = (AddressedTuple) this.relation.getEmptyTuple();
		this.counterpart.mainOldValue = this.mainOldValue;
		this.counterpart.mainNewValue = this.mainNewValue;
				
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
		
		this.detailsRelationCursor = null;
	}

	private void unsetAggregateVariables() {
		for (int i = (this.numberOfKeyColumns + 1); i < this.arity; i++)
			((Variable)this.getArgument(i)).makeFree();		
	}
	
	protected Status getReadTuple() {
		Status status = Status.FAIL;
		
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
									
			this.unsetAggregateVariables();
						
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
			if (this.counterpart.hasOld) {
				// we are going to update the previous old value from the detailsRelation
				// get the multiValue from the functor
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbNumericType newMultiplicity = (DbNumericType) func.getArgument(1).toDbType(this.typeManager);
				//if (this.isResultInteger && !newMultiplicity.isLongLong())
				//	newMultiplicity = DbLongLong.create(newMultiplicity.getLongValue());
				DbNumericType oldMultiplicity = (DbNumericType) this.counterpart.oldValue.getColumn(this.numberOfKeyColumns + this.numberOfJoinKeys); 

				newMultiplicity.getBytes(this.currentLeaf.getData().getData(), (this.currentLeaf.getBytesPerTuple() * this.currentTupleAddress) + this.detailRelationValueOffset);
				
				newAggregateMultiplicity = this.updateMultiplicity(oldMultiplicity, newMultiplicity);
				this.counterpart.hasOld = false;
			} else { 
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					this.newValue.columns[i] = this.getArgumentAsDbType(i);
				
				InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns);
				DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
				DbNumericType value = (DbNumericType) func.getArgument(1).toDbType(this.typeManager);
				
				this.newValue.columns[this.numberOfKeyColumns] = key;				
				this.newValue.columns[this.numberOfKeyColumns + 1] = value;
				
				this.detailsRelation.add(this.newValue, true);
	
				newAggregateMultiplicity = this.updateMultiplicity(null, value);
			}

			this.aggregateValueVariable.setValue(newAggregateMultiplicity);
			this.isEntry = false;
			status = Status.SUCCESS;
		} else {
			status = Status.FAIL;
			this.cleanUp();
		}
		
		return status;
	}
	
	private DbNumericType updateMultiplicity(DbNumericType oldMultiplicity, DbNumericType newMultiplicity) {
		// PROCESS:
		// 1) Search for tuple in summaryRelation.
		// 2) if not in summaryRelation, then insert new tuple into summaryRelation with 'multiplicity'
		//	  else in summaryRelation, then update existing tuple from summaryRelation by incrementing count by 'multiplicity'
		// 3) return value
		//int value = 0;
		DbNumericType value;
		// last column holds the multiplicity
		int multiplicityColumnPosition = this.relation.getArity() - 1;		
		DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
		
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			boundValues[i] = this.getArgumentAsDbType(i);

		if (this.cursor instanceof SelectionCursor)
			((SelectionCursor<?>)this.cursor).reset(boundValues);
		else
			this.cursor.reset();
		
		AddressedTuple tuple = (AddressedTuple) this.cursor.getEmptyTuple();
		
		// if no match is found, we insert a new tuple as this value is a new key we haven't seen before
		if (this.cursor.getTuple(tuple) == 0) {
			AddressedTuple newTuple = new AddressedTuple(this.relation.getArity());
			for (int i = 0; i < this.numberOfKeyColumns; i++)
				newTuple.setColumn(i, boundValues[i]);
			
			newTuple.setColumn(multiplicityColumnPosition, newMultiplicity);
			this.relation.add(newTuple, true);
			value = newMultiplicity;
			
			this.setLastTupleModified(true);
		} else {
			DbNumericType currentMultiplicity = (DbNumericType) tuple.columns[multiplicityColumnPosition];
					
			this.detailsRelationCursor2.reset(boundValues);
			
			AddressedTuple detailTuple = this.detailsRelationCursor2.getEmptyTuple();
			int position = this.numberOfKeyColumns + this.numberOfJoinKeys;
			boolean isNewAggregateMaxValue = false;
			
			if (this.isResultInteger)
				value = this.typeManager.castToCountDataType(0);
			else
				value = DbInteger.create(0).convertTo(detailTuple.getColumn(0).getDataType());
			
			while (this.detailsRelationCursor2.getTuple(detailTuple) > 0)
				value = value.add((DbNumericType) detailTuple.getColumn(position));
				
			if (value.greaterThan(currentMultiplicity)) 
				isNewAggregateMaxValue = true;
			
			// ok, so by now we've checked if we have an new aggregate max value by summing the detail records
			// if so, we add the delta of the detail (if there was an old value) to the aggregated value to get the new max
			// this might or might not be the total of the summed detail records
			if (isNewAggregateMaxValue) {
				// get updated value
				if (oldMultiplicity == null)
					value = currentMultiplicity.add(newMultiplicity);
				else
					value = currentMultiplicity.add(newMultiplicity.subtract(oldMultiplicity));				
				
				TupleRowPageLeaf leaf = ((TupleUnorderedHeapStore)this.relation.getTupleStore()).getPage(tuple.address);
				int address = ((TupleUnorderedHeapStore)this.relation.getTupleStore()).getAddressInPage(tuple.address);
				value.getBytes(leaf.getData().getData(), (leaf.getBytesPerTuple() * address) + this.primaryRelationValueOffset);

				this.setLastTupleModified(false);
			} else {
				// we do not have a greater value, so we cannot update the summary relation
				value = null;
			}
		}							
		return value;
	}
}
