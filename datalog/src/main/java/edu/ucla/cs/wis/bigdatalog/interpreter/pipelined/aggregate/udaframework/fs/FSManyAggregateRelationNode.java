package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs;

import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbComplex;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// This class is necessary for rules that have multiple fs aggregates
// It is not as optimized as the individual fscnt or fsmax classes
// 1 Heap tuplestore used
public class FSManyAggregateRelationNode 
	extends FSAggregateRelationNodeBase {	
	protected FSManyAggregateRelationNode 	counterpart;
	protected FSAggregateType[] 			aggregateTypes;
	protected VariableList 				aggregateValueVariables;
	
	public FSManyAggregateRelationNode(String predicateName, NodeArguments args, 
			Binding binding, VariableList freeVariables, boolean isRead, int numberOfAggregates, 
			AggregateStoreType aggregateStoreType) {
		super(predicateName, args, binding, freeVariables, isRead,  aggregateStoreType);
		this.numberOfAggregateColumns = numberOfAggregates;
	}
	
	@Override
	public boolean initialize() {
		/*READ & WRITE NEED THIS*/
		this.numberOfKeyColumns = this.arity - (this.numberOfAggregateColumns * 2);
		
		int[] keyColumns = new int[this.numberOfKeyColumns];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			keyColumns[i] = i;
		
		this.aggregateValueVariables = new VariableList(this.numberOfAggregateColumns);
		for (int i = 0; i < this.numberOfAggregateColumns; i++)
			this.aggregateValueVariables.set(i, (Variable)this.getArgument(this.numberOfKeyColumns + this.numberOfAggregateColumns + i));
				
		int fscntCounter = 0;
		this.aggregateTypes = new FSAggregateType[this.numberOfAggregateColumns];
		
		List<DataType> tempSchema = new LinkedList<>();
		for (int i = this.numberOfKeyColumns; i < (this.numberOfAggregateColumns + this.numberOfKeyColumns); i++) {
			if (this.getArgument(i) instanceof InterpreterFunctor) {
				InterpreterFunctor functor = (InterpreterFunctor)this.getArgument(i);
				tempSchema.add(functor.getArgument(1).getDataType());
				tempSchema.add(DataType.LIST);
				fscntCounter++;
				this.aggregateTypes[i - this.numberOfKeyColumns] = FSAggregateType.FSCNT;
			} else {
				tempSchema.add(this.getArgument(i).getDataType());
				this.aggregateTypes[i - this.numberOfKeyColumns] = FSAggregateType.FSMAX;
			}
		}
		
		int relationArity = this.numberOfKeyColumns + (fscntCounter * 2) + (numberOfAggregateColumns - fscntCounter);

		DataType[] schema = new DataType[relationArity];
		for (int i = 0; i < this.numberOfKeyColumns; i++)
			schema[i] = this.getArgument(i).getDataType(); 
			
		for (int i = 0; i < tempSchema.size(); i++)
			schema[i + this.numberOfKeyColumns] = tempSchema.get(i);

		this.relation = this.relationManager.createAggregateRelationHeap(this.getRelationName(), schema, keyColumns);
		this.newValue = new AddressedTuple(relationArity);
		
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
			
			this.cursor = this.database.getCursorManager().createCursor(this.relation, keyColumns);
			
			return this.getChild(0).initialize();
		}
		
		/*ONLY WRITE NEEDS THIS*/
		this.counterpart = (FSManyAggregateRelationNode) readAggregateNodes.pop();
		this.counterpart.counterpart = this;

		return true;
	}

	public FSAggregateType[] getFSAggregateTypes() { return this.aggregateTypes; }
	
	protected Status getReadTuple() {
		this.logTrace("Entering getReadTuple for {}", this);
		Status status = Status.FAIL;
		
		if (this.getOrNodeTuple() == Status.SUCCESS) {
			if (this.numberOfKeyColumns > 0) {
				DbTypeBase[] boundValues = new DbTypeBase[this.numberOfKeyColumns];
				for (int i = 0; i < this.numberOfKeyColumns; i++)
					boundValues[i] = this.getArgumentAsDbType(i);
				
				((SelectionCursor)this.cursor).reset(boundValues);
			} else {
				this.cursor.reset();
			}

			this.hasOld = (this.cursor.getTuple(this.oldValue) > 0);
			
			this.aggregateValueVariables.makeFree();
			
			if (this.oldValue != null) {
				this.hasOld = true;
				this.counterpart.currentLeaf = ((TupleUnorderedHeapStore)this.tupleStore).getPage(((AddressedTuple)this.oldValue).address);
				this.counterpart.currentTupleAddress = ((TupleUnorderedHeapStore)this.tupleStore).getAddressInPage(((AddressedTuple)this.oldValue).address);

				if (this.setArgumentValues(FSValueType.OldFS))
					status = Status.SUCCESS;
				
			} else {
				this.hasOld = false;
				this.counterpart.currentLeaf = null;
				this.counterpart.currentTupleAddress = -1;

				boolean isMatch = true;
				Argument argument;
				for (int i = 0; i < this.numberOfAggregateColumns && isMatch; i++) {
					argument = this.getArgument(this.numberOfKeyColumns + this.numberOfAggregateColumns + i);
					if (this.aggregateTypes[i] == FSAggregateType.FSCNT)
						isMatch = argument.match(this.typeManager.castToCountDataType(0));
					else if (this.aggregateTypes[i] == FSAggregateType.FSMAX)
						isMatch = argument.match(this.getMinValue());
					else if (this.aggregateTypes[i] == FSAggregateType.FSMIN)
						isMatch = argument.match(this.getMaxValue());
				}
				
				if (isMatch)
					status = Status.SUCCESS;
			}	
		} else {
			// if no new value, we fail
			status = Status.FAIL;
		}
		
		this.logTrace("Exiting getReadTuple with status = {}", status.getName());
		
		return status;
	}

	protected Status getWriteTuple() {
		Status status;

		// With one getReadTuple call, we might enter getWriteTuple several times,
		// depending on how many tuples are returned from multi rule
		if (this.isEntry) {
			// we're going to update the previous old value
			// if we reached here, we have a new max for the keys
			
			// fsmax aggregates require only 1 aggregate value column
			// fscnt aggregates requires 2 - 1 for the list of join keys, 1 for the total sum count
			if (this.counterpart.hasOld) {
				Tuple newTuple = this.counterpart.oldValue.copy();
				
				DbTypeBase dbTypeObject;
				DbTypeBase newMaxValue;
				int positionInTuple = this.numberOfKeyColumns;
				for (int i = 0; i < this.numberOfAggregateColumns; i++) {
					if (this.aggregateTypes[i] == FSAggregateType.FSCNT) {
						InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns + i);
						DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
						DbTypeBase value = func.getArgument(1).toDbType(this.typeManager);
						// insert/update key's value in list
						Pair<DbList, Long> updatedListAndNewCount = this.insertUpdateValue((DbList)newTuple.getColumn(positionInTuple + 1), key, value);

						newMaxValue = this.typeManager.castToCountDataType(updatedListAndNewCount.getSecond());
						newTuple.setColumn(positionInTuple++, newMaxValue);
						newTuple.setColumn(positionInTuple++, updatedListAndNewCount.getFirst());						
					} else {
						dbTypeObject = this.getArgumentAsDbType(this.numberOfKeyColumns + i);
						// update the value					
						newTuple.setColumn(positionInTuple++, dbTypeObject);
						newMaxValue = dbTypeObject;
					}
										
					this.getArgument(this.numberOfKeyColumns + this.numberOfAggregateColumns + i).match(newMaxValue);
				}
				((DerivedRelation)this.relation).update(this.counterpart.oldValue, newTuple);
				this.counterpart.hasOld = false;
			} else {				
				this.newValue = new AddressedTuple(this.newValue.columns.length);
				
				// write all bound columns to tuple
				DbTypeBase dbTypeObject;
				for (int i = 0; i < this.numberOfKeyColumns; i++) {
					dbTypeObject = this.getArgumentAsDbType(i);
					this.newValue.setColumn(i, dbTypeObject);
				}
				
				DbTypeBase newMaxValue;
				int positionInTuple = this.numberOfKeyColumns;
				for (int i = 0; i < this.numberOfAggregateColumns; i++) {
					if (this.aggregateTypes[i] == FSAggregateType.FSCNT) {
						InterpreterFunctor func = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns + i);
						DbTypeBase key = func.getArgument(0).toDbType(this.typeManager);
						DbTypeBase value = func.getArgument(1).toDbType(this.typeManager);
						
						// add list to hold
						DbList list = this.typeManager.createList(typeManager.createComplex("", new DbTypeBase[]{key, value}), null);
						this.newValue.setColumn(positionInTuple++, value);
						this.newValue.setColumn(positionInTuple++, list);						
						newMaxValue = value;
					} else {
						// get the multiValue
						dbTypeObject = this.getArgumentAsDbType(this.numberOfKeyColumns + i);
						// update the value					
						this.newValue.setColumn(positionInTuple++, dbTypeObject);
						newMaxValue = dbTypeObject;
					}
					
					this.getArgument(this.numberOfKeyColumns + this.numberOfAggregateColumns + i).match(newMaxValue);
				}
				this.relation.add(this.newValue, true);
			}
			
			this.isEntry = false;
			status = Status.SUCCESS;
		} else {
			status = Status.FAIL;
			this.cleanUp();
		}

		return status;
	}
	
	protected boolean setArgumentValues(FSValueType type) {
		Argument argument;
		boolean isMatch = true;
 
		switch (type) {
		case FS:
			for (int i = 0; i < this.numberOfAggregateColumns && isMatch; i++) {
				argument = this.getArgument(this.numberOfKeyColumns + (2 * this.numberOfAggregateColumns) + (2 *i) - 1);
				isMatch = argument.match(this.newValue.getColumn(this.numberOfKeyColumns + this.numberOfAggregateColumns + i));
			}
		break;
				
		case OldFS:
			// OLD_FS_VALUE_ID attributes follow Key columns, aggregate value columns
			int positionInTuple = this.numberOfKeyColumns;
			for (int i = 0; i < this.numberOfAggregateColumns && isMatch; i++) {
				argument = this.getArgument(this.numberOfKeyColumns + this.numberOfAggregateColumns + i);
				DbTypeBase dbType = null;
				if (this.aggregateTypes[i] == FSAggregateType.FSCNT) { 
					// the key for the join is in the fscnt<> aggregate argument
					InterpreterFunctor argumentWithKey = (InterpreterFunctor)this.getArgument(this.numberOfKeyColumns + i);
					DbTypeBase key = argumentWithKey.getArgument(0).toDbType(this.typeManager);
					
					DbTypeBase dbTypeTemp = this.oldValue.getColumn(positionInTuple+1);
					positionInTuple += 2;
					
					// dbTypeTemp is now a list of dbComplex - we need to search for the correct key
					DbList temp = (DbList)dbTypeTemp;
					
					while (temp != null && !temp.isEmpty()) {
						if (((DbComplex)temp.getHead()).getArgument(0).equals(key)) {
							dbType = ((DbComplex)temp.getHead()).getArgument(1); // DbInteger value
							break;
						}
						temp = temp.getTail();
					}
					
					if (dbType == null)
						dbType = this.typeManager.castToCountDataType(0);
				} else {
					dbType = this.oldValue.getColumn(positionInTuple++);
				}

				isMatch = argument.match(dbType);
			}
		break;
		}

		return isMatch;
	}
	
	private Pair<DbList, Long> insertUpdateValue(DbList list, DbTypeBase key, DbTypeBase value) {
		// for now, assume key and value are not complex objects
		
		// list has format [(a,1), (b, 1), (c,4)...]
		// list will be sorted
		DbList temp = list;
		DbComplex item;
		int counter = 0;
		int foundAt = -1;
		boolean insert = false;
		while (temp != null && !temp.isEmpty()) {
			item = (DbComplex)temp.getHead();
			// check for key and set value if found 
			if (item.getArgument(0).equals(key)) {
				//item.setArgument(1, value);
				foundAt = counter;
				break;
			} else if (item.getArgument(0).greaterThan(key)) {
				insert = true;
				// we won't find it in this list, so insert new key after first miss to keep order
				break;
			}
			temp = temp.getTail();
			counter++;
		}
		
		if (foundAt == -1) {
			if (insert)
				list = list.insertAt(counter, this.typeManager.createComplex("", new DbTypeBase[]{key, value}), this.database.getTypeManager());
			else
				list = list.append(this.typeManager.createComplex("", new DbTypeBase[]{key, value}), this.database.getTypeManager());
		} else {
			DbComplex oldComplex = (DbComplex)list.get(foundAt);
			DbTypeBase[] args = new DbTypeBase[oldComplex.getArity()];
			args[0] = oldComplex.getArgument(0);
			args[1] = value;
			list = list.overwriteAt(foundAt, this.typeManager.createComplex("", args), this.database.getTypeManager());
		}
		
		temp = list;		
		long count = 0;
		
		while (temp != null && !temp.isEmpty()) {
			item = (DbComplex)temp.getHead();
			count += ((DbLong)item.getArgument(1)).getValue();
			temp = temp.getTail();
			counter++;
		}		

		return new Pair<>(list, count);
	}
}
