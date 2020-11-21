package edu.ucla.cs.wis.bigdatalog.database.relation;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.index.Index;
import edu.ucla.cs.wis.bigdatalog.database.index.IndexManager;
import edu.ucla.cs.wis.bigdatalog.database.index.IndexManager.IndexType;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree.BPlusTreeKeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStoreRO;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.exception.RelationManagerException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class Relation<T extends Tuple> 
	implements MemorySize, Serializable {

	private static final long serialVersionUID = 1L;
	public final boolean DEBUG = false;
	
	protected String					name;			// Relation name
	protected RelationType				relationType;	// Relation Type
	protected SecondaryIndex<?>[]		secondaryIndexes;	// list of indices for tuple retrieval
	
	protected TupleStoreBase<T>		tupleStore;		// where the data lives
	protected int						arity;		// we either have a schema or an arity - if only arity, we use ArrayListTupleStore 
	protected DataType[]				schema;		// schema in terms of RelationManager types for the relation
	protected Index<T> 				uniqueIndex;	// relations use a unique index to ensure set-oriented semantics
	protected Database					database;

	public Relation(String relationName, RelationType relationType, DataType[] schema, TupleStoreBase<T> tupleStore, 
			boolean addUniqueIndex, Database database) {
		this.name 						= relationName;
		this.relationType 				= relationType;
		this.secondaryIndexes			= new SecondaryIndex[0];
		this.schema						= schema;
		this.arity 						= schema.length;
		this.tupleStore					= tupleStore;
		this.database					= database;
		if (addUniqueIndex) {			
			int[] columnsForIndex = new int[this.getArity()];
			
			for (int i = 0; i < columnsForIndex.length; i++)
				columnsForIndex[i] = i;
			
			this.uniqueIndex = (Index<T>) this.database.getIndexManager().createKeyIndex(this, columnsForIndex);
			/*} else {
				if (this.tupleStore instanceof BPlusTreeTupleStore)	
					index = (Index<T>) IndexManager.createKeyIndex(this, columnsForIndex);
				else
					index = (Index<T>) IndexManager.createSecondaryIndex((Relation<AddressedTuple>) this, columnsForIndex);
			}
			this.uniqueIndex = index;*/
		}
	}
	
	public int getArity() {
		if (this.schema == null)
			return this.arity;
		return this.schema.length; 
	}
	
	public String getName() { return this.name; }
	
	public SecondaryIndex<?>[] getSecondaryIndexes() { 
		return this.secondaryIndexes; 
	}
	
	public DataType[] getSchema() { return this.schema; }
	
	public TupleStoreBase<T> getTupleStore() { return this.tupleStore; }

	public RelationType getRelationType() { return this.relationType; }
	
	public Database getDatabase() { return this.database; }
		
	public Index<T> getUniqueIndex() { return this.uniqueIndex; }
		
	public T add(T newTuple) {
		return add(newTuple, false);
	}
	
	public T add(T newTuple, boolean isUniqueTuple) {
		// If required, first check to see if the tuple is already present		
		if (isUniqueTuple || this.canAdd(newTuple)) {
			this.tupleStore.add(newTuple);

			for (int i = 0; i < this.secondaryIndexes.length; i++)
				this.secondaryIndexes[i].put((AddressedTuple) newTuple);

			return newTuple;
	    }
		return null;		
	}
	
	protected boolean canAdd(T tuple) {
		if (this.uniqueIndex == null)
			return true;
		
		return this.uniqueIndex.put(tuple);
	}
	
	protected T get(T tuple) {
		return this.tupleStore.exists(tuple);
	}
	
	public void remove(T tuple) {	
		T tupleToDelete = null;

		if (this.tupleStore instanceof AddressedTupleStore) {
			// case 1 - already has address
			if (((AddressedTuple)tuple).address > -1)
				tupleToDelete = tuple;

			// case 2 - get address
			// the tuple must be ground, so we can look it up - should already be checked per case 2 above
			if (tupleToDelete == null)
				tupleToDelete = this.get(tuple);
		
			if (tupleToDelete == null) {
				String message = "No tuple found to delete for relation '" + this.name + "' " + tuple.toString();
				throw new DatabaseException(message);
			}

			this.tupleStore.remove(tupleToDelete);
			((AddressedTuple)tupleToDelete).setDeleted();			
		} else {
			((BPlusTreeTupleStore)this.tupleStore).remove(tuple);
			tupleToDelete = tuple;
		}
		
		if (this.uniqueIndex != null)
			this.uniqueIndex.remove(tupleToDelete);
		
		for (SecondaryIndex<?> index : this.secondaryIndexes)
			index.remove((AddressedTuple) tupleToDelete);
	}
	
	public int commit() {
		int numberOfTuplesDeleted = 0;
		if (this.tupleStore != null) {
			numberOfTuplesDeleted = this.tupleStore.commit();
			// synchronize the indices with the relation - otherwise lots of problems
			this.rebuildIndexes();
		}
		return numberOfTuplesDeleted;
	}
	
	public SecondaryIndex<?> addSecondaryIndex(int[] columns) {
		return this.addSecondaryIndex(columns, IndexType.Default);
	}
	
	public SecondaryIndex<?> addSecondaryIndex(int[] columns, IndexManager.IndexType indexType) {
		return this.addSecondaryIndex(columns, indexType, true);
	}
	
	@SuppressWarnings("unchecked")
	public SecondaryIndex<?> addSecondaryIndex(int[] columns, IndexManager.IndexType indexType, boolean build) {
		if (columns.length == 0)
			return null;
		
		SecondaryIndex<?> secondaryIndex = this.getSecondaryIndex(columns);
		// only create index if there is not already an index on these columns
		if (secondaryIndex == null) {
			// only create new index if valid
			if (this.isValidIndexRequested(columns)) {
				secondaryIndex = this.database.getIndexManager().createSecondaryIndex((Relation<AddressedTuple>) this, 
						columns, indexType, build);
				this.addSecondaryIndex(secondaryIndex);			
			}
		}
		
		return secondaryIndex;
	}
	
	// APS 11/21/2013 - this bypasses the checks to keep indexes unique
	// we need this for the special bplustree indexes we use for sorting join order
	public void addSecondaryIndex(SecondaryIndex<?> secondaryIndex) {
		SecondaryIndex<?>[] temp = new SecondaryIndex[this.secondaryIndexes.length + 1];
		for (int i = 0; i < this.secondaryIndexes.length; i++)
			temp[i] = this.secondaryIndexes[i];
		temp[this.secondaryIndexes.length] = secondaryIndex;
		this.secondaryIndexes = temp;	
	}

	public SecondaryIndex<?> getSecondaryIndex(int[] columns) {
		if (this.uniqueIndex != null && this.uniqueIndex instanceof SecondaryIndex)
			if (IndexManager.isMatch(columns, ((SecondaryIndex<?>) this.uniqueIndex).getIndexedColumns()))
				return (SecondaryIndex<?>)this.uniqueIndex;
		
		for (SecondaryIndex<?> index : this.secondaryIndexes)
			if (IndexManager.isMatch(columns, index.getIndexedColumns()))
				return index;

		return null;
	}
		
	protected boolean isValidIndexRequested(int[] columns) { 
		if (columns == null || columns.length == 0)
			return false;
			
		if (this.getArity() < columns.length)
			throw new RelationManagerException("Too many columns requested for index for this relation's arity");
	    
		for (int i = 0; i < columns.length; i++) {
	    	if (columns[i] >= this.getArity())
	    		throw new RelationManagerException("Requested column is not among the indexed columns");
	    }
		
		// AddressedTupleStore required for index
		return (this.tupleStore instanceof AddressedTupleStore);
	}
	
	public void rebuildIndexes() {
		if (this.uniqueIndex != null) {
			this.uniqueIndex.clear();
			this.uniqueIndex.build();
		}
		
		for (SecondaryIndex<?> index : this.secondaryIndexes) {
			index.clear();
			index.build();
		}
	}
	
	public void cleanUp() {
		this.removeAllTuples();
		if (this.uniqueIndex != null)
			this.uniqueIndex.clear();
		
		if (this.secondaryIndexes != null)
			for (SecondaryIndex<?> index : this.secondaryIndexes)
				index.clear();
	}
	
	public void delete() {
		if (this.uniqueIndex != null)
			this.uniqueIndex.clear();
		
		this.uniqueIndex = null;
		this.deleteSecondaryIndexes();
		
		this.removeAllTuples();
		this.tupleStore = null;		
	}
	
	public void deleteSecondaryIndexes() {
		if (this.secondaryIndexes == null)
			return;
		
		for (int i = 0; i < this.secondaryIndexes.length; i++)
			this.secondaryIndexes[i].clear();

		this.secondaryIndexes = null;
	}
	
	public void deleteSecondaryIndex(SecondaryIndex<?> index) {
		if (this.secondaryIndexes == null)
			return;
		
		for (int i = 0; i < this.secondaryIndexes.length; i++) {
			if (index == this.secondaryIndexes[i]) {
				this.secondaryIndexes[i].clear();
				this.removeSecondaryIndex(index);
				break;
			}
		}
	}
	
	// we just want to take the index away from the relation, but not clear or destroy it
	public SecondaryIndex<?> removeSecondaryIndex(SecondaryIndex<?> index) {
		int i;
		SecondaryIndex<?> si = null;
		for (i = 0; i < this.secondaryIndexes.length; i++) {
			if (index == this.secondaryIndexes[i]) {
				si = this.secondaryIndexes[i];
				break;
			}
		}
		
		if (i == this.secondaryIndexes.length)
			return null;
		
		SecondaryIndex<?>[] temp = new SecondaryIndex[this.secondaryIndexes.length - 1];
		int counter = 0;
		for (int j = 0; j < this.secondaryIndexes.length; j++)
			if (j != i)
				temp[counter++] = this.secondaryIndexes[j];

		this.secondaryIndexes = temp;		

		return si;
	}
	
	public void removeAllTuples() {
		if (this.tupleStore != null) {
			this.tupleStore.removeAll();
			this.tupleStore.commit();
		}
	}
	
	public void sort() {
		// TupleUnorderedHeapStore will return true
		// BPlusTrees don't need sorting
		// we only want to rebuild the indexes if we have sorted
		if (this.tupleStore.sort())
			this.rebuildIndexes();		
	}
		
	// APS 11/29/2013
	// this method allows anything to be added - needed for query form, fs aggregate, and fs clique relations	
	public void removeUniqueIndex() {
		if (this.uniqueIndex != null) {
			this.database.getIndexManager().removeIndex(this, this.uniqueIndex);
			this.uniqueIndex.clear();
			this.uniqueIndex = null;
		}
	}
		
	public boolean isEmpty() { return (this.tupleStore.getNumberOfTuples() == 0); }
	
	public String toString() {
		StringBuilder retval = new StringBuilder();	  
		retval.append("Relation '");
		retval.append(this.name);
		retval.append("'");
		if (this.schema != null) {
			retval.append("{");
			//retval.append(this.schema.toString());
			retval.append("}");
		}
		retval.append(" [");
		retval.append(this.relationType.name().toLowerCase());
		retval.append("]");
		
		if (this.tupleStore == null) {
			retval.append("\nNo TupleStore associated with this relation.");
		} else {
			if (this.tupleStore.getNumberOfTuples() == 0) {
				retval.append("\n\tEMPTY");
			} else {
				//int counter = 0;
				Cursor cursor = this.database.getCursorManager().createScanCursor(this);
				Tuple tuple = cursor.getEmptyTuple();
								
				while (cursor.getTuple(tuple) > 0) {
					// limit to 200 facts
					//if (counter >= 200) {
					//	retval.append("\t\nStopping at 200...");
					//	return retval.toString();
					//}
					
					retval.append("\n\t");
					retval.append(tuple.toString());
					//counter++;
			    }
			}
		}
		return retval.toString();
	}
	
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		MemoryMeasurement sizes;
		if (this.tupleStore != null) {
			sizes = this.tupleStore.getSizeOf();
			used += sizes.getUsed();
			allocated += sizes.getAllocated();
		}
		
		if (this.uniqueIndex != null) {
			sizes = this.uniqueIndex.getSizeOf();
			used += sizes.getUsed();
			allocated += sizes.getAllocated();
		}
		
		if (this.secondaryIndexes != null) {
			for (SecondaryIndex<?> index : this.secondaryIndexes) {
				sizes = index.getSizeOf();
				used += sizes.getUsed();
				allocated += sizes.getAllocated();
			}
		}
		return new MemoryMeasurement(used, allocated);
	}
	
	public String toStringSizeOfDetailed() {
		StringBuilder output = new StringBuilder();

		if (this.tupleStore != null) {
			if (this.tupleStore instanceof TupleAggregationStore) {
				output.append("TupleStore["+this.tupleStore.getClass().getSimpleName()
						+ "<"+((TupleAggregationStore)this.tupleStore).storageStructure.getClass().getSimpleName()+">] "
				+ "[height: " + ((TupleAggregationStore)this.tupleStore).getHeight() 
				+ "|# entries:" + ((TupleAggregationStore)this.tupleStore).getNumberOfTuples() 
				+ "|node size:"  + ((TupleAggregationStore)this.tupleStore).getNodeSize()
				+ "]" + " = " + this.tupleStore.getSizeOf().toString());
				 /*if (((TupleAggregationStore) this.tupleStore).storageStructure instanceof AggregatorBPlusTreeStoreStructure) {
					 output.append("# of modified keys trees:" + 
							 ((AggregatorBPlusTreeStoreStructure<?,?,?>)((TupleAggregationStore)this.tupleStore).storageStructure).modifiedKeysTrees.size());
					 Map<Integer, BPlusTreeStoreStructure<?, ?, ?>> trees = ((AggregatorBPlusTreeStoreStructure<?,?,?>)((TupleAggregationStore)this.tupleStore).storageStructure).modifiedKeysTrees;
					 for (Map.Entry<Integer, BPlusTreeStoreStructure<?, ?, ?>> tree : trees.entrySet()) {
						 output.append(tree.getValue().getNumberOfEntries() + "\n");
					 }
				 }*/
				output.append("\n");
			} else if (this.tupleStore instanceof TupleBPlusTreeStoreRO) {
				output.append("TupleStore["+this.tupleStore.getClass().getSimpleName()
						+ "<"+((TupleBPlusTreeStoreRO)this.tupleStore).bPlusTree.getClass().getSimpleName()+">] "
				+ "[height: " + ((TupleBPlusTreeStoreRO)this.tupleStore).getHeight() 
				+ "|# entries:" + ((TupleBPlusTreeStoreRO)this.tupleStore).getNumberOfTuples() 
				+ "|node size:"  + ((TupleBPlusTreeStoreRO)this.tupleStore).getNodeSize()
				+ "]" + " = " + this.tupleStore.getSizeOf().toString());
				output.append("\n");
			} else {
				output.append("TupleStore["+this.tupleStore.getClass().getSimpleName()+"]"
						+ "[# entries:" + this.tupleStore.getNumberOfTuples()
						+ "] = " + this.tupleStore.getSizeOf().toString() + "\n");
			}
		}
				
		if (this.uniqueIndex != null) {
			if (this.uniqueIndex instanceof BPlusTreeKeyIndex) {
				output.append("Index["+this.uniqueIndex.getClass().getSimpleName()+"]" + Arrays.toString(((KeyIndex<?>)this.uniqueIndex).getKeyColumns()) 
						+ "[height: " + ((BPlusTreeKeyIndex)this.uniqueIndex).getHeight() 
						+ "|# entries:" + ((BPlusTreeKeyIndex)this.uniqueIndex).getNumberOfEntries() 
						+ "|node size:"  + ((BPlusTreeKeyIndex)this.uniqueIndex).getNodeSize()
						+ "]" + " = " +  this.uniqueIndex.getSizeOf().toString() + "\n");
			} else {
				output.append("UniqueIndex[" + this.uniqueIndex.getClass().getSimpleName() 
						+ "][# entries:" + this.uniqueIndex.getNumberOfEntries() + "] = " 
						+ this.uniqueIndex.getSizeOf().toString() + "\n");
			}
		}
				
		if (this.secondaryIndexes != null) {
			for (SecondaryIndex<?> index : this.secondaryIndexes) {
				if (index instanceof BPlusTreeSecondaryIndex) {
					output.append("Index["+index.getClass().getSimpleName()+"]" + Arrays.toString(index.getIndexedColumns()) 
							+ "[height: " + ((BPlusTreeSecondaryIndex<?>)index).getHeight() 
							+ "|# entries:" + ((BPlusTreeSecondaryIndex<?>)index).getSize() 
							+ "|node size:"  + ((BPlusTreeSecondaryIndex<?>)index).getNodeSize()
							+ "]" + " = " +  index.getSizeOf().toString() + "\n");
				} else {
					output.append("Index["+index.getClass().getSimpleName()
							+ "][# entries:" + index.getNumberOfEntries() 
							+"]" + Arrays.toString(index.getIndexedColumns()) + " = " +  index.getSizeOf().toString() + "\n");
				}
			}
		}
				
		return output.toString();
	}
	
	abstract public String toStringDetails();
	
	public T getEmptyTuple() { return (T) this.tupleStore.getEmptyTuple(); }
}
