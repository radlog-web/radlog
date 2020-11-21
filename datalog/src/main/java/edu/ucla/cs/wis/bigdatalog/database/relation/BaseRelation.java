package edu.ucla.cs.wis.bigdatalog.database.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.IndexManager;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BaseRelation<T extends Tuple> extends Relation<T> {
	private static final long serialVersionUID = 1L;
	
	protected ArrayList<KeyIndex<T>> keyIndexes;		// list of keys for constraint checking

	public BaseRelation(String name, DataType[] schema, TupleStoreBase<T> tupleStore, Database database) {
		super(name, RelationType.BASE, schema, tupleStore, true, database);
		this.keyIndexes	 = new ArrayList<>();
	}
	
	@Override
	protected boolean canAdd(T tuple) {
		if (super.canAdd(tuple))
			return true;

		for (KeyIndex<T> keyIndex : this.keyIndexes)
			if (!keyIndex.put(tuple))
				return false;

		return true;
	}
	
	@Override
	public T add(T newTuple, boolean uniqueTuple) {
		T tuple = super.add(newTuple, uniqueTuple);
		if (tuple != null) {
			for (KeyIndex<T> keyIndex : this.keyIndexes)
				keyIndex.put(tuple);
		}
		
		return tuple;
	}
	
	@Override
	public void remove(T tuple) {
		super.remove(tuple);
		
		for (KeyIndex<T> keyIndex : this.keyIndexes)
			keyIndex.remove(tuple);
	}	
	
	@SuppressWarnings("unchecked")
	public KeyIndex<T> addKeyIndex(int[] columns) {
		if (columns == null || columns.length == 0)
			return null;
		
		KeyIndex<T> keyIndex = this.getKeyIndex(columns);
		// only create index if there is not already an index on these columns
		if (keyIndex == null) {
			if (this.isValidIndexRequested(columns)) {
				keyIndex = (KeyIndex<T>) this.database.getIndexManager().createKeyIndex(this, columns);
				if (keyIndex == null)
					throw new DatabaseException("Key index could not be created for relation: " + this.name + " on columns " + Arrays.toString(columns));
				
				this.keyIndexes.add(keyIndex);	
			}
		}
		
		return keyIndex;
	}
	
	public List<KeyIndex<T>> getKeyIndexes() { return this.keyIndexes; }

	public KeyIndex<T> getKeyIndex(int[] columns) {
		for (KeyIndex<T> index : this.keyIndexes)
			if (IndexManager.isMatch(columns, index.getKeyColumns()))
				return index;

		return null;
	}
	
	public void rebuildIndexes() {
		super.rebuildIndexes();
		
		for (KeyIndex<T> index : this.keyIndexes) {
			index.clear();
			index.build();
		}
	}
	
	public void cleanUp() {
		super.cleanUp();
		
		for (KeyIndex<T> index : this.keyIndexes)
			index.clear();
	}
	
	public void delete() {
		super.delete();
		
		for (int i = this.keyIndexes.size() - 1; i >= 0; i--) {
			this.keyIndexes.get(i).clear();
			this.keyIndexes.remove(i);
		}
	}

	public String toStringDetails() {
		StringBuilder retval = new StringBuilder();
		retval.append("  Base Relation: " + this.name + " ");
		retval.append(" | Tuple Store: " + this.getTupleStore().getClass().getSimpleName() + "]\n");
		
		if (this.uniqueIndex != null)
			retval.append("    " + this.uniqueIndex.getClass().getSimpleName() + " unique index on all columns\n");
		
		if (this.getSecondaryIndexes() != null) {
			for (SecondaryIndex<?> index : this.getSecondaryIndexes())
				retval.append("    " + index.getClass().getSimpleName() + " secondary index on columns: " + Arrays.toString(index.getIndexedColumns()) + "\n");				
		}
		
		if (this.getKeyIndexes() != null && this.getKeyIndexes().size() > 0) {
			for (KeyIndex<T> index : this.getKeyIndexes())
				retval.append("    " + index.getClass().getSimpleName() + " key index on columns: " + Arrays.toString(index.getKeyColumns()) + "\n");				
		}
		
		return retval.toString();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		MemoryMeasurement sizes = super.getSizeOf();
		int used = sizes.getUsed();
		int allocated = sizes.getAllocated();
		
		for (KeyIndex<T> keyIndex : this.keyIndexes) {
			sizes = keyIndex.getSizeOf();
			used += sizes.getUsed();
			allocated += sizes.getAllocated();
		}

		return new MemoryMeasurement(used, allocated);
	}
}