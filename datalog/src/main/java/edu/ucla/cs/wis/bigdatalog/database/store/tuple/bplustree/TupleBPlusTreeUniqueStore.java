package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysbytevalues.BPlusTreeByteKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysbytevalues.BPlusTreeIntKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysbytevalues.BPlusTreeLongKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// B-Tree tuple store for relations with keys and values (i.e. not only keys) 
// hope first column is not a float
public class TupleBPlusTreeUniqueStore 
	extends BPlusTreeTupleStore 
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public	   TupleBPlusTreeStoreStructure<byte[]>		storageStructure;
	protected boolean									trackModifiedTuples;
	
	public TupleBPlusTreeUniqueStore() { super(); }
	
	public TupleBPlusTreeUniqueStore(String relationName, DataType[] schema, TupleStoreConfiguration configuration, int nodeSize, 
			TypeManager typeManager) {
		super(relationName, schema, configuration.keyColumns, nodeSize, typeManager);
		this.trackModifiedTuples = configuration.trackModifiedTuples;

		super.initialize();
		this.initializeStorageStructure(configuration);		
	}
	
	protected void initializeStorageStructure(TupleStoreConfiguration configuration) {
		// use 'best' bplustree
		if (this.bytesPerKey == 4)
			this.storageStructure = new BPlusTreeIntKeysByteValues(this.nodeSize, this.bytesPerValue, 
					this.keyColumns, this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.typeManager);
		else if (this.bytesPerKey == 8)
			this.storageStructure = new BPlusTreeLongKeysByteValues(this.nodeSize, this.bytesPerValue, 
					this.keyColumns, this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.typeManager);
		else
			this.storageStructure = new BPlusTreeByteKeysByteValues(this.nodeSize, this.bytesPerKey, this.bytesPerValue, 
					this.keyColumns, this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.typeManager);
	}
	
	@Override
	public void add(Tuple tuple) {
		this.storageStructure.insert(tuple);
	}
	
	@Override
	public void update(Tuple tuple) {
		// we're going to hope this tuple is already in the store keys must be the same
		this.storageStructure.insert(tuple);
	}
		
	public void update(DbTypeBase[] keyColumns, byte[] data) {
		this.storageStructure.insert(keyColumns, data);
	}
	
	@Override
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		byte[] value = this.storageStructure.get(keyColumns);
		if (value != null)
			return this.loadTuple(keyColumns, value, tuple);

		return 0;
	}
	
	public int getTuple(byte[] key, Tuple tuple) {
		byte[] value =  ((BPlusTreeByteKeysByteValues)this.storageStructure).get(key);
		if (value != null)
			return this.loadTuple(key, value, tuple);
	
		return 0;
	}
	
	public int getTuple(long key, Tuple tuple) {
		byte[] value = ((BPlusTreeLongKeysByteValues)this.storageStructure).get(key);
		if (value != null)
			return this.loadTuple(key, value, tuple);

		return 0;
	}
	
	public int getTuple(int key, Tuple tuple) {
		byte[] value = ((BPlusTreeIntKeysByteValues)this.storageStructure).get(key);
		if (value != null)
			return this.loadTuple(key, value, tuple);

		return 0;
	}
	
	@Override
	public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor) {
		RangeSearchResult result = new RangeSearchResult();
		//int bytesPerKey = ((RangeSearchableStorageStructure<?>)this.storageStructure).getBytesPerKey();
		switch (this.bytesPerKey) { 
			case 4:
				((RangeSearchableStorageStructure<Integer>)this.storageStructure).getTuple((Integer)searchKeys.startKey, (Integer)searchKeys.endKey, result);
				break;
			case 8:
				((RangeSearchableStorageStructure<Long>)this.storageStructure).getTuple((Long)searchKeys.startKey, (Long)searchKeys.endKey, result);
				break;				
			default:
				((RangeSearchableStorageStructure<byte[]>)this.storageStructure).getTuple((byte[])searchKeys.startKey, (byte[])searchKeys.endKey, result);
		}
		
		if (result.success) {
			cursor.initialize(result.leaf, result.index, searchKeys);
			return 1;
		}
		
		return 0;
	}
	
	@Override
	public void remove(Tuple tuple) {
		this.storageStructure.delete(tuple);
	}

	@Override
	public void removeAll() {
		this.storageStructure.deleteAll();
	}

	@Override
	public int commit() { return 0; }

	@Override
	public int getNumberOfTuples() {
		return this.storageStructure.getNumberOfEntries();
	}

	@Override
	public String toString() {		
		if (this.getNumberOfTuples() == 0)
			return "Relation is empty";
		
		StringBuilder output = new StringBuilder();
		output.append("bytesPerKey | bytesPerValue : [" + this.bytesPerKey + " | " + this.bytesPerValue + "]");		
		output.append("key columns : " + Arrays.toString(this.keyColumns));
		output.append(this.storageStructure.toString());
		return output.toString();
	}
	
	public BPlusTreeLeaf<?> getFirstChild() {
		return this.storageStructure.getFirstChild();
	}
	
	
	public MemoryMeasurement getSizeOf() {
		return this.storageStructure.getSizeOf();
	}
}
