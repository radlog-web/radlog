package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//B-Tree tuple store only single column relations
public class TupleBPlusTreeKeysOnlyStore 
	extends BPlusTreeTupleStore 
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public BPlusTreeByteKeysOnly bPlusTree;

	public TupleBPlusTreeKeysOnlyStore() { super(); }
	
	public TupleBPlusTreeKeysOnlyStore(String relationName, DataType[] schema, int nodeSize, 
			TypeManager typeManager) {
		super(relationName, schema, nodeSize, typeManager);

		this.initialize();
	}
	
	@Override
	protected void initialize() {	
		super.initialize();
				
		// no values, just keys
		this.bPlusTree = new BPlusTreeByteKeysOnly(this.nodeSize, this.bytesPerKey, this.schema/*, this.deALSConfiguration, this.typeManager*/);
	}
	
	@Override
	public void add(Tuple tuple) {
		byte[] key = this.getKey(tuple.columns);
		this.bPlusTree.insert(key);
	}

	@Override
	public void update(Tuple tuple) {
		byte[] key = this.getKey(tuple.columns);	
		this.bPlusTree.delete(key);		
		this.add(tuple);
	}

	public byte[] get(DbTypeBase[] keyColumns) {
		byte[] key = this.getKey(keyColumns);
		return this.bPlusTree.get(key);
	}
	
	public byte[] get(byte[] key) { 
		return this.bPlusTree.get(key);	
	}
	
	public int getTuple(byte[] key, Tuple tuple) {
		if (this.bPlusTree.get(key) == null)
			return 0;
		
		return this.loadTuple(key, tuple);
	}
	
	@Override
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		byte[] key = this.getKey(keyColumns);
		return this.getTuple(key, tuple);
	}
	
	@Override
	public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor) {
		return 0;
	}
	
	@Override
	public Tuple exists(Tuple tuple) {
		byte[] key = this.getKey(tuple.columns);
		if (this.bPlusTree.get(key) != null)
			return tuple;
		
		return null;
	}

	@Override
	public void remove(Tuple tuple) {		
		byte[] key = this.getKey(tuple.columns);
		this.bPlusTree.delete(key);
	}

	@Override
	public void removeAll() {
		this.bPlusTree.deleteAll();
	}

	@Override
	public int commit() {
		return 0;	// this bplustree does deletes on the fly
	}

	@Override
	public int getNumberOfTuples() {
		return this.bPlusTree.getNumberOfEntries();
	}
	
	@Override
	public String toString() {		
		if (this.getNumberOfTuples() == 0)
			return "Relation is empty";
		
		StringBuilder retval = new StringBuilder();
		
		retval.append("bytesPerKey : [" + this.bytesPerKey + "] ");		
		retval.append("data type of colum : " + this.schema);
		retval.append(this.bPlusTree.toString());
		return retval.toString();
	}
	
	public BPlusTreeLeaf<?> getFirstChild() {
		return this.bPlusTree.getFirstChild();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		return this.bPlusTree.getSizeOf();
	}
}
