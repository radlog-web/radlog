package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeMultiValueLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysheap.BPlusTreeByteKeysHeapValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// B-Tree tuple store
// hope first column is not a float
public class TupleBPlusTreeMultiStore 
	extends BPlusTreeTupleStore 
	implements Serializable {
	//private static Logger logger = LoggerFactory.getLogger(TupleBPlusTreeStore.class.getName());
	private static final long serialVersionUID = 1L;
		
	protected boolean							useOrderedHeap;
	public TupleBPlusTreeMultiStoreStructure 	storageStructure;
	
	public TupleBPlusTreeMultiStore() { super(); }
	
	public TupleBPlusTreeMultiStore(String relationName, DataType[] schema, TupleStoreConfiguration configuration, 
			int nodeSize, boolean useOrderedHeap, TypeManager typeManager) {
		super(relationName, schema, configuration.keyColumns, nodeSize, typeManager);
		this.useOrderedHeap = useOrderedHeap;
		//this.useOrderedHeap = Boolean.parseBoolean(DeALSContext.getConfiguration().getProperty("deals.database.tuplestores.bplustree.useorderedheap"));

		super.initialize();
				
		this.initializeStorageStructure(configuration);
	}
	
	protected void initializeStorageStructure(TupleStoreConfiguration configuration) {
		// store multiple values under a key w/ the keysheap tree
		this.storageStructure = new BPlusTreeByteKeysHeapValues(this.nodeSize, this.bytesPerKey, 
				this.bytesPerValue, this.keyColumns, this.keyColumnTypes, this.valueColumns, 
				this.valueColumnTypes, this.useOrderedHeap, this.typeManager);
	}
	
	@Override
	public void add(Tuple tuple) {
		this.storageStructure.insert(tuple);
	}
	
	@Override
	public void update(Tuple tuple) {
		this.storageStructure.delete(tuple);		
		this.add(tuple);
	}
	
	public Heap get(byte[] key) {
		return ((BPlusTreeByteKeysHeapValues)this.storageStructure).get(key);
	}	
	
	@Override
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		byte[] key = this.getKey(keyColumns);
		Heap heap = this.get(key);
		if (heap == null || heap.isEmpty())
			return 0;
		return this.loadTuple(key, heap.get(0), tuple);
	}	
	
	@Override
	public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor) {
		return 0;		
	}
	
	@Override
	public Tuple exists(Tuple tuple) {
		DbTypeBase[] key = new DbTypeBase[this.keyColumns.length];
		for (int i = 0; i < this.keyColumns.length; i++)
			key[i] = tuple.columns[this.keyColumns[i]];

		Heap heap = this.storageStructure.get(key);
		if (heap == null)
			return null;
		
		// if we're just checking keys, stop here
		if (tuple.getArity() == this.keyColumns.length) {
			if (this.loadTuple(key, heap.get(0), this.templateTuple) > 0)
				return this.templateTuple;
			return null;
		}
		
		for (int i = 0; i <= heap.getLastAddress(); i++) {
			this.loadTuple(key, heap.get(i), this.templateTuple);
			// no need to check key columns
			for (int j = 0; j < this.valueColumns.length; j++) {
				if (!tuple.columns[this.valueColumns[j]].equals(this.templateTuple.columns[this.valueColumns[j]])) {
					return null;
				}
			}
		}
		return this.templateTuple;//possibleMatch;
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
	public int commit() {
		return this.storageStructure.commit();
	}

	@Override
	public int getNumberOfTuples() {
		return this.storageStructure.getNumberOfEntries();
	}

	@Override
	public String toString() {		
		if (this.getNumberOfTuples() == 0)
			return "Relation is empty";
		
		StringBuilder retval = new StringBuilder();
		
		retval.append("bytesPerKey | bytesPerTuple : [" + this.bytesPerKey + " | " + this.bytesPerValue + "]");		
		retval.append("key columns : " + Arrays.toString(this.keyColumns));
		retval.append(this.storageStructure.toString());
		return retval.toString();
	}
	
	public BPlusTreeMultiValueLeaf<?> getFirstChild() {
		return (BPlusTreeMultiValueLeaf<?>) this.storageStructure.getFirstChild();
	}

	public MemoryMeasurement getSizeOf() {
		return this.storageStructure.getSizeOf();
	}

}
