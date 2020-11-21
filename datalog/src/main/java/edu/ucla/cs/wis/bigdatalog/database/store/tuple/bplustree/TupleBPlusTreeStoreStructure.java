package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public interface TupleBPlusTreeStoreStructure<T> {
	
	public int getBytesPerKey();
	
	public int getBytesPerValue();
	
	public void insert(Tuple tuple);
	
	public void insert(DbTypeBase[] keyColumns, byte[] data);
	
	public T get(DbTypeBase[] keyColumns);

	public boolean delete(Tuple tuple);
	
	public void deleteAll();
	
	public BPlusTreeLeaf<?> getFirstChild();
	
	public int getNumberOfEntries();
	
	public MemoryMeasurement getSizeOf();
}