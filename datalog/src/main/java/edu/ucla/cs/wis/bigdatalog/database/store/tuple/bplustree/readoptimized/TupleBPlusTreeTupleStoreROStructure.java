package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public interface TupleBPlusTreeTupleStoreROStructure {
	
	public void insert(Tuple tuple);
	
	public Tuple[] get(DbTypeBase[] keyColumns);
		
	public boolean delete(Tuple tuple);
	
	public void deleteAll();
	
	public ROBPlusTreeTupleStoreLeaf getFirstChild();
	
	public int getNumberOfEntries();
	
	public int getHeight();
	
	public MemoryMeasurement getSizeOf();
}
