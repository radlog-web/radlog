package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public interface TupleBPlusTreeTupleStoreStructure {
	
	public void insert(Tuple tuple);
	
	public ArrayList<Tuple> get(DbTypeBase[] keyColumns);
		
	public boolean delete(Tuple tuple);
	
	public void deleteAll();
	
	public BPlusTreeTupleStoreLeaf getFirstChild();
	
	public int getNumberOfEntries();
	
	public MemoryMeasurement getSizeOf();
}
