package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public interface BPlusTree {
	
	public int getNumberOfEntries();
	
	public MemoryMeasurement getSizeOf();
	
	public String toString();
	
	public String toStringShort();
		
	public void deleteAll();

	public BPlusTreeLeaf<?> getFirstChild();
}
