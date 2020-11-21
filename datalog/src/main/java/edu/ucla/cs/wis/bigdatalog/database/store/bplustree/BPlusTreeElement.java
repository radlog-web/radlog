package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public interface BPlusTreeElement {
	
	public int getHeight();

	public boolean isEmpty();
	
	public boolean hasOverflow();
	
	public String toString(int indent);
	
	public String toStringShort();
	
	public MemoryMeasurement getSizeOf();
	
	public void deleteAll();
}
