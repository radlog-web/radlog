package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

public interface BPlusTreeKeyIndex {

	public int getNodeSize();
	
	public int getHeight();

	public int getNumberOfEntries();
}
