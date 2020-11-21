package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysbytevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeLongKeysByteValuesPage extends BPlusTreeElement {

	public long getLeftMostLeafKey();
	
	public byte[] get(long key);
	
	public void insert(long key, byte[] data, BPlusTreeLongKeysByteValuesInsertResult result);
	
	public boolean delete(long key);
	
	public void get(long startKey, long endKey, BPlusTreeLongKeysByteValuesGetResultRange result);
}
