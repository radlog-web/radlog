package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysbytevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeIntKeysByteValuesPage extends BPlusTreeElement {

	public int getLeftMostLeafKey();
	
	public byte[] get(int key);
	
	public void insert(int key, byte[] data, BPlusTreeIntKeysByteValuesInsertResult result);
	
	public boolean delete(int key);
	
	public void get(int startKey, int endKey, BPlusTreeIntKeysByteValuesGetResultRange result);
}
