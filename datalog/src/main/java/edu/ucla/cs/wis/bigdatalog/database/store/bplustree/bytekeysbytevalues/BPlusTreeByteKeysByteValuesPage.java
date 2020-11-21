package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysbytevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeByteKeysByteValuesPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public byte[] get(byte[] key);
	
	public void insert(byte[] key, byte[] data, BPlusTreeByteKeysByteValuesInsertResult result);
	
	public boolean delete(byte[] key);

	public void deleteAll();
	
	public void get(byte[] startKey, byte[] endKey, BPlusTreeByteKeysByteValuesGetResultRange result);
}
