package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.bytekeysbytevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeByteKeysByteValuesPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public void get(byte[] key, BPlusTreeByteKeysByteValuesGetResult result);
	
	public void insert(byte[] key, byte[] data, BPlusTreeByteKeysByteValuesInsertResult result);
	
	public boolean delete(byte[] key);

	public void deleteAll();
	
	public String toStringShort();
}
