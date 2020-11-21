package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysbytevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public interface BPlusTreeByteKeysByteValuesPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public byte[] get(byte[] key);
	
	public void insert(byte[] key, byte[] data, BPlusTreeByteKeysByteValuesInsertResult result, TypeManager typeManager);
	
	public boolean delete(byte[] key);

	public void deleteAll();
	
	public void get(byte[] startKey, byte[] endKey, BPlusTreeByteKeysByteValuesGetResultRange result);
}
