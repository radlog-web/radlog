package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.bytekeysonly;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeByteKeysOnlyPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public byte[] get(byte[] key);
	
	public void insert(byte[] key, BPlusTreeByteKeysOnlyInsertResult result);
	
	public boolean delete(byte[] key);

	public String toStringShort();
}
