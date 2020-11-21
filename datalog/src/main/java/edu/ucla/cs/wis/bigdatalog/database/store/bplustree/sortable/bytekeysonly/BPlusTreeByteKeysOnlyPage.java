package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysonly;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public interface BPlusTreeByteKeysOnlyPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public byte[] get(byte[] key);
	
	public void insert(byte[] key, BPlusTreeByteKeysOnlyInsertResult result, TypeManager typeManager);
	
	public boolean delete(byte[] key);
}
