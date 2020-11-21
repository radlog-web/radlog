package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface GeneralKeyBPlusTreeKeyIndexPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public boolean get(byte[] key);
	
	public void insert(byte[] key, GeneralKeyBPlusTreeKeyIndexResult result);
	
	public boolean delete(byte[] key);
}
