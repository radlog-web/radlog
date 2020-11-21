package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.intkeysonly;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeIntKeysOnlyPage extends BPlusTreeElement {
	public int getLeftMostLeafKey();
	
	public Integer get(int key);
	
	public void insert(int key, BPlusTreeIntKeysOnlyInsertResult result);
	
	public boolean delete(int key);

	public String toStringShort();
}
