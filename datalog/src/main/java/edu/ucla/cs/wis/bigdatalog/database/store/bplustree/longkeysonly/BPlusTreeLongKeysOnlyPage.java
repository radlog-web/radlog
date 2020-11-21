package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeLongKeysOnlyPage extends BPlusTreeElement {
	public long getLeftMostLeafKey();
	
	public Long get(long key);
	
	public void insert(long key, BPlusTreeLongKeysOnlyInsertResult result);
	
	public boolean delete(long key);
}