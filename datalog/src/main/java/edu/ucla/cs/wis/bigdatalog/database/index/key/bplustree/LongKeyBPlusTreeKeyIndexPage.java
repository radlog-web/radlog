package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface LongKeyBPlusTreeKeyIndexPage extends BPlusTreeElement {
	public long getLeftMostLeafKey();
	
	public boolean get(long key);
	
	public void insert(long key, LongKeyBPlusTreeKeyIndexResult result);
	
	public boolean delete(long key);
}
