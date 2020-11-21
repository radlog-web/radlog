package edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface BPlusTreeIntKeysBitmapValuesPage extends BPlusTreeElement {
	public int getLeftMostLeafKey();
	
	public void get(long key, BPlusTreeIntKeysBitmapValuesGetResult result);
	
	public void insert(long key, BPlusTreeIntKeysBitmapValuesInsertResult result);
	
	public boolean delete(long key);
}