package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysdbtypevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public interface BPlusTreeIntKeysDbTypeValuesPage extends BPlusTreeElement {
	public int getLeftMostLeafKey();
	
	public void get(int key, BPlusTreeIntKeysDbTypeValuesGetResult result);
	
	public void insert(int key, DbTypeBase value, BPlusTreeIntKeysDbTypeValuesInsertResult result);
	
	public boolean delete(int key);
	
	public String toStringShort();
	
	//public DbLongLong sumAllValues();
}
