package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysbytevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLong;

public interface BPlusTreeIntKeysByteValuesPage extends BPlusTreeElement {
	public int getLeftMostLeafKey();
	
	public void get(int key, BPlusTreeIntKeysByteValuesGetResult result);
	
	public void insert(int key, byte[] value, BPlusTreeIntKeysByteValuesInsertResult result);
	
	public boolean delete(int key);
	
	public String toStringShort();
	
	public DbLongLong sumAllValues();
}
