package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonlytwolevel;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public interface BPlusTreeLongKeysOnlyTwoLevelPage extends BPlusTreeElement {
	public int getLeftMostLeafKey();
	
	public void get(long key, BPlusTreeLongKeysOnlyTwoLevelGetResult result);
	
	public void insert(long key, BPlusTreeLongKeysOnlyTwoLevelInsertResult result, TypeManager typeManager);
	
	public boolean delete(long key);
}