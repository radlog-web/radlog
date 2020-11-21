package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface AggregatorBPlusTreeIntKeysIntValuesPage extends BPlusTreeElement {
 
	public int getLeftMostLeafKey();
	
	public void get(int key, AggregatorBPlusTreeIntKeysIntValuesGetResult result);
	
	public void insert(int key, int value, AggregatorBPlusTreeIntKeysIntValuesInsertResult result);
	
	public boolean delete(int key);

	public void get(int startKey, int endKey, AggregatorBPlusTreeIntKeysIntValuesGetResultRange result);
}
