package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface AggregatorBPlusTreeLongKeysIntValuesPage extends BPlusTreeElement {

	public long getLeftMostLeafKey();
	
	public void get(long key, AggregatorBPlusTreeLongKeysIntValuesGetResult result);
	
	public void insert(long key, int value, AggregatorBPlusTreeLongKeysIntValuesInsertResult result);
	
	public boolean delete(long key);
	
	public void get(long startKey, long endKey, AggregatorBPlusTreeLongKeysIntValuesGetResultRange result);

}
