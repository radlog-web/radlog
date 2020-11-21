package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysfloatvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface AggregatorBPlusTreeIntKeysFloatValuesPage extends BPlusTreeElement {

	public int getLeftMostLeafKey();
	
	public void get(int key, AggregatorBPlusTreeIntKeysFloatValuesGetResult result);
	
	public void insert(int key, double value, AggregatorBPlusTreeIntKeysFloatValuesInsertResult result);
	
	public boolean delete(int key);
	
	public void get(int startKey, int endKey, AggregatorBPlusTreeIntKeysFloatValuesGetResultRange result);

}
