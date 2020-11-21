package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysfloatvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface AggregatorBPlusTreeLongKeysFloatValuesPage extends BPlusTreeElement {
	public long getLeftMostLeafKey();
	
	public void get(long key, AggregatorBPlusTreeLongKeysFloatValuesGetResult result);
	
	public void insert(long key, double value, AggregatorBPlusTreeLongKeysFloatValuesInsertResult result);
	
	public boolean delete(long key);
	
	public void get(long startKey, long endKey, AggregatorBPlusTreeLongKeysFloatValuesGetResultRange result);

}
