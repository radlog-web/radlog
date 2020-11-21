package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public interface AggregatorBPlusTreeLongKeysDbTypeValuesPage extends BPlusTreeElement {
	
	public long getLeftMostLeafKey();
	
	public void get(long key, AggregatorBPlusTreeLongKeysDbTypeValuesGetResult result);
	
	public void insert(long key, DbTypeBase value, AggregatorBPlusTreeLongKeysDbTypeValuesInsertResult result);
	
	public boolean delete(long key);	
	
	public void get(long startKey, long endKey, AggregatorBPlusTreeLongKeysDbTypeValuesGetResultRange result);

}
