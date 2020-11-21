package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public interface AggregatorBPlusTreeIntKeysDbTypeValuesPage extends BPlusTreeElement {	
	public int getLeftMostLeafKey();
	
	public void get(int key, AggregatorBPlusTreeIntKeysDbTypeValuesGetResult result);
	
	public void insert(int key, DbTypeBase value, AggregatorBPlusTreeIntKeysDbTypeValuesInsertResult result);
	
	public boolean delete(int key);	

	public void get(int startKey, int endKey, AggregatorBPlusTreeIntKeysDbTypeValuesGetResultRange result);

}
