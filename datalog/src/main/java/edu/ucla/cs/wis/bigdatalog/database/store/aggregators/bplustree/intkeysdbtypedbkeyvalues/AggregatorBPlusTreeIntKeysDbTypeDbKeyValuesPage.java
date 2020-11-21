package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public interface AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesPage extends BPlusTreeElement {	
	public int getLeftMostLeafKey();
	
	public void get(int key, AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResult result);
	
	public void insert(int key, DbTypeBase subKey, DbNumericType value, 
			AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesInsertResult result, TypeManager typeManager);
	
	public boolean delete(int key);	
	
	public void get(int startKey, int endKey, AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResultRange result);
}
