package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypedbkeyvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public interface AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesPage extends BPlusTreeElement {	
	public long getLeftMostLeafKey();
	
	public void get(long key, AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesGetResult result);
	
	public void insert(long key, DbTypeBase subKey, DbNumericType value, 
			AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesResult result, TypeManager typeManager);
	
	public boolean delete(long key);	
	
	public void get(long startKey, long endKey, AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesGetRangeResult result);
}
