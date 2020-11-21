package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public interface AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesPage extends BPlusTreeElement {	
	public byte[] getLeftMostLeafKey();
	
	public void get(byte[] key, AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResult result);
	
	public void insert(byte[] key, DbTypeBase subKey, DbNumericType value, 
			AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesInsertResult result, TypeManager typeManager);
	
	public boolean delete(byte[] key);	
	
	public void get(byte[] startKey, byte[] endKey, AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResultRange result);
}
