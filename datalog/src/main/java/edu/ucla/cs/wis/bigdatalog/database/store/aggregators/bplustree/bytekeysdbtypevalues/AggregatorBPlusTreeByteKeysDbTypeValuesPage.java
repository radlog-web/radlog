package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public interface AggregatorBPlusTreeByteKeysDbTypeValuesPage extends BPlusTreeElement {	
	public byte[] getLeftMostLeafKey();
	
	public void get(byte[] key, AggregatorBPlusTreeByteKeysDbTypeValuesGetResult result);
	
	public void insert(byte[] key, DbTypeBase value, AggregatorBPlusTreeByteKeysDbTypeValuesInsertResult result);
	
	public boolean delete(byte[] key);	

	public void get(byte[] startKey, byte[] endKey, AggregatorBPlusTreeByteKeysDbTypeValuesGetResultRange result);

}
