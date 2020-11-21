package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypedbkeyvalues;

import java.io.Serializable;

public class AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesGetRangeResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesLeaf leaf; // leaf with first matching key to start of range 
	public int index; // index to first key within page matching start of range 
	
	public AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesGetRangeResult(){}
}
