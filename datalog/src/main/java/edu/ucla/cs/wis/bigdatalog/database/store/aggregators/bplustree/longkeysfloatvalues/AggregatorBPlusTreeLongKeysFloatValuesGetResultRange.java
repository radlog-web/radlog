package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysfloatvalues;

import java.io.Serializable;

public class AggregatorBPlusTreeLongKeysFloatValuesGetResultRange implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public AggregatorBPlusTreeLongKeysFloatValuesLeaf leaf; // leaf with first matching key to start of range 
	public int index; // index to first key within page matching start of range
	
	public AggregatorBPlusTreeLongKeysFloatValuesGetResultRange(){}
}
