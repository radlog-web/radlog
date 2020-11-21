package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysfloatvalues;

import java.io.Serializable;

public class AggregatorBPlusTreeLongKeysFloatValuesGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public double value;
	
	public AggregatorBPlusTreeLongKeysFloatValuesGetResult(){}
}
