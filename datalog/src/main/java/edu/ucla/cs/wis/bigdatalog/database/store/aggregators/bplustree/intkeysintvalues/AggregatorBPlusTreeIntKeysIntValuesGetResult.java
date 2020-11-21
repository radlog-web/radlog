package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues;

import java.io.Serializable;

public class AggregatorBPlusTreeIntKeysIntValuesGetResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public int value;
	
	public AggregatorBPlusTreeIntKeysIntValuesGetResult(){}
}
