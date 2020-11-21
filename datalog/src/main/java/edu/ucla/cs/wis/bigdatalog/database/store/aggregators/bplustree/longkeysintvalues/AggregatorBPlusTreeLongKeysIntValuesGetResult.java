package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues;

import java.io.Serializable;

public class AggregatorBPlusTreeLongKeysIntValuesGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public int value;
	
	public AggregatorBPlusTreeLongKeysIntValuesGetResult(){}
}
