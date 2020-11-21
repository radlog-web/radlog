package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysfloatvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;

public class AggregatorBPlusTreeIntKeysFloatValuesInsertResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeIntKeysFloatValuesPage newPage;
	public AggregatorInsertStatus status;
	
	public AggregatorBPlusTreeIntKeysFloatValuesInsertResult(){}
}
