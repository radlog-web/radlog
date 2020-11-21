package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;

public class AggregatorBPlusTreeIntKeysIntValuesInsertResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeIntKeysIntValuesPage newPage;
	public AggregatorInsertStatus status;
	
	public AggregatorBPlusTreeIntKeysIntValuesInsertResult(){}
}
