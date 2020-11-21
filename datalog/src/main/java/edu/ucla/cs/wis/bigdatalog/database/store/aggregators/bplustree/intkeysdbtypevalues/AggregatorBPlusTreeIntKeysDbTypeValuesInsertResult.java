package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;

public class AggregatorBPlusTreeIntKeysDbTypeValuesInsertResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeIntKeysDbTypeValuesPage newPage;
	public AggregatorInsertStatus status;
	
	public AggregatorBPlusTreeIntKeysDbTypeValuesInsertResult(){}
}
