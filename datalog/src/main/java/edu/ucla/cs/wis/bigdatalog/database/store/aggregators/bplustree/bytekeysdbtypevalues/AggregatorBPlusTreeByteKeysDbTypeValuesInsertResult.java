package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;

public class AggregatorBPlusTreeByteKeysDbTypeValuesInsertResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeByteKeysDbTypeValuesPage newPage;
	public AggregatorInsertStatus status;
	
	public AggregatorBPlusTreeByteKeysDbTypeValuesInsertResult() {}
}
