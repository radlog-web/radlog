package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;

public class AggregatorBPlusTreeLongKeysDbTypeValuesInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeLongKeysDbTypeValuesPage newPage;
	public AggregatorInsertStatus status;
	
	public AggregatorBPlusTreeLongKeysDbTypeValuesInsertResult(){}
}
