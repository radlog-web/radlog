package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;

public class AggregatorBPlusTreeLongKeysIntValuesInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeLongKeysIntValuesPage newPage;
	public AggregatorInsertStatus status;	
	
	public AggregatorBPlusTreeLongKeysIntValuesInsertResult(){}
}
