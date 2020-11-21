package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysfloatvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysfloatvalues.AggregatorBPlusTreeLongKeysFloatValuesPage;

public class AggregatorBPlusTreeLongKeysFloatValuesInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeLongKeysFloatValuesPage newPage;
	public AggregatorInsertStatus status;
	
	public AggregatorBPlusTreeLongKeysFloatValuesInsertResult(){}
}
