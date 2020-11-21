package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesPage newPage;
	public AggregatorInsertStatus status;
	public DbTypeBase newTotalResult;
	
	public AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesResult(){}
	
	public String toString() {
		return status.name() + " : " + newTotalResult; 
	}
}
