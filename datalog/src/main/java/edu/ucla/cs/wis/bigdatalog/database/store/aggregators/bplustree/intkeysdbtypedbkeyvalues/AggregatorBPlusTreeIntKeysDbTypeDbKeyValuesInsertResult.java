package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesInsertResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesPage newPage;
	public AggregatorInsertStatus status;
	public DbTypeBase newTotalResult;
	
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesInsertResult() {}
		
	public String toString() {
		return status.name() + " : " + newTotalResult; 
	}
}
