package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesInsertResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesPage newPage;
	public AggregatorInsertStatus status;
	public DbTypeBase newTotalResult;
		
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesInsertResult() {}
	
	public String toString() {
		return status.name() + " : " + newTotalResult; 
	}
}
