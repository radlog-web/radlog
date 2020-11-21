package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public DbTypeBase value;
	
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResult() {}
}
