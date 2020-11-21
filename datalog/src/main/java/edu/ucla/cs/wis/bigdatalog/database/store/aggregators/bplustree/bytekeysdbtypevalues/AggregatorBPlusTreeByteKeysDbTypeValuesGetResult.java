package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeByteKeysDbTypeValuesGetResult implements Serializable  {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public DbTypeBase value;
	
	public AggregatorBPlusTreeByteKeysDbTypeValuesGetResult() {}
}
