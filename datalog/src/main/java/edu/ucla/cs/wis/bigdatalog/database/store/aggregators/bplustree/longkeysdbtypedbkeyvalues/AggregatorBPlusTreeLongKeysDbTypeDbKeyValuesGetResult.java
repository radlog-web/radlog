package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public DbTypeBase value;
	
	public AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesGetResult() {}
	
	public AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesGetResult(DataType dataType) {
		this.value = DbTypeBase.loadFrom(dataType, 0);
	}
}
