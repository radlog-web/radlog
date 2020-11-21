package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeIntKeysDbTypeValuesGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public DbTypeBase value;
	
	public BPlusTreeIntKeysDbTypeValuesGetResult() {}
	
	public BPlusTreeIntKeysDbTypeValuesGetResult(DataType dataType){
		this.value = DbTypeBase.loadFrom(dataType, 0);
	}
}
