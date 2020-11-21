package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeIntKeysDbTypeValuesInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public BPlusTreeIntKeysDbTypeValuesPage newPage;
	public KeyValueOperationStatus status;
	public DbTypeBase oldValue;
	
	public BPlusTreeIntKeysDbTypeValuesInsertResult() {}
	
	public BPlusTreeIntKeysDbTypeValuesInsertResult(DataType dataType){
		this.oldValue = DbTypeBase.loadFrom(dataType, 0);
	}
}
