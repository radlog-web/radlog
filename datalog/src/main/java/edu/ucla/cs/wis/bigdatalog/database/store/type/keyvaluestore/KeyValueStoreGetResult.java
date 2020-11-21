package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class KeyValueStoreGetResult	implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public DbTypeBase value;
	
	public KeyValueStoreGetResult(){}
	
	public KeyValueStoreGetResult(DataType dataType) {
		this.value = DbTypeBase.loadFrom(dataType, 0);
	}
}
