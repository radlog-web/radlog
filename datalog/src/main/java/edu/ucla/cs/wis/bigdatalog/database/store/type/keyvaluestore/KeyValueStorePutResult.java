package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class KeyValueStorePutResult	implements Serializable {
	private static final long serialVersionUID = 1L;

	public KeyValueOperationStatus status;
	public DbTypeBase oldValue;
	
	public KeyValueStorePutResult(){}
	
	public KeyValueStorePutResult(DataType dataType) {
		this.oldValue = DbTypeBase.loadFrom(dataType, 0);
	}

}
