package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;

public class HashTablePutResult	implements Serializable {
	private static final long serialVersionUID = 1L;
	public KeyValueOperationStatus status;
	public byte[] oldValue;
	
	public HashTablePutResult() {}
	
	public HashTablePutResult(int bytesPerValue) {
		this.oldValue = new byte[bytesPerValue];
	}
}
