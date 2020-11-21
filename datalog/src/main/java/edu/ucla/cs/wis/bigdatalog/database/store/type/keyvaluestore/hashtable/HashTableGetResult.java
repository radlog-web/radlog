package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable;

import java.io.Serializable;

public class HashTableGetResult	implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public byte[] value;
	
	public HashTableGetResult() {}
	
	public HashTableGetResult(int bytesPerValue) {
		this.value = new byte[bytesPerValue];
	}

}
