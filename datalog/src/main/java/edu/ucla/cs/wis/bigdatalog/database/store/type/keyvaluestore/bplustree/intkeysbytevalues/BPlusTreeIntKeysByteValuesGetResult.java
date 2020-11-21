package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysbytevalues;

import java.io.Serializable;

public class BPlusTreeIntKeysByteValuesGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public byte[] value;
	
	public BPlusTreeIntKeysByteValuesGetResult() {}
	
	public BPlusTreeIntKeysByteValuesGetResult(int bytesPerValue) { 
		this.value = new byte[bytesPerValue];
	}
}
