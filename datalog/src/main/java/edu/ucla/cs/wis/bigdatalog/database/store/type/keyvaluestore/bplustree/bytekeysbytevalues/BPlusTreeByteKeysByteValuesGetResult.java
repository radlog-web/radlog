package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.bytekeysbytevalues;

import java.io.Serializable;

public class BPlusTreeByteKeysByteValuesGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public byte[] value;
	
	public BPlusTreeByteKeysByteValuesGetResult() {}
	
	public BPlusTreeByteKeysByteValuesGetResult(int bytesPerValue) { 
		this.value = new byte[bytesPerValue];
	}
}
