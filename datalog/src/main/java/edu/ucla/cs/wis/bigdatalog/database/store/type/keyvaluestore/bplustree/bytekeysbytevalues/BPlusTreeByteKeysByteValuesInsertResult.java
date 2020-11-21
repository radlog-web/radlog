package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.bytekeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;

public class BPlusTreeByteKeysByteValuesInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public BPlusTreeByteKeysByteValuesPage newPage;
	public KeyValueOperationStatus status;
	public byte[] oldValue;
	
	public BPlusTreeByteKeysByteValuesInsertResult() {}
	
	public BPlusTreeByteKeysByteValuesInsertResult(int bytesPerValue) { 
		this.oldValue = new byte[bytesPerValue];
	}
}
