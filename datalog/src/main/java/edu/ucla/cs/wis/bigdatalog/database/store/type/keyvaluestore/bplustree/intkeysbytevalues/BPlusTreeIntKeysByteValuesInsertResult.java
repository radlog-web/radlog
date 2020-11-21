package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;

public class BPlusTreeIntKeysByteValuesInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public BPlusTreeIntKeysByteValuesPage newPage;
	public KeyValueOperationStatus status;
	public byte[] oldValue;
	
	public BPlusTreeIntKeysByteValuesInsertResult() {}
	
	public BPlusTreeIntKeysByteValuesInsertResult(int bytesPerValue) {
		this.oldValue = new byte[bytesPerValue];
	}
}
