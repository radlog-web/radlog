package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeInsertResult;

public class BPlusTreeByteKeysByteValuesInsertResult extends BPlusTreeInsertResult<BPlusTreeByteKeysByteValuesPage> implements Serializable {
	private static final long serialVersionUID = 1L;
	public byte[] oldValue;
	
	public BPlusTreeByteKeysByteValuesInsertResult() { super(); }
	
	public BPlusTreeByteKeysByteValuesInsertResult(int bytesPerValue) {
		this.oldValue = new byte[bytesPerValue];
	}
}
