package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeInsertResult;

public class BPlusTreeIntKeysByteValuesInsertResult extends BPlusTreeInsertResult<BPlusTreeIntKeysByteValuesPage> implements Serializable {
	private static final long serialVersionUID = 1L;
	public byte[] oldValue;
	
	public BPlusTreeIntKeysByteValuesInsertResult() { super(); }
	
	public BPlusTreeIntKeysByteValuesInsertResult(int bytesPerValue) {
		this.oldValue = new byte[bytesPerValue];
	}
}
