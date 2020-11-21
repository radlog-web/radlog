package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeInsertResult;

public class BPlusTreeLongKeysByteValuesInsertResult extends BPlusTreeInsertResult<BPlusTreeLongKeysByteValuesPage> implements Serializable {
	private static final long serialVersionUID = 1L;
	public byte[] oldValue;
	
	public BPlusTreeLongKeysByteValuesInsertResult() { super(); }
	
	public BPlusTreeLongKeysByteValuesInsertResult(int bytesPerValue) {
		this.oldValue = new byte[bytesPerValue];
	}
}
