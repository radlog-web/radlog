package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.bytekeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;

public class BPlusTreeByteKeysOnlyInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public BPlusTreeByteKeysOnlyPage newPage;
	public BPlusTreeOperationStatus status;
	
	public BPlusTreeByteKeysOnlyInsertResult() {}
}
