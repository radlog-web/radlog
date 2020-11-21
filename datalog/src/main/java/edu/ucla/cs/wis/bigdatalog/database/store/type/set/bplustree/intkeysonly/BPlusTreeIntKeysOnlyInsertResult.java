package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.intkeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;

public class BPlusTreeIntKeysOnlyInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public BPlusTreeIntKeysOnlyPage newPage;
	public BPlusTreeOperationStatus status;
	
	public BPlusTreeIntKeysOnlyInsertResult(){}
}
