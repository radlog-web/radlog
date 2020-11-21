package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;

public abstract class BPlusTreeInsertResult<T> implements Serializable {
	private static final long serialVersionUID = 1L;
	public T newPage;
	public BPlusTreeOperationStatus status;
	
	public BPlusTreeInsertResult() {}
}
