package edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;

public class BPlusTreeIntKeysBitmapValuesInsertResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public BPlusTreeIntKeysBitmapValuesPage newPage;
	public BPlusTreeOperationStatus status;

	public BPlusTreeIntKeysBitmapValuesInsertResult(){}
}
