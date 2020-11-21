package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;

public class RangeSearchResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public BPlusTreeLeaf<?> leaf;
	public int index;
	
	public RangeSearchResult() {}
}
