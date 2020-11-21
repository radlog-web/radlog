package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;

public abstract class BPlusTreeSecondaryIndexLeaf<L extends BPlusTreeLeaf<L>> extends BPlusTreeLeaf<L> {
	private static final long serialVersionUID = 1L;

	protected TupleAddressArray[] entryArray;
	
	public BPlusTreeSecondaryIndexLeaf(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);
	}

	public TupleAddressArray[] getAddressArrays() { return this.entryArray; }
	
}
