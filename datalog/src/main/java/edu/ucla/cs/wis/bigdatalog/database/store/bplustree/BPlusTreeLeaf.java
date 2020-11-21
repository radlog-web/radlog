package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;

public abstract class BPlusTreeLeaf<L extends BPlusTreeLeaf<L>> 
	extends BPlusTreePage implements Serializable {
	private static final long serialVersionUID = 1L;
	
	transient protected L next;
	
	public BPlusTreeLeaf() { super(); }
	
	public BPlusTreeLeaf(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);
	}

	public L getNext() { return next; }
	
	public void setNext(L next) { this.next = next; }

	@Override
	public int getHeight() { return 0; }
	
	@Override
	public boolean isEmpty() { return (this.highWaterMark == 0); }
	
	@Override
	public boolean hasOverflow() { return (this.highWaterMark == this.numberOfKeys); }
}
