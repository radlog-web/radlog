package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;

abstract public class BPlusTreeNode<P extends BPlusTreeElement, L extends BPlusTreeLeaf<L>> 
	extends BPlusTreePage implements Serializable {
	private static final long serialVersionUID = 1L;

	public P[] children;
	protected int numberOfEntries;
	
	public BPlusTreeNode() { super(); }
	
	public BPlusTreeNode(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);
	}
	
	public int getNumberOfEntries() { return this.numberOfEntries; }
	
	@Override
	public int getHeight() {
		if (this.highWaterMark == 0)
			return 1;
		
		// all levels at same height in the tree
		return this.children[0].getHeight() + 1;
	}
	
	@Override
	public boolean isEmpty() {
		return (this.highWaterMark == 0);
	}
	
	@Override
	public boolean hasOverflow() {
		return (this.highWaterMark == this.children.length); 
	}

	@SuppressWarnings("unchecked")
	public L getFirstChild() {
		if (this.children == null || this.children[0] == null)
			return null;
		
		if (this.getHeight() > 1)
			return ((BPlusTreeNode<P,L>) this.children[0]).getFirstChild();
		
		return (L) this.children[0];
	}
	
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < this.highWaterMark; i++)
			output.append(this.children[i].toStringShort());
		return output.toString();
	} 
}
