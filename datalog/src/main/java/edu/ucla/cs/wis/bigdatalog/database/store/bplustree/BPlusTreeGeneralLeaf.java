package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;

public abstract class BPlusTreeGeneralLeaf<L  extends BPlusTreeLeaf<L>> 
	extends BPlusTreeLeaf<L> implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected byte[] values;
	protected int bytesPerValue;
	
	public BPlusTreeGeneralLeaf() { super(); }
	
	public BPlusTreeGeneralLeaf(int bytesPerLeaf, int bytesPerKey, int bytesPerValue) {
		super(bytesPerLeaf, bytesPerKey);
		this.bytesPerValue = bytesPerValue;
		// must also call this here after bytesPerValue is set
		this.numberOfKeys = this.getNumberOfKeys();
	}

	public byte[] getValues() { return this.values; }
	
	protected int getNumberOfKeys() { 
		return (this.nodeSize / (this.bytesPerKey + this.bytesPerValue)); 
	}
	
}
