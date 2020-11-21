package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public abstract class BPlusTreeMultiValueLeaf<L extends BPlusTreeMultiValueLeaf<L>> 
	extends BPlusTreeLeaf<L> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	protected Heap[] values;
	protected int bytesPerValue;
	protected boolean useOrderedHeap;
	
	public BPlusTreeMultiValueLeaf() { super(); }
	
	public BPlusTreeMultiValueLeaf(int nodeSize, int bytesPerKey, int bytesPerValue, boolean useOrderedHeap) {
		super(nodeSize, bytesPerKey);
		this.bytesPerValue = bytesPerValue;
		if (this.bytesPerValue == 0)
			throw new DatabaseException("BPlusTreeLeafByteKeysHeap requires a bytesPerValue setting!");
		
		this.useOrderedHeap = useOrderedHeap;
		this.values = new Heap[this.numberOfKeys];
	}
		
	public Heap[] getValues() { return this.values; }

}
