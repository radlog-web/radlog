package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

abstract public class BPlusTreePage /*extends Page */
	implements BPlusTreeElement, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int nodeSize;
	protected int bytesPerKey;
	public int numberOfKeys;
	public int highWaterMark; // maximum number of entries so far into this page

	public BPlusTreePage() {};
	
	public BPlusTreePage(int nodeSize, int bytesPerKey) {
		this.nodeSize = nodeSize;
		this.bytesPerKey = bytesPerKey;
		this.numberOfKeys = this.getNumberOfKeys();
		this.highWaterMark = 0;
	}
	
	public int getBytesPerKey() { return this.bytesPerKey; }
	
	protected int getBranchingFactor() { return this.numberOfKeys + 1; }
	
	protected int getNumberOfKeys() { 
		return (this.nodeSize / this.bytesPerKey); 
	}
	
	public int getHighWaterMark() { return this.highWaterMark; }
	
	public String toString() { return toString(0); }
	
	abstract public int getHeight();

	abstract public boolean isEmpty();
	
	abstract public boolean hasOverflow();
	
	abstract public void deleteAll();
	
	abstract public MemoryMeasurement getSizeOf();
	
	abstract public String toString(int indent);
}
