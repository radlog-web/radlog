package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

abstract public class Bucket implements Serializable {
	private static final long serialVersionUID = 1L; 
	protected static final int FAST_GROW_SIZE = 8;
	protected static final int INITIAL_SIZE = 0;
	protected int highWaterMark;
	protected int bytesPerValue;
	protected long[] hashes;
	protected byte[] values;
	
	public Bucket() {}
	
	public Bucket(int bytesPerValue) {
		this.bytesPerValue = bytesPerValue;
	}
	
	protected void initialize() {
		this.highWaterMark = 0;
		this.hashes = new long[INITIAL_SIZE];
		this.values = new byte[INITIAL_SIZE * this.bytesPerValue];
	}
	
	public boolean isEmpty() { return (this.highWaterMark == 0); }
	
	public boolean isFull() { return (this.highWaterMark == this.hashes.length); }

	public int getNumberOfKeys() { return this.highWaterMark; }
	
	public long[] getHashes() { return this.hashes; }
	
	public byte[] getValues() { return this.values; }	
	
	public void clear() {
		this.initialize();
	}
	
	abstract protected void grow();
	
	abstract public String toStringShort();
	
	abstract public MemoryMeasurement getSizeOf();
}
