package edu.ucla.cs.wis.bigdatalog.database.store.buffer;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.Data;

public class BufferPage implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int highWaterMark; // maximum number of entries so far into this page	
	protected Data data;
	protected BufferPage next;
	
	public BufferPage(){}
	
	public BufferPage(int pageSize) {
		this.data = new Data(pageSize);
	}
	
	public int getBytesWritten() { return this.highWaterMark; }

	public int getCapacity() { return this.data.getSize(); }

	public boolean hasAvailableCapacity(int length) {
		this.data.setOffset(this.highWaterMark);
		return this.data.canWrite(length);
	}
	
	public int getAvailableCapacity() {
		return this.data.getSize() - this.highWaterMark;
	}
	
	public boolean isFull() {
		return (this.getBytesWritten() == this.getCapacity());
	}
	
	public byte[] read(int address, int length) {
		return this.data.read(address, length);
	}

	public int append(byte[] data) {
		int startingAddress = this.highWaterMark;
		// only write if we already know it will fit
		this.data.setOffset(this.highWaterMark);
		this.data.write(data);
		this.highWaterMark += data.length;
		return startingAddress;
	}

	public void clear() {
		this.data.reset();
		this.data.clear();		
	}
	
	public void free() {
		this.data = null;
		this.next = null;
	}

	public void delete(int address, int length) {
		byte[] nulls = new byte[length];
		for (int i = 0; i < length; i++)
			nulls[i] = 0;
		this.data.setOffset(address);
		this.data.write(nulls);
	}

	@Override
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("# of bytes used / Capacity:" + this.highWaterMark + "/" + this.getCapacity() + "\n");
		return retval.toString();
	}
}
