package edu.ucla.cs.wis.bigdatalog.database.store.heap;

import java.io.Serializable;
import java.util.BitSet;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.Data;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

abstract public class Heap 
	implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	private static final int FAST_GROW_SIZE = 8;

	protected int bytesPerEntry;
	protected Data data;
	protected BitSet deletedEntries;
	protected int highWaterMark; // maximum number of entries so far into this page
	
	public Heap(){}
	
	public Heap(int initialSize, int bytesPerEntry) {		
		if (initialSize < 1)
			throw new DatabaseException("InitialSize must be larger than 0.");
		
		if (bytesPerEntry < 1)
			throw new DatabaseException("BytesPerEntry must be larger than 0.");
		
		this.data = new Data(initialSize);
		this.bytesPerEntry = bytesPerEntry;
		this.initialize();
	}
	
	private void initialize() {
		this.highWaterMark = 0;
		this.deletedEntries = new BitSet(this.getMaxNumberOfEntries());
		this.data.clear();
	}
	
	public int getHighWaterMark() { return this.highWaterMark; }

	public int getNumberOfEntries() { return this.highWaterMark - this.deletedEntries.cardinality(); }
	
	public int getMaxNumberOfEntries() { return this.data.getSize() / this.bytesPerEntry; }
	
	public int getOffset() { return this.data.getOffset(); }
	
	public void setOffset(int offset) { this.data.setOffset(offset); }
	
	public void setOffsetByAddress(long address) {
		if (address < 0) {
			this.data.setOffset(0);
			return;
		}
		
		this.data.setOffset(((int)address) * this.bytesPerEntry);
	}
	
	public void setOffsetToHighWaterMark() {			
		this.data.setOffset(this.highWaterMark * this.bytesPerEntry);
	}
	
	public int getFirstAddress() {
		if (this.highWaterMark > 0)
			return 0;
		
		return -1;
	}
	
	public int getLastAddress() {
		if (this.highWaterMark == 0)
			return -1;
		
		return (this.highWaterMark - 1);
	}

	public boolean isEmpty() { return (this.highWaterMark == 0); }
	
	public boolean isFull() { return (this.highWaterMark == this.getMaxNumberOfEntries()); }
	
	abstract public int add(byte[] data);
	
	public int update(int address, byte[] data) {
		this.setOffsetByAddress(address);
		this.data.write(data);
		return address;
	}

	public byte[] get(int address) {
		if (address >= this.highWaterMark || address < 0)
			return null;

		if (this.deletedEntries.get(address))
			return null;
		
		this.setOffsetByAddress(address);		
		return this.data.read(this.bytesPerEntry);
	}
	
	public int exists(byte[] data) {
		byte[] temp;
		int originalOffset = this.data.getOffset();
		for (int i = this.getFirstAddress(); i <= this.getLastAddress(); i++) {
			temp = this.get(i);
			if (temp == null)
				continue;
			if (ByteArrayHelper.compare(data, temp, this.bytesPerEntry) == 0)
				return i;
		}
		
		this.setOffset(originalOffset);

		return -1;
	}

	public void deleteAll() {
		this.data.reset();
		this.data.clear();
		this.initialize();
	}
	
	public void delete(int address) {
		if (address > this.highWaterMark) return;
		if (address < 0) return;
		// mark the tuple at address as invalid
		this.deletedEntries.set(address);
	}
	
	public int commit() {
		if (this.deletedEntries.isEmpty())
			return 0;
		
		int numberDeleted = 0;
		
		// if deleting everything, just reset the data block		
		if (this.deletedEntries.intersects(new BitSet())) {
			numberDeleted = this.deletedEntries.size();
			this.deleteAll();
		} else {		
			// to commit the deletes, we will move the valid records to a new data block
			Data newData = new Data(this.data.getSize());
			Data temp;
			
			this.data.setOffset(0);
			byte[] data;
			for (int i = 0; i <= this.getLastAddress(); i++) {			
				data = this.get(i);
				// if we have null = we deleted the tuple
				if (data == null) {
					numberDeleted++;
					continue;
				}
				temp = this.data;
				this.data = newData;
				this.data.write(data);				
				this.data = temp;
			}
			
			this.data = newData;
			this.setOffset(0);
			this.deletedEntries = new BitSet(this.getMaxNumberOfEntries());
			this.highWaterMark -= numberDeleted;
		}		
	
		return numberDeleted;
	}
	
	protected void grow() {
		int newSize = this.data.getSize();
		if (newSize < (FAST_GROW_SIZE * this.bytesPerEntry))
			newSize *= 2;
		else
			newSize *= 1.25;
		
		Data newData = new Data(newSize);		
		newData.write(this.data.read(0, this.data.getSize()));		
		newData.setOffset(this.data.getOffset());
		this.data = newData;
	}
		
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Max Size: " + this.data.getSize());
		retval.append(", Max Entries: " + this.getMaxNumberOfEntries() + ", Number of entries: " + this.highWaterMark );
		retval.append(", IsEmpty: " + this.isEmpty() + ", IsFull: " + this.isFull());
		return retval.toString();
	}

	public MemoryMeasurement getSizeOf() {
		int used = this.bytesPerEntry * this.highWaterMark;
		int allocated = this.data.getSize();
		return new MemoryMeasurement(used, allocated);
	}
}
