package edu.ucla.cs.wis.bigdatalog.database.store.heap;

import java.io.Serializable;
import java.util.BitSet;

import edu.ucla.cs.wis.bigdatalog.database.store.Data;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public class UnorderedHeapPage implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private int bytesPerEntry;
	private int maxNumberOfEntries;
	private Data data;
	private BitSet deletedEntries;
	protected int highWaterMark; // maximum number of entries so far into this page
	
	public UnorderedHeapPage(){}
	
	public UnorderedHeapPage(int pageSize, int bytesPerEntry) {
		if (pageSize < 1)
			throw new DatabaseException("PageSize must be larger than 0.");
		
		if (bytesPerEntry < 1)
			throw new DatabaseException("BytesPerEntry must be larger than 0.");
		
		this.data = new Data(pageSize);
		this.bytesPerEntry = bytesPerEntry;
		this.maxNumberOfEntries = pageSize / bytesPerEntry;
		this.initialize();
	}
	
	private void initialize() {
		this.highWaterMark = 0;
		this.deletedEntries = new BitSet(this.maxNumberOfEntries);
		this.data.clear();
	}	
	
	public int getHighWaterMark() { return this.highWaterMark; }

	public int getMaxNumberOfEntries() { return this.maxNumberOfEntries; }
	
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
	
	public boolean isFull() { return (this.highWaterMark == this.maxNumberOfEntries); }
	
	public int add(byte[] data) {
		if (data.length != this.bytesPerEntry)
			return -1;
		
		if (!this.data.canWrite(this.bytesPerEntry))
			return -1;
		this.setOffsetToHighWaterMark();
		this.data.write(data);
		
		return highWaterMark++;
	}
	
	public int update(int address, byte[] data) {
		this.setOffsetByAddress(address);
		this.data.write(data);
		return address;
	}

	public byte[] read(int address) {
		if (address >= this.highWaterMark || address < 0)
			return null;

		if (this.deletedEntries.get(address))
			return null;
		
		this.setOffsetByAddress(address);		
		return this.data.read(this.bytesPerEntry);
	}

	public void deleteAll() {
		this.data.reset();
		this.data.clear();
		this.initialize();
	}
	
	public void delete(int address) {
		if (address > this.highWaterMark)
			return;
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
				data = this.read(i);
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
			this.deletedEntries = new BitSet(this.maxNumberOfEntries);
			this.highWaterMark -= numberDeleted;
		}		
	
		return numberDeleted;
	}
		
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Max Number of entries: " + this.maxNumberOfEntries + "\n");
		retval.append("Number of entries: " + this.highWaterMark + "\n");
		retval.append("IsEmpty: " + this.isEmpty() + "\n");
		retval.append("IsFull: " + this.isFull() + "\n");
		return retval.toString();
	}

}
