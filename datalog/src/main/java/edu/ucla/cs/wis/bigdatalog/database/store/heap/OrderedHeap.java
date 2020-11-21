package edu.ucla.cs.wis.bigdatalog.database.store.heap;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;

public class OrderedHeap extends Heap implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public OrderedHeap(){}
	
	public OrderedHeap(int initialSize, int bytesPerEntry) {
		super(initialSize, bytesPerEntry);
	}
	
	public int add(byte[] data) {
		if (data.length != this.bytesPerEntry)
			return -1;
		
		int originalOffset = this.data.getOffset();
		
		this.setOffsetToHighWaterMark();
		
		// if full, clean up or grow or do both
		if (!this.data.canWrite(this.bytesPerEntry)) {
			if (this.deletedEntries.isEmpty()) {
				this.grow();
			} else {
				// cleanup deleted entries
				if (this.commit() == 0)
					this.grow();
			}
		}
		// now find location to insert				
		byte[] entry;
		int i;
		for (i = 0; i < this.highWaterMark; i++) {			
			entry = this.data.read(i * this.bytesPerEntry, this.bytesPerEntry);
			if (ByteArrayHelper.compare(data, entry, this.bytesPerEntry) < 0)
				break;
		}

		int insertAt = i * this.bytesPerEntry;
		
		// shift data right so we can insert here if necessary
		if (i < this.highWaterMark)
			this.data.shiftRight(insertAt, this.bytesPerEntry);

		this.data.setOffset(insertAt);
		this.data.write(data);
		
		this.setOffset(originalOffset);
		return highWaterMark++;
	}
}
