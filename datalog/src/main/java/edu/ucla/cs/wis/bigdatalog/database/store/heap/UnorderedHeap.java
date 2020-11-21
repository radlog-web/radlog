package edu.ucla.cs.wis.bigdatalog.database.store.heap;

import java.io.Serializable;

public class UnorderedHeap extends Heap implements Serializable {
	private static final long serialVersionUID = 1L;

	public UnorderedHeap(){}
	
	public UnorderedHeap(int initialSize, int bytesPerEntry) {
		super(initialSize, bytesPerEntry);
	}
	
	public int add(byte[] data) {
		if (data.length != this.bytesPerEntry)
			return -1;
		
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
		
		this.setOffsetToHighWaterMark();
		this.data.write(data);
		
		return highWaterMark++;
	}
}
