package edu.ucla.cs.wis.bigdatalog.database.store.buffer;

import java.io.Serializable;
import java.util.Arrays;

public class DynamicBuffer implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static final int DEFAULT_PAGE_SIZE = 8192;
	private static final int DEFAULT_INITIAL_NUMBER_OF_PAGES = 32;
	
	protected	int 			pageSize;
	protected 	BufferPage[]	leaves;
	protected	int 			numberOfEntries;
	protected 	int 			highWaterMark;
	
	public DynamicBuffer() {
		this(DEFAULT_INITIAL_NUMBER_OF_PAGES, DEFAULT_PAGE_SIZE);
	}
	
	public DynamicBuffer(int pageSize) {
		this(DEFAULT_INITIAL_NUMBER_OF_PAGES, pageSize);
	}
	
	public DynamicBuffer(int initialNumberOfPages, int pageSize) {
		this.pageSize = pageSize;
		this.initialize(initialNumberOfPages);
	}
	
	private void initialize(int numberOfPages) {		
		this.leaves = new BufferPage[numberOfPages];
		this.numberOfEntries = 0;
	}

	public int getNumberOfEntries() { return this.numberOfEntries; }
	
	public int getPageSize() { return this.pageSize; }
		
	public int getCapacity() { 
		int capacity = 0;
		for (int i = 0; i < this.highWaterMark; i++)
			capacity += this.leaves[i].getCapacity();
		return capacity;
	}

	public int append(byte[] data) {
		// 1) attempt to write within existing branch's capacity
		//   if space is available, write and return address
		// 2) not enough capacity, must grow tree 
		if (this.highWaterMark == 0)
			this.allocateLeaf();

		int firstAddress = -1;
		int firstChild = -1;
		int tempAddress;
		byte[] dataToWrite = data;
		while (dataToWrite.length > 0) {
			int availableCapacity = this.leaves[this.highWaterMark - 1].getAvailableCapacity();
			
			if (availableCapacity == 0) {
				 this.allocateLeaf();
			} else if (dataToWrite.length <= availableCapacity) {
				tempAddress = this.leaves[this.highWaterMark - 1].append(dataToWrite);
				if (firstAddress == -1) firstAddress = tempAddress;
				if (firstChild == -1) firstChild = this.highWaterMark - 1;						
				break;
			} else {
				// we don't have enough to write the entire amount, but we can write some
				byte[] partialDataToWrite = Arrays.copyOf(dataToWrite, availableCapacity);

				tempAddress = this.leaves[this.highWaterMark - 1].append(partialDataToWrite);
				if (firstAddress == -1) firstAddress = tempAddress;
				if (firstChild == -1) firstChild = this.highWaterMark - 1;
				
				dataToWrite = Arrays.copyOfRange(dataToWrite, partialDataToWrite.length, dataToWrite.length);
			}
		}
		this.numberOfEntries++;		
		return this.getExternalAddress(firstChild, firstAddress);
	}
	
	private void allocateLeaf() {
		if (this.leaves.length == this.highWaterMark)
			expandDirectory();
		this.leaves[this.highWaterMark++] = new BufferPage(this.pageSize);
	}

	private int getExternalAddress(int leafId, int address) {
		return (leafId * this.pageSize) + address;
	}
	
	private void expandDirectory() {
		BufferPage[] newDirectory = new BufferPage[2 * this.leaves.length];
		
		for (int i = 0; i < this.leaves.length; i++)
			newDirectory[i] = this.leaves[i];
		
		this.leaves = newDirectory;
	}

	public byte[] read(long address, int length) {
		int leafId = (int) (address / this.pageSize);
		int localAddress = (int) (address % this.pageSize);
		int dataReadSoFar = 0;
		int remainingLengthToRead = length;
		
		byte[] data = new byte[length];		
		byte[] partialDataRead;
		
		// we might have to read from multiple pages
		while (remainingLengthToRead > 0) {
			if ((remainingLengthToRead + localAddress) <= this.pageSize) {
				partialDataRead = this.leaves[leafId].read(localAddress, remainingLengthToRead);
				for (int i = 0; i < remainingLengthToRead; i++)
					data[dataReadSoFar + i] = partialDataRead[i];
				
				break;
			}

			int lengthToRead;
			// on first read, we read the remainder from the page
			if (dataReadSoFar == 0) {
				lengthToRead = this.pageSize - localAddress;
			} else {
				// otherwise, determine if we can read within the page, or if it will read from several pages 
				if (remainingLengthToRead > this.pageSize)
					lengthToRead = this.pageSize;
				else
					lengthToRead = remainingLengthToRead;				
			}

			partialDataRead = this.leaves[leafId].read(localAddress, lengthToRead);
				
			for (int i = 0; i < lengthToRead; i++)
				data[dataReadSoFar + i] = partialDataRead[i];

			// setup for next iteration
			dataReadSoFar += lengthToRead;
			remainingLengthToRead -= lengthToRead;
			localAddress = 0;
			leafId++;
		}
		
		return data;
	}

	public void clear() {
		if (this.leaves == null)
			return;
		
		for (int i = 0; i < this.highWaterMark; i++)
			this.leaves[i].clear();
		this.initialize(DynamicBuffer.DEFAULT_INITIAL_NUMBER_OF_PAGES);
	}
	
	public void free() {
		if (this.leaves == null)
			return;
		
		for (int i = 0; i < this.leaves.length; i++) {
			if (this.leaves[i] != null) {
				this.leaves[i].free();
				this.leaves[i] = null;
			}
		}
		
		this.leaves = null;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("# of used leaves | max # of leaves: " + this.highWaterMark + " | " + this.leaves.length + "\n");
		retval.append("Capacity: " + this.getCapacity() + "\n");
		retval.append("# of entries: " + this.numberOfEntries + "\n");
		return retval.toString();
	}
}
