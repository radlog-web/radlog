package edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnlyLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleUnorderedHeap 
	implements Serializable {

	private static final long serialVersionUID = 1L;
	private final int DEFAULT_SIZE_OF_PAGE_DIRECTORY = 32;

	protected 	int 			pageSize;	
	protected 	DataType[] 		schema;
	private 	int 			bytesPerTuple;
	private		int 			tuplesPerPage;
	
	public 		TupleRowPageLeaf[]	pages;
	private		int 				numberOfTuples;
	protected	int 				highWaterMark;
	protected 	AddressedTuple		caputedTuple;
	protected 	int					lastTupleAddress;
	protected 	TypeManager			typeManager;

	public TupleUnorderedHeap(){}
	
	public TupleUnorderedHeap(int pageSize, DataType[] columnTypes, TypeManager typeManager) {
		this.pageSize = pageSize;
		this.schema = columnTypes;
		this.typeManager = typeManager;
		
		int bytesPerTuple = 0;
		for (int i = 0; i < this.schema.length; i++)
			bytesPerTuple += this.schema[i].getNumberOfBytes();
		
		this.bytesPerTuple = bytesPerTuple;
		this.tuplesPerPage = this.pageSize / this.bytesPerTuple;
		this.caputedTuple = this.getEmptyTuple();
		
		this.initialize();
	}
	
	public boolean isEmpty() { return this.pages[0] == null; }

	public int getPageSize() { return this.pageSize; }

	public int getNumberOfTuples() { return this.numberOfTuples; }

	public int getArity() { return this.schema.length; }

	public DataType[] getSchema() { return this.schema; }

	public int getHighWaterMark() { return this.highWaterMark; }
	
	public int getBytesPerTuple() { return this.bytesPerTuple; }
	
	private void initialize() {		 
		this.pages = new TupleRowPageLeaf[DEFAULT_SIZE_OF_PAGE_DIRECTORY];
		this.highWaterMark = 0;
		this.numberOfTuples = 0;
		this.lastTupleAddress = -1;
	}

	public int getFirstTupleAddress() {
		if (this.pages[0] == null)
			return -1;
				
		int firstAddress;
		for (int i = 0; i < this.highWaterMark; i++) {
			firstAddress = this.pages[i].getFirstTupleAddress();
			if (firstAddress != -1)
				return this.getExternalAddress(i, firstAddress);			
		}
			
		return -1;
	}

	public int getLastTupleAddress() {
		return this.lastTupleAddress;
	}

	private TupleRowPageLeaf allocatePage() {
		// expand directory if we've reached the end
		if (this.highWaterMark > (this.pages.length - 1)) {
			TupleRowPageLeaf[] newPageDirectory = new TupleRowPageLeaf[this.pages.length * 2];
			for (int i = 0; i < this.pages.length; i++)
				newPageDirectory[i] = this.pages[i];

			this.pages = newPageDirectory;
		}
		
		TupleRowPageLeaf page = new TupleRowPageLeaf(this.pageSize, this.bytesPerTuple, this.schema, this.typeManager);
		this.pages[this.highWaterMark++] = page;

		return page;
	}

	private void freePage(int position) {
		if ((position < 0) || (position > this.highWaterMark - 1))
			return;

		TupleRowPageLeaf page = this.pages[position];
		this.pages[position] = null;
		page.deleteAll();
		page.commit();
		
		if (position == this.highWaterMark - 1)
			this.highWaterMark--;
	}

	public int appendTuple(AddressedTuple tuple) {
		// three cases:
		// 1) this is the first insert in this page list, we need a page
		// 2) space available for tuple on current page
		// 3) page is full, we need a new page
		TupleRowPageLeaf page;
		
		// case 1
		if (this.isEmpty()) {
			page = this.allocatePage();
		} else {
			page = this.pages[this.highWaterMark - 1];

			// case 3 is if the page is full
			if (page.isFull()) {
				page = this.allocatePage();
			} else {
				// case 2 is the page has space - we want to ensure and append, so move the offset to the highwatermark
				page.setOffsetToHighWaterMark();
			}
		}

		int address = page.appendTuple(tuple);

		tuple.address = this.getExternalAddress(this.highWaterMark - 1, address);
		this.numberOfTuples++;
		this.lastTupleAddress = tuple.address;
		
		return tuple.address;
	}

	private int appendTupleData(byte[] tupleData) {
		// three cases:
		// 1) this is the first insert in this page list, we need a page
		// 2) space available for tuple on current page
		// 3) page is full, we need a new page
		TupleRowPageLeaf page;
		
		// case 1
		if (this.isEmpty()) {
			page = this.allocatePage();
		} else {
			page = this.pages[this.highWaterMark - 1];

			// case 3 is if the page is full
			if (page.isFull()) {
				page = this.allocatePage();
			} else {
				// case 2 is the page has space - we want to ensure and append, so move the offset to the highwatermark
				page.setOffsetToHighWaterMark();
			}
		}

		int address = page.appendTupleData(tupleData);
		this.numberOfTuples++;
		this.lastTupleAddress = this.getExternalAddress(this.highWaterMark - 1, address);
		return this.lastTupleAddress;		
	}
	
	public int updateTuple(AddressedTuple tuple) {
		int address = tuple.address;
		if (address < 0)
			return -1;

		int pageId = this.getPageId(address);
		int tupleId = this.getTupleId(address);

		if (pageId < 0 || pageId >= this.highWaterMark) 
			return -1;

		TupleRowPageLeaf page = this.pages[pageId];
		if (page == null)
			return -1;

		page.setOffsetByAddress(tupleId);
		page.updateTuple(tupleId, tuple);
		
		return tuple.address;
	}

	public int getTuple(int address, AddressedTuple tuple) {	
		if (address < 0) 
			return 0;
		
		int pageId = address / this.tuplesPerPage;
		int tupleId = address % this.tuplesPerPage;

		if (pageId >= this.highWaterMark) 
			return 0;

		if (this.pages[pageId].readTuple(tupleId, tuple) >= 0) {
			tuple.address = address;
			return 1;
		}
		return 0;
	}
	
	public int getPageId(int address) { return address / this.tuplesPerPage; }

	public int getTupleId(int address) { return address % this.tuplesPerPage; }

	public AddressedTuple exists(AddressedTuple tuple) {
		boolean found = true;
		int originalOffset = 0;
		TupleRowPageLeaf page;
		
		for (int leafId = 0; leafId < this.highWaterMark; leafId++) {
			page = this.pages[leafId];
			originalOffset = page.getOffset();

			for (int i = page.getFirstTupleAddress(); i <= page.getLastTupleAddress(); i++) {
				if (page.readTuple(i, this.caputedTuple) < 0)
					continue;

				for (int j = 0; j < this.schema.length; j++) {
					if (!tuple.getColumn(j).equals(this.caputedTuple.getColumn(j))) {
						found = false;
						break;
					}
				}

				if (found && !this.caputedTuple.isDeleted()) {
					page.setOffset(originalOffset);					
					this.caputedTuple.address =  this.getExternalAddress(leafId,  this.caputedTuple.address);
					return this.caputedTuple;
				}
				found = true;
			}

			page.setOffset(originalOffset);
		}

		return null;
	}

	public void deleteTuple(AddressedTuple tuple) {
		if (tuple == null)
			return;

		this.deleteTuple(tuple.address);
	}
	
	public void deleteTuple(int address) {
		if (address < 0)
			return;
		
		int pageId = this.getPageId(address);
		int tupleId = this.getTupleId(address);

		this.pages[pageId].deleteTuple(tupleId);
		this.numberOfTuples--;
		if (this.lastTupleAddress == address)
			this.lastTupleAddress--;
	}

	public int commit() {
		int totalNumberDeleted = 0;
		TupleRowPageLeaf page, nextPage;

		// first delete all tuples and compact within the page
		for (int i = 0; i < this.highWaterMark; i++)
			totalNumberDeleted += this.pages[i].commit();

		// compact the pages since there might be empty area at their ends
		for (int i = 0; i < this.highWaterMark; i++) {
			page = this.pages[i];
			// if full or empty, go to next page
			// if empty, we delete later
			if (page.isFull() || page.isEmpty())
				continue;

			int numberOfPages = this.highWaterMark;
			// take tuples for later pages to fill earlier pages 
			for (int j = i + 1; j < numberOfPages; j++) {
				nextPage = this.pages[j];
				if (nextPage.isEmpty())
					continue;
				
				page.setOffsetToHighWaterMark();
				
				byte[] tupleToMoveData;
				// move tuples until the page is full or the nextpage is empty
				for (int k = 0; k <= nextPage.getLastTupleAddress(); k++) {
					tupleToMoveData = nextPage.readTupleData(k);
					if (tupleToMoveData == null)
						continue;
					
					page.appendTupleData(tupleToMoveData);
					nextPage.deleteTuple(k);
					if (page.isFull()) {
						page.commit();
						break;
					}						
				}
				
				nextPage.commit();
				if (page.isFull())
					break;
			}
		}
		
		boolean removedPage = false;
		// remove pages that were emptied
		for (int i = this.highWaterMark - 1; i >= 0; i--) {
			if (this.pages[i].isEmpty()) {
				this.freePage(i);
				removedPage = true;
			}
		}
		
		if (removedPage) {
			// finally, compact the array if at least 1 page was removed
			int numberOfPagesRemaining = 0;
					
			for (int i = 0; i < this.highWaterMark; i++) {
				if (this.pages[i] != null)
					numberOfPagesRemaining++;	
			}
			
			int sizeOfNewDirectory = DEFAULT_SIZE_OF_PAGE_DIRECTORY;
			if (numberOfPagesRemaining > DEFAULT_SIZE_OF_PAGE_DIRECTORY) {
				// round up to the nearest power of two
				// found this algorithm at http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
				sizeOfNewDirectory = numberOfPagesRemaining; 
				sizeOfNewDirectory--;
				sizeOfNewDirectory |= sizeOfNewDirectory >> 1;
				sizeOfNewDirectory |= sizeOfNewDirectory >> 2;
				sizeOfNewDirectory |= sizeOfNewDirectory >> 4;
				sizeOfNewDirectory |= sizeOfNewDirectory >> 8;
				sizeOfNewDirectory |= sizeOfNewDirectory >> 16;
				sizeOfNewDirectory++;
			}
	
			TupleRowPageLeaf[] tempPages = new TupleRowPageLeaf[sizeOfNewDirectory];
			
			int position = 0;
			for (int i = 0; i < this.highWaterMark; i++) {
				if (this.pages[i] != null) {
					tempPages[position] = this.pages[i];
					position++;
				}
			}
			// overwrite previous directory with new one
			this.pages = tempPages;
			
			this.highWaterMark = position;
		}
		
		if (this.pages[0] == null) {
			this.lastTupleAddress = -1;
		} else {
			int lastPageId = this.highWaterMark - 1;
			int lastPageLastTupleAddress = this.pages[lastPageId].getLastTupleAddress();

			this.lastTupleAddress = this.getExternalAddress(lastPageId, lastPageLastTupleAddress);
		}		
		
		return totalNumberDeleted;
	}	

	public void deleteAll() {
		for (int i = this.highWaterMark -1; i >= 0; i--) {
			this.freePage(i);
			this.pages[i] = null;
		}

		this.initialize();
	}

	public int getExternalAddress(int leafId, int address) {
		return (leafId * (this.pageSize / this.bytesPerTuple)) + address;
	}
	
	public void sort() {
		// To sort this unordered heap, we:
		//   - load a bplustree from this heap
		//   - clear this heap
		//   - load it again from the bplustree
		// 1) declare BPlusTree keyed on all columns
		// 2) load BPlusTree with all tuples
		// 3) removeAll from this heap
		// 4) load all from bplustree back into this heap
		
		//int nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.tuplestores.bplustree.nodesize"));
		int nodeSize = 256;
		BPlusTreeByteKeysOnly bPlusTree = new BPlusTreeByteKeysOnly(nodeSize, this.bytesPerTuple, this.schema/*, this.deALSConfiguration*/); 
 		TupleRowPageLeaf currentHeapLeaf = null;
		int leafIndex = -1;
		int tupleIndex = 0;
		byte[] tuple = null;

		while (leafIndex < this.highWaterMark) {
			while ((currentHeapLeaf != null)
					&& (tupleIndex <= currentHeapLeaf.getLastTupleAddress())
					&& (tuple = currentHeapLeaf.readTupleData(tupleIndex++)) != null) {
				bPlusTree.insert(tuple);
			}
			
			// if we reached here, we have gone through the entire heap
			if (leafIndex + 1 >= this.highWaterMark)
				break;

			if ((currentHeapLeaf = this.pages[++leafIndex]) == null)
				continue;

			tupleIndex = 0;
		}

		if (bPlusTree.getNumberOfEntries() == this.getNumberOfTuples()) {
			this.deleteAll();

			BPlusTreeLeaf<?> currentBPlusTreeLeaf = bPlusTree.getFirstChild();
			int keyIndex = 0;
		
			// get leaf
			// get keys
			// when out of keys, get next leaf
			// when out of leaves, done			
			while (currentBPlusTreeLeaf != null) {
				while (keyIndex < currentBPlusTreeLeaf.getHighWaterMark()) {
					byte[] tupleData = new byte[this.bytesPerTuple];
					System.arraycopy(((BPlusTreeByteKeysOnlyLeaf) currentBPlusTreeLeaf).getKeys(), keyIndex++ * this.bytesPerTuple, tupleData, 0, this.bytesPerTuple);
					this.appendTupleData(tupleData);
				}
				
				// out of keys, so move to next leaf
				currentBPlusTreeLeaf = currentBPlusTreeLeaf.getNext();
				keyIndex = 0;
			}
		}
		// cleanup
		bPlusTree.deleteAll();
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("# of pages: " + this.highWaterMark);
		retval.append("\n# of tuples: " + this.numberOfTuples);
		return retval.toString();
	}
	
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		MemoryMeasurement sizes;
		for (int i = 0; i < this.highWaterMark; i++) {
			sizes = this.pages[i].getSizeOf();
			used += sizes.getUsed();
			allocated += sizes.getAllocated();
		}
		
		return new MemoryMeasurement(used, allocated);
	}
	
	public AddressedTuple getEmptyTuple() {
		AddressedTuple tuple = new AddressedTuple(this.schema.length);
		for (int i = 0; i < this.schema.length; i++) {
			// APS 7/16/2014 using loadFrom and defaulting all values to 0
			tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], 0);
		}
		
		return tuple;
	}
}
