package edu.ucla.cs.wis.bigdatalog.database.cursor.heap;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;

import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeap;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;

public class HeapScanCursor 
	extends Cursor<AddressedTuple> {

	private TupleUnorderedHeapStore tupleUnorderedHeapStore;
	private TupleUnorderedHeap heapPageDirectory;
	private int currentTupleAddress;
	
	public HeapScanCursor(Relation<AddressedTuple> relation) {
		super(relation);

		this.tupleUnorderedHeapStore = (TupleUnorderedHeapStore)this.relation.getTupleStore();
		this.initialize();
	}

	public void reset() {
		this.initialize();		
	}
	
	public void initialize() {
		this.heapPageDirectory = this.tupleUnorderedHeapStore.getDirectory();
		this.currentTupleAddress = -1;
	}
	
	public int getTuple(AddressedTuple tuple) {		
		if (this.currentTupleAddress == -1)
			this.currentTupleAddress = 0;			
		
		for (int i = this.currentTupleAddress; i <= this.heapPageDirectory.getLastTupleAddress(); i++) {
			this.currentTupleAddress++;
			if (this.heapPageDirectory.getTuple(i, tuple) > 0)
				return 1;
		}

		return 0;
	}
	
	public void moveNext() { 
		this.currentTupleAddress++;
	}
}