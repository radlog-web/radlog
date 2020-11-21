package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeByteKeysLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeMultiValueLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeMultiStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public class TupleBPlusTreeMultiStoreScanCursor 
	extends Cursor<Tuple>  {

	protected TupleBPlusTreeMultiStore tupleStore;
	protected BPlusTreeMultiValueLeaf<?> currentLeaf;
	protected byte[] 	keyBytes;
	protected int		keyIndex;	
	protected int 		heapIndex;
	protected Heap 	currentHeap;
	protected int		bytesPerKey;
		
	public TupleBPlusTreeMultiStoreScanCursor(Relation<Tuple> relation) {
		super(relation);
		if (!(relation.getTupleStore() instanceof TupleBPlusTreeMultiStore))
			throw new DatabaseException("TupleBPlusTreeStoreScanCursor can only be used on a TupleBPlusTreeStore.");
		
		this.tupleStore = (TupleBPlusTreeMultiStore)relation.getTupleStore();
		this.initialize();
	}
	
	private void initialize() {
		this.currentLeaf = this.tupleStore.getFirstChild();
		this.keyIndex = 0;
		this.heapIndex = 0;
		this.currentHeap = null;
		this.keyBytes = null;
		this.bytesPerKey = this.tupleStore.getBytesPerKey(); 
	}
	
	public void reset() {
		//this.timesCalled[0]++;
		//long start = System.nanoTime();	
		this.initialize();
		//this.timeSpent[0] += System.nanoTime() - start;
	}
	@Override
	public int getTuple(Tuple tuple) {
		//this.timesCalled[1]++;
		//long start = System.nanoTime();	
		
		// get leaf
		// get key & values (heap)
		// get all values from heap
		// when out of values in heap, get next heap
		// when out of heaps, get next leaf
		// when out of leaves, done
		while (this.currentLeaf != null) {
			while ((this.currentHeap != null)
					&& (this.heapIndex < this.currentHeap.getHighWaterMark())) {
				if (this.tupleStore.loadTuple(this.keyBytes, this.currentHeap.get(this.heapIndex++), tuple) > 0)
					return 1;
			}
			
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				this.currentHeap = this.currentLeaf.getValues()[this.keyIndex];
				this.heapIndex = 0;
				if (this.currentHeap != null && this.currentHeap.getNumberOfEntries() > 0) {
					this.keyBytes = new byte[this.bytesPerKey];
					System.arraycopy(((BPlusTreeByteKeysLeaf)this.currentLeaf).getKeys(), 
							this.keyIndex * this.tupleStore.getBytesPerKey(), 
							this.keyBytes, 
							0, 
							this.tupleStore.getBytesPerKey());
					//this.keyBytes = Data.getData(this.currentLeaf.getKeys(), this.keyIndex * this.tupleStore.getBytesPerKey(), this.tupleStore.getBytesPerKey());
				}
				
				this.keyIndex++;				
			} else {
				// out of key/values, so move to next leaf
				this.currentLeaf = this.currentLeaf.getNext();				
				this.keyIndex = 0;
				this.heapIndex = 0;
				this.currentHeap = null;
				this.keyBytes = null;
			}			
		}
		
		//if (tuple == null)
		//	this.timesNoResults[1]++;
		
		//this.timeSpent[1] += System.nanoTime() - start;
		return 0;
	}	
	
	@Override
	public void moveNext() { this.keyIndex++; }
}