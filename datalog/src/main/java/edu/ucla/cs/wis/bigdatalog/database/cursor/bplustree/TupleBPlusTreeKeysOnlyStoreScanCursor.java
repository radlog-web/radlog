package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnlyLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeKeysOnlyStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public class TupleBPlusTreeKeysOnlyStoreScanCursor 
	extends Cursor<Tuple> {

	protected TupleBPlusTreeKeysOnlyStore tupleStore;
	protected BPlusTreeByteKeysOnlyLeaf currentLeaf;
	protected int	keyIndex;
	protected int	bytesPerKey;

	public TupleBPlusTreeKeysOnlyStoreScanCursor(Relation<Tuple> relation) {
		super(relation);
		if (!(relation.getTupleStore() instanceof TupleBPlusTreeKeysOnlyStore))
			throw new DatabaseException("TupleBPlusTreeKeysOnlyStoreScanCursor can only be used on a TupleBPlusTreeKeysOnlyStore.");
		
		this.tupleStore = (TupleBPlusTreeKeysOnlyStore)relation.getTupleStore();
		this.initialize();
	}
	
	private void initialize() {
		this.currentLeaf = (BPlusTreeByteKeysOnlyLeaf) this.tupleStore.getFirstChild();
		this.keyIndex = 0;
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
		// get keys
		// when out of keys, get next leaf
		// when out of leaves, done
		
		while (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				byte[] keyBytes = new byte[this.bytesPerKey];
				System.arraycopy(this.currentLeaf.getKeys(), this.keyIndex++ * this.bytesPerKey, keyBytes, 0, this.bytesPerKey);
				if (this.tupleStore.loadTuple(keyBytes, tuple) > 0)
					return 1;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			this.keyIndex = 0;						
		}		
		//if (tuple == null)
		//	this.timesNoResults[1]++;
		//this.timeSpent[1] += System.nanoTime() - start;		
		return 0;
	}
	
	@Override
	public void moveNext() { this.keyIndex++; }
}
