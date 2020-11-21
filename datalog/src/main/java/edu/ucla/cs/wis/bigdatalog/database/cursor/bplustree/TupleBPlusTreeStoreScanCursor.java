package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import java.util.List;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStoreLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStore;

public class TupleBPlusTreeStoreScanCursor 
	extends Cursor<Tuple> {
	protected TupleBPlusTreeStore 		tupleStore;
	protected BPlusTreeTupleStoreLeaf	currentLeaf;
	protected int						keyIndex;	
	protected List<Tuple> 				tuples;
	protected int						numberOfTuples;
	protected int 						tupleIndex;
	
	public TupleBPlusTreeStoreScanCursor(Relation<Tuple> relation) {
		super(relation);
		
		this.tupleStore = (TupleBPlusTreeStore)relation.getTupleStore();
		this.initialize();
	}
	
	private void initialize() {
		this.currentLeaf = this.tupleStore.getFirstChild();
		this.keyIndex = 0;
		this.tuples = null;
		this.numberOfTuples = 0;
		this.tupleIndex = 0;
	}
	
	public void reset() {
		this.initialize();
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		// get tuple from current key's tuple's
		if ((this.numberOfTuples > 0) && (this.tupleIndex < this.numberOfTuples)) {
			tuple.setValues(this.tuples.get(this.tupleIndex++));
			return 1;
		}

		// exhausted previous key's tuples, then get next key's tuples 
		// when exhausted current leaf, get next leaf
		// when exhausted all leaves, done
		while (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				this.tuples = this.currentLeaf.getTuples()[this.keyIndex++];
				this.numberOfTuples = this.tuples.size();
				this.tupleIndex = 0;
				tuple.setValues(this.tuples.get(this.tupleIndex++));
				return 1;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			this.keyIndex = 0;
			this.tupleIndex = 0;
		}
		
		return 0;
	}	
	
	@Override
	public void moveNext() { this.tupleIndex++; }
	
	// so we only take the first tuple from the key
	public void startNextKey() {
		// keyIndex has already been incremented, so just set the tuple index
		if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
			this.tuples = this.currentLeaf.getTuples()[this.keyIndex++];
			this.numberOfTuples = this.tuples.size();
			this.tupleIndex = 0;
		} else {
			this.currentLeaf = this.currentLeaf.getNext();
			this.keyIndex = 0;
			this.tupleIndex = 0;
			this.numberOfTuples = 0;
		}
	}

}