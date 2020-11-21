package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnlyLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.IntTreeChangeTracker;

public class IntTreeChangeTrackerScanCursor 
	extends ChangeTrackerCursor<ChangeTrackingStore<Integer>> {	
	protected BPlusTreeIntKeysOnlyLeaf currentLeaf;
	protected int[] keys;
	protected BPlusTreeIntKeysOnly tree;
	
	public IntTreeChangeTrackerScanCursor(DerivedRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize() {
		if (this.deltaSKeys == null) {
			this.currentLeaf = null;
		} else {
			this.tree = ((IntTreeChangeTracker)this.deltaSKeys).getTree();
			this.currentLeaf = this.tree.getFirstChild();
		}
		
		if (this.currentLeaf != null)
			this.keys = this.currentLeaf.getKeys();
				
		this.keyIndex = 0;
	}
	
	@Override
	public void reset() {
		//super.reset();
		if (this.tree != null) {
			this.currentLeaf = this.tree.getFirstChild();
			this.keyIndex = 0;
		}
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		if (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark())
				return this.store.getTuple(this.keys[this.keyIndex++], tuple); 

			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			if (this.currentLeaf != null) {
				this.keys = this.currentLeaf.getKeys();
				this.keyIndex = 0;
				return this.store.getTuple(this.keys[this.keyIndex++], tuple);
			}
		}

		return 0;
	}
}
