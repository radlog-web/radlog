package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnlyLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.GeneralChangeTracker;

public class GeneralChangeTrackerScanCursor 
	extends ChangeTrackerCursor<ChangeTrackingStore<byte[]>> {
	protected BPlusTreeByteKeysOnlyLeaf currentLeaf;
	protected byte[] keys;
	protected BPlusTreeByteKeysOnly tree;
	protected int bytesPerKey;
	protected byte[] key;
	
	public GeneralChangeTrackerScanCursor(DerivedRelation relation) {
		super(relation);
	}

	@Override
	public void initialize() {
		if (this.deltaSKeys == null) {
			this.currentLeaf = null;
		} else {
			this.tree = ((GeneralChangeTracker)this.deltaSKeys).getTree();
			this.currentLeaf = tree.getFirstChild();
		}
		
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.bytesPerKey = this.currentLeaf.getBytesPerKey();
			this.key = new byte[this.bytesPerKey];
		}
				
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
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				System.arraycopy(this.keys, this.keyIndex * this.bytesPerKey, this.key, 0, this.bytesPerKey);
				this.keyIndex++;
				return this.store.getTuple(this.key, tuple);				
			}

			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			if (this.currentLeaf != null) {
				this.keys = this.currentLeaf.getKeys();
				System.arraycopy(this.keys, 0, this.key, 0, this.bytesPerKey);
				this.keyIndex = 1;
				return this.store.getTuple(this.key, tuple);
			}
		}

		return 0;
	}

}
