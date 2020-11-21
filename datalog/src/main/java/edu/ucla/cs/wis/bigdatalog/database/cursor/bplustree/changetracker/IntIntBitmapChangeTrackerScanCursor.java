package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker;

import java.util.BitSet;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.IntTreeIntBitmapChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues.BPlusTreeIntKeysBitmapValues;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues.BPlusTreeIntKeysBitmapValuesLeaf;

public class IntIntBitmapChangeTrackerScanCursor 
	extends ChangeTrackerCursor<ChangeTrackingStore<Long>> {	
	protected BPlusTreeIntKeysBitmapValuesLeaf currentLeaf;
	protected BPlusTreeIntKeysBitmapValues tree;
	protected int[] keys;
	protected BitSet[] values;
	protected BitSet lowerKeys;
	protected int lowerKeyIndex;
	
	public IntIntBitmapChangeTrackerScanCursor(DerivedRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize() {
		if (this.deltaSKeys == null) {
			this.currentLeaf = null;
		} else {
			this.tree = ((IntTreeIntBitmapChangeTracker)this.deltaSKeys).getTree();
			this.currentLeaf = tree.getFirstChild();
		}
		
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.values = this.currentLeaf.getValues();
		}

		this.keyIndex = 0;
		this.lowerKeyIndex = -1;
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
			int lowerKey;
			while (true) {
				if (this.lowerKeyIndex == -1) {
					if (this.keyIndex >= this.currentLeaf.getHighWaterMark()) {
						// out of keys, so move to next leaf
						this.currentLeaf = this.currentLeaf.getNext();
						
						if (this.currentLeaf == null)
							return 0;
						
						this.keys = this.currentLeaf.getKeys();
						this.keyIndex = 0;
						this.values = this.currentLeaf.getValues();						
						//return this.storageStructure.getTuple(this.keys[this.keyIndex++], tuple);
					}
					this.lowerKeyIndex = 0;
					this.lowerKeys = this.values[this.keyIndex++];
				}
							
				lowerKey = this.lowerKeys.nextSetBit(this.lowerKeyIndex);
				if (lowerKey > -1)
					break;
				
				this.lowerKeyIndex = -1;
				// if we reach here, we need to moe to the next bitset or next leaf
			}
	
			this.lowerKeyIndex = lowerKey + 1;
			
			long key = (((long)this.keys[this.keyIndex-1] << 32) | ((this.lowerKeyIndex - 1) & 0xffffffffL));
			return this.store.getTuple(key, tuple);
		}
		return 0;
	}

}
