package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;

abstract public class AggregatorBPlusTreeByteKeysRangeSearchResultScanCursor<L extends BPlusTreeLeaf<?>>
	extends AggregatorBPlusTreeRangeSearchResultScanCursor<L, byte[], byte[]> {

	protected int bytesPerKey;
	private byte[] tempKey;

	protected AggregatorBPlusTreeByteKeysRangeSearchResultScanCursor(Relation<Tuple> relation) {
		super(relation);
	}
	
	@Override
	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		super.initialize(startLeaf, startIndex, keys);
		if (this.currentLeaf != null) {
			this.bytesPerKey = this.currentLeaf.getBytesPerKey();
			this.currentKey = new byte[this.bytesPerKey];
			this.tempKey = new byte[this.bytesPerKey];
		}
	}
	
	@Override
	protected void adjustAfterInsert() {
		System.arraycopy(this.keys, (this.keyIndex - 1) * this.bytesPerKey, this.tempKey, 0, this.bytesPerKey);
		// if the position of the changes has not been reached yet, we're ok
		if (ByteArrayHelper.compare(this.tempKey, this.currentKey, this.bytesPerKey) < 0) {
			while (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				// ignore keys added before the last used key by moving index past it
				System.arraycopy(this.keys, this.keyIndex * this.bytesPerKey, this.tempKey, 0, this.bytesPerKey);
				if (ByteArrayHelper.compare(this.tempKey, this.currentKey, this.bytesPerKey) > 0)
					break;
				this.keyIndex++;
			}
		} else if (ByteArrayHelper.compare(this.tempKey, this.currentKey, this.bytesPerKey) > 0) {
			// otherwise, we need to move the cursor 
			this.keyIndex += (this.currentLeaf.getHighWaterMark() - this.lastLeafSize);
		}
		this.lastLeafSize = this.currentLeaf.getHighWaterMark();
	}
	
	@Override
	protected void adjustAfterSplitOrDelete() {
		int newIndex = -1;
		int index = 0;
		while (index < this.currentLeaf.getHighWaterMark()) {
			System.arraycopy(this.keys, index * this.bytesPerKey, this.tempKey, 0, this.bytesPerKey);
			if (ByteArrayHelper.compare(this.tempKey, this.currentKey, this.bytesPerKey) == 0) {
				newIndex = index;
				break;
			}
			index++;
		}

		if (newIndex == -1) {
			// could have been a split, and we need to check the new leaf			
			if (this.getNextLeaf() == 0)
				return;
			
			index = 0;
			while (index < this.currentLeaf.getHighWaterMark()) {
				System.arraycopy(this.keys, index * this.bytesPerKey, this.tempKey, 0, this.bytesPerKey);
				if (ByteArrayHelper.compare(this.tempKey, this.currentKey, this.bytesPerKey) == 0) {
					newIndex = index;
					break;
				}
				index++;
			}			
		}

		if (newIndex > -1)
			this.keyIndex = newIndex + 1;		
	}
		
	@Override
	protected boolean hasReachedEndOfRange() {
		return (ByteArrayHelper.compare(this.endKey, this.currentKey, this.endKey.length) < 0);
	}
	
	@Override
	protected void loadKey() {
		System.arraycopy(this.keys, this.keyIndex * this.bytesPerKey, this.currentKey, 0, this.bytesPerKey);
	}
	
	@Override
	public void reset() {
		super.reset();
		this.bytesPerKey = 0;
		this.tempKey = null;
	}
}
