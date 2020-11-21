package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;

abstract public class AggregatorBPlusTreeLongKeysRangeSearchResultScanCursor<L extends BPlusTreeLeaf<?>>
	extends AggregatorBPlusTreeRangeSearchResultScanCursor<L, Long, long[]> {

	protected AggregatorBPlusTreeLongKeysRangeSearchResultScanCursor(Relation<Tuple> relation) {
		super(relation);
	}

	@Override
	protected void adjustAfterInsert() {
		// if the position of the changes has not been reached yet, we're ok
		if (this.keys[this.keyIndex - 1] < this.currentKey) {
			// ignore keys added before the last used key by moving index past it
			while (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				if (this.keys[this.keyIndex] > this.currentKey)
					break;
				//System.out.println("skipping key: " + this.keys[this.keyIndex]);
				this.keyIndex++;
			}					
		} else if (this.keys[this.keyIndex - 1] > this.currentKey) {
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
			if (this.keys[index] == this.currentKey) {
				newIndex = index;
				break;
			}
			index++;
		}

		if (newIndex == -1) {
			// could have been a split, and we need to check the new leaf 
			this.getNextLeaf();
			if (this.currentLeaf == null)
				return;
			
			index = 0;
			while (index < this.currentLeaf.getHighWaterMark()) {
				if (this.keys[index] == this.currentKey) {
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
		return (this.endKey < this.currentKey);
	}
	
	@Override
	protected void loadKey() {
		this.currentKey = this.keys[this.keyIndex];
	}
}
