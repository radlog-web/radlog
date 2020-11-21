package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysbytevalues.BPlusTreeLongKeysByteValuesLeaf;

public class TupleBPlusTreeUniqueStoreLongKeysRangeSearchResultScanCursor 
	extends TupleBPlusTreeUniqueStoreRangeSearchResultScanCursor<BPlusTreeLongKeysByteValuesLeaf, Long, long[]> 
	implements SelectionCursor<Tuple> {
	
	public TupleBPlusTreeUniqueStoreLongKeysRangeSearchResultScanCursor(Relation relation) {
		super(relation);
	}
/*
	@Override
	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		super.initialize(startLeaf, startIndex, keys);
		this.tupleStore = (BPlusTreeTupleStore)this.relation.getTupleStore();
		this.storageStructure = ((TupleBPlusTreeUniqueStore)this.relation.getTupleStore()).storageStructure;
		this.currentLeaf = (BPlusTreeLongKeysByteValuesLeaf) startLeaf;
		this.keyIndex = startIndex;
		this.searchKeys = keys;
		this.endKey = (long)keys.endKey;
		this.bytesPerValue = this.storageStructure.getBytesPerValue();
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.values = this.currentLeaf.getValues();
			this.lastLeafSize = this.currentLeaf.getHighWaterMark();
			this.value = new byte[this.bytesPerValue];
		}		
		this.printKeys();
	}*/
/*
	@Override
	public int getTuple(Tuple tuple) {
		int status = 0;
		while (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				// before we do anything, make sure this leaft was not inserted into after the last read
				// this could move the cursor and us read a tuple we've already read
				if (this.currentLeaf.getHighWaterMark() > this.lastLeafSize) {
					this.adjustAfterInsert();
					// check that we didn't go past the leaf's keys
					if (this.keyIndex >= this.currentLeaf.getHighWaterMark())
						break;
				}
				
				this.currentKey = this.keys[this.keyIndex];
				System.out.println("current key: " + this.currentKey);
				// stop when reach end of range
				if (this.endKey < this.currentKey) {
					System.out.println("past end of range");
					this.reset();
					return 0;
				}
				
				System.arraycopy(this.values, this.keyIndex * this.bytesPerValue, this.value, 0, this.bytesPerValue);
				status = this.tupleStore.loadTuple(this.currentKey, this.value, tuple);
				this.keyIndex++;
				break;
			} else if (this.keyIndex > this.currentLeaf.getHighWaterMark()){
				this.adjustAfterSplitOrDelete();
				continue;
			}

			// out of keys, so move to next leaf
			this.getNextLeaf();
			if (this.currentLeaf == null)
				break;
		}
		return status;
	}
	
	private void getNextLeaf() {
		this.currentLeaf = this.currentLeaf.getNext();
		if (this.currentLeaf == null)
			return;
	
		this.keyIndex = 0;
		this.keys = this.currentLeaf.getKeys();
		this.values = this.currentLeaf.getValues();
		this.lastLeafSize = this.currentLeaf.getHighWaterMark();
	}
*/
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
			if (this.getNextLeaf() == 0)
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
	protected long[] getKeys() {
		return this.currentLeaf.getKeys();
	}
	
	@Override
	protected boolean hasReachedEndOfRange() {
		return (this.endKey < this.currentKey);
	}
	
	@Override
	protected void loadKey() {
		this.currentKey = this.keys[this.keyIndex];
	}
	
	@Override
	protected int loadTuple(Tuple tuple) {
		return this.tupleStore.loadTuple(this.currentKey, this.value, tuple);
	}
}