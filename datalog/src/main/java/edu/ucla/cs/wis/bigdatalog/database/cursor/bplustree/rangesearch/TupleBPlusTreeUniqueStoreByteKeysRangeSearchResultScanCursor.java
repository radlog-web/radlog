package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysbytevalues.BPlusTreeByteKeysByteValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;

public class TupleBPlusTreeUniqueStoreByteKeysRangeSearchResultScanCursor 
	extends TupleBPlusTreeUniqueStoreRangeSearchResultScanCursor<BPlusTreeByteKeysByteValuesLeaf, byte[], byte[]> {

	private byte[] tempKey;
	
	public TupleBPlusTreeUniqueStoreByteKeysRangeSearchResultScanCursor(Relation relation) {
		super(relation);
	}

	@Override
	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		super.initialize(startLeaf, startIndex, keys);
		this.currentKey = new byte[this.bytesPerKey];
		this.tempKey = new byte[this.bytesPerKey];
		/*
		this.tupleStore = (BPlusTreeTupleStore) this.relation.getTupleStore();
		this.storageStructure = ((TupleBPlusTreeUniqueStore)this.relation.getTupleStore()).storageStructure;
		this.currentLeaf = (BPlusTreeByteKeysByteValuesLeaf) startLeaf;
		this.keyIndex = startIndex;
		this.searchKeys = keys;
		this.endKey = (byte[])keys.endKey;
		this.bytesPerKey = this.storageStructure.getBytesPerKey();
		this.bytesPerValue = this.storageStructure.getBytesPerValue();
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.values = this.currentLeaf.getValues();
			this.lastLeafSize = this.currentLeaf.getHighWaterMark();
			this.currentKey = new byte[this.bytesPerKey];
			this.value = new byte[this.bytesPerValue];
			this.tempKey = new byte[this.bytesPerKey];
		}
		this.printKeys();*/
	}
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
				
				System.arraycopy(this.keys, this.keyIndex * this.bytesPerKey, this.currentKey, 0, this.bytesPerKey);
				System.out.println("current key: " + Arrays.toString(this.currentKey));
				// stop when reach end of range
				if (ByteArrayHelper.compare(this.endKey, this.currentKey, this.endKey.length) < 0) {
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
	protected byte[] getKeys() {
		return this.currentLeaf.getKeys();
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
	protected int loadTuple(Tuple tuple) {
		return this.tupleStore.loadTuple(this.currentKey, this.value, tuple);
	}
}