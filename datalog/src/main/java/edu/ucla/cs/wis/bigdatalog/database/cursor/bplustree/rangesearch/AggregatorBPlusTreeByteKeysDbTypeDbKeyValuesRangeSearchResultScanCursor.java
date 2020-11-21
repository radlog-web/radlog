package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues.AggregatorBPlusTreeByteKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues.AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesRangeSearchResultScanCursor 
	extends AggregatorBPlusTreeByteKeysRangeSearchResultScanCursor<AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf> {

	protected DbTypeBase[] totalValues;
	protected DbKeyValueStore[] keyValues;
	
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesRangeSearchResultScanCursor(AggregateRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		super.initialize(startLeaf, startIndex, keys);
		if (this.currentLeaf != null) {
			this.totalValues = this.currentLeaf.getTotalValues();
			this.keyValues = this.currentLeaf.getKeyValues();
		}
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
						this.lastLeafSize = this.currentLeaf.getHighWaterMark();
						// check that we didn't go past the leaf's keys
						if (this.keyIndex >= this.currentLeaf.getHighWaterMark())
							break;
					} else if (ByteArrayHelper.compare(this.tempKey, this.currentKey, this.bytesPerKey) > 0) {
						// otherwise, we need to move the cursor 
						this.keyIndex += (this.currentLeaf.getHighWaterMark() - lastLeafSize);
						this.lastLeafSize = this.currentLeaf.getHighWaterMark();
						// check that we didn't go past the leaf's keys
						if (this.keyIndex >= this.currentLeaf.getHighWaterMark())
							break;
					}
				}
					
				System.arraycopy(this.keys, this.keyIndex * this.bytesPerKey, this.currentKey, 0, this.bytesPerKey);
				// stop when reach end of range
				if (ByteArrayHelper.compare(this.endKey, this.currentKey, this.bytesPerKey) < 0) {
					this.reset();
					return 0;
				}
				
				status = this.storageStructure.loadTuple(this.currentKey, this.totalValues[this.keyIndex], 
							this.keyValues[this.keyIndex], tuple);
				this.keyIndex++;
				break;				
			}

			if (this.getNextLeaf() == 0)
				break;
		}
		
		return status;
	}
*/
	@Override
	protected int getNextLeaf() {
		if (super.getNextLeaf() == 1) {
			this.totalValues = this.currentLeaf.getTotalValues();
			this.keyValues = this.currentLeaf.getKeyValues();
			return 1;
		}
		return 0;
	}
	
	@Override
	protected byte[] getKeys() {
		return this.currentLeaf.getKeys();
	}
	
	@Override
	public int loadTuple(Tuple tuple) {
		return ((AggregatorBPlusTreeByteKeysDbTypeDbKeyValues)this.storageStructure).loadTuple(this.currentKey, this.totalValues[this.keyIndex], 
				this.keyValues[this.keyIndex], tuple);			
	}
	
	@Override
	public void reset() { 
		super.reset();
		this.totalValues = null;
		this.keyValues = null;
	}
}
