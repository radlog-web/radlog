package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypevalues.AggregatorBPlusTreeIntKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypevalues.AggregatorBPlusTreeIntKeysDbTypeValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeIntKeysDbTypeValuesRangeSearchResultScanCursor 
	extends AggregatorBPlusTreeIntKeysRangeSearchResultScanCursor<AggregatorBPlusTreeIntKeysDbTypeValuesLeaf> {

	protected DbTypeBase[] values;

	public AggregatorBPlusTreeIntKeysDbTypeValuesRangeSearchResultScanCursor(AggregateRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		super.initialize(startLeaf, startIndex, keys);
		if (this.currentLeaf != null)
			this.values = this.currentLeaf.getValues();			
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
					// if the position of the changes has not been reached yet, we're ok
					if (this.keys[this.keyIndex - 1] < this.currentKey) {
						// ignore keys added before the last used key by moving index past it
						while (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
							if (this.keys[this.keyIndex] > this.currentKey)
								break;
							this.keyIndex++;
						}
						this.lastLeafSize = this.currentLeaf.getHighWaterMark();
						if (this.keyIndex >= this.currentLeaf.getHighWaterMark())
							break;						
					} else if (this.keys[this.keyIndex - 1] > this.currentKey) {
						// otherwise, we need to move the cursor 
						this.keyIndex += (this.currentLeaf.getHighWaterMark() - lastLeafSize);
						this.lastLeafSize = this.currentLeaf.getHighWaterMark();
						// check that we didn't go past the leaf's keys
						if (this.keyIndex >= this.currentLeaf.getHighWaterMark())
							break;
					}
				}
				this.currentKey = this.keys[this.keyIndex];
				// stop when reach end of range
				if (this.endKey < this.currentKey) {
					this.reset();
					return 0;
				}
				status = this.storageStructure.loadTuple(this.currentKey, this.values[this.keyIndex], tuple);
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
			this.values = this.currentLeaf.getValues();
			return 1;
		}
		return 0;
	}
	
	@Override
	protected int[] getKeys() {
		return this.currentLeaf.getKeys();
	}
	
	@Override
	public int loadTuple(Tuple tuple) {
		return ((AggregatorBPlusTreeIntKeysDbTypeValues)this.storageStructure).loadTuple(this.currentKey, this.values[this.keyIndex], tuple);
	}
	
	@Override
	public void reset() { 
		super.reset();
		this.values = null;
	}
}
