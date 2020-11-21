package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues.AggregatorBPlusTreeLongKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues.AggregatorBPlusTreeLongKeysIntValuesLeaf;

public class AggregatorBPlusTreeLongKeysIntValuesRangeSearchResultScanCursor 
	extends AggregatorBPlusTreeLongKeysRangeSearchResultScanCursor<AggregatorBPlusTreeLongKeysIntValuesLeaf> {

	protected int[] values;

	public AggregatorBPlusTreeLongKeysIntValuesRangeSearchResultScanCursor(AggregateRelation relation) {
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
					this.adjustAfterInsert();
					// check that we didn't go past the leaf's keys
					if (this.keyIndex >= this.currentLeaf.getHighWaterMark())
						break;
				}
								
				this.loadKey();				
				// stop when reach end of range
				//if (this.endKey < this.currentKey) {
				if (this.hasReachedEndOfRange()) {
					this.reset();
					return 0;
				}
				
				//status = this.storageStructure.loadTuple(this.currentKey, this.values[this.keyIndex], tuple);
				status = this.loadTuple(tuple);
				this.keyIndex++;
				break;
			} else if (this.keyIndex > this.currentLeaf.getHighWaterMark()){
				this.adjustAfterSplitOrDelete();
				continue;
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
	protected long[] getKeys() {
		return this.currentLeaf.getKeys();
	}
	
	@Override
	protected int loadTuple(Tuple tuple) {
		return ((AggregatorBPlusTreeLongKeysIntValues)this.storageStructure).loadTuple(this.currentKey, this.values[this.keyIndex], tuple);
	}
	
	@Override
	public void reset() { 
		super.reset();
		this.values = null;
	}
}
