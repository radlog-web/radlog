package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.AggregatorBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;

abstract public class AggregatorBPlusTreeRangeSearchResultScanCursor<L extends BPlusTreeLeaf<?>, T, T2> 
	extends RangeSearchResultCursor<L, T, T2> {

	protected AggregatorBPlusTreeStoreStructure<?, ?, ?> storageStructure;
	
	protected AggregatorBPlusTreeRangeSearchResultScanCursor(Relation<Tuple> relation) {
		super(relation);
	}
	
	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		super.initialize(startLeaf, startIndex, keys);
		this.storageStructure = (AggregatorBPlusTreeStoreStructure<?, ?, ?>) ((TupleAggregationStore)relation.getTupleStore()).storageStructure;		
	}
	
	protected int getNextLeaf() {
		this.currentLeaf = (L) this.currentLeaf.getNext();
		if (this.currentLeaf == null)
			return 0;
	
		this.keyIndex = 0;
		this.keys = this.getKeys();
		this.lastLeafSize = this.currentLeaf.getHighWaterMark();
		return 1;
	}
	
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
				if (this.hasReachedEndOfRange()) {
					this.reset();
					return 0;
				}
				
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
	
	abstract protected void adjustAfterInsert();
	
	abstract protected void adjustAfterSplitOrDelete();
}
