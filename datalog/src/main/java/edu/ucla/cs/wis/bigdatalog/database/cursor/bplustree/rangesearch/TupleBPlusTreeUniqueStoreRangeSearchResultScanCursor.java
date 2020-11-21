package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeGeneralLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public abstract class TupleBPlusTreeUniqueStoreRangeSearchResultScanCursor<L extends BPlusTreeGeneralLeaf<?>, T, T2>
	extends RangeSearchResultCursor<L, T, T2> 
	implements SelectionCursor<Tuple> {

	protected BPlusTreeTupleStore					tupleStore;
	protected TupleBPlusTreeStoreStructure 	storageStructure;
	
	protected byte[] 								value;
	protected byte[]								values;
	protected int 									bytesPerKey;
	protected int 									bytesPerValue;
	
	public TupleBPlusTreeUniqueStoreRangeSearchResultScanCursor(Relation relation) {
		super(relation);
	}

	@Override
	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		super.initialize(startLeaf, startIndex, keys);
		this.tupleStore = (BPlusTreeTupleStore) this.relation.getTupleStore();
		this.storageStructure = ((TupleBPlusTreeUniqueStore)this.relation.getTupleStore()).storageStructure;
		
		this.bytesPerKey = this.storageStructure.getBytesPerKey();
		this.bytesPerValue = this.storageStructure.getBytesPerValue();
		if (this.currentLeaf != null) {
			this.value = new byte[this.bytesPerValue];
			this.values = this.currentLeaf.getValues();
		}
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
				
				//System.arraycopy(this.keys, this.keyIndex * this.bytesPerKey, this.currentKey, 0, this.bytesPerKey);
				this.loadKey();
				//System.out.println("current key: " + Arrays.toString(this.currentKey));
				// stop when reach end of range
				//if (ByteArrayHelper.compare(this.endKey, this.currentKey, this.endKey.length) < 0) {
				if (this.hasReachedEndOfRange()) {
					//System.out.println("past end of range");
					this.reset();
					return 0;
				}
				
				System.arraycopy(this.values, this.keyIndex * this.bytesPerValue, this.value, 0, this.bytesPerValue);
				//status = this.tupleStore.loadTuple(this.currentKey, this.value, tuple);
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
	
	protected int getNextLeaf() {
		this.currentLeaf = (L) this.currentLeaf.getNext();
		if (this.currentLeaf == null)
			return 0;
	
		this.keyIndex = 0;
		this.keys = this.getKeys();
		this.values = this.currentLeaf.getValues();
		this.lastLeafSize = this.currentLeaf.getHighWaterMark();
		return 1;
	}

	@Override
	public void reset() {
		super.reset();
		this.values = null;
	}

	@Override
	public int[] getFilterColumns() { return null; }

	@Override
	public DbTypeBase[] getFilter() { return null; }
	
	private void printKeys() {
		System.out.print("start key: " + this.searchKeys.startKey + " | ");
		System.out.print("end key: " + this.searchKeys.endKey + "\n");
	}
		
	abstract protected void adjustAfterInsert();
	
	abstract protected void adjustAfterSplitOrDelete();

}