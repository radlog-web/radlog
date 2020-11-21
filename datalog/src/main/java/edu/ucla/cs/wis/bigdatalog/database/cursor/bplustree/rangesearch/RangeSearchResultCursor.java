package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public abstract class RangeSearchResultCursor<L extends BPlusTreeLeaf<?>, T, T2> 
	extends Cursor<Tuple> {
	
	protected RangeSearchKeys<?> 					searchKeys;
	protected L 									currentLeaf;
	protected int 									keyIndex;
	protected T 									currentKey;
	protected T 									endKey;
	protected T2 									keys;
	protected int 									lastLeafSize;

	protected RangeSearchResultCursor(Relation<Tuple> relation) {
		super(relation);
	}

	public void initialize(BPlusTreeLeaf<?> startLeaf, int startIndex, RangeSearchKeys<?> keys) {
		this.searchKeys = keys;		
		this.currentLeaf = (L) startLeaf;
		this.keyIndex = startIndex;		
		this.endKey = (T)keys.endKey;
		this.lastLeafSize = 0;
		if (this.currentLeaf != null) {
			this.keys = this.getKeys();
			this.lastLeafSize = this.currentLeaf.getHighWaterMark();
		}
	}
	
	public boolean hasResults() { return (this.currentLeaf != null); }
	
	@Override
	public void reset() {
		this.currentLeaf = null;
		this.keyIndex = 0;
		this.keys = null;
		this.lastLeafSize = 0;
	}
	
	@Override
	public void reset(DbTypeBase[] keyColumns){	this.reset(); }
	
	@Override
	public void moveNext() { this.keyIndex++; }	

	abstract protected T2 getKeys();
	
	abstract public int getTuple(Tuple tuple);	
	
	abstract protected void loadKey();
	
	abstract protected boolean hasReachedEndOfRange();
	
	abstract protected int loadTuple(Tuple tuple);
	
	abstract protected int getNextLeaf();
}
