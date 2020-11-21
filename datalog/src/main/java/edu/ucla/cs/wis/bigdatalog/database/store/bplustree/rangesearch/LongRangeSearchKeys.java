package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch;

public class LongRangeSearchKeys extends RangeSearchKeys<Long> {
	private static final long serialVersionUID = 1L;

	public LongRangeSearchKeys() { super(); }
	
	public LongRangeSearchKeys(long startKey, long endKey) {
		super(startKey, endKey);
	}

}
