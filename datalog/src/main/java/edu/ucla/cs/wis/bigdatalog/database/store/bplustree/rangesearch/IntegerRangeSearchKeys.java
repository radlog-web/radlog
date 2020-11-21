package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch;

public class IntegerRangeSearchKeys extends RangeSearchKeys<Integer> {
	private static final long serialVersionUID = 1L;

	public IntegerRangeSearchKeys() { super(); }
	
	public IntegerRangeSearchKeys(Integer startKey, Integer endKey) {
		super(startKey, endKey);
	}
}
