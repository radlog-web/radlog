package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch;

public class ByteRangeSearchKeys extends RangeSearchKeys<byte[]> {
	private static final long serialVersionUID = 1L;

	public ByteRangeSearchKeys(){ super(); }
	
	public ByteRangeSearchKeys(byte[] startKey, byte[] endKey) {
		super(startKey, endKey);
	}
}
