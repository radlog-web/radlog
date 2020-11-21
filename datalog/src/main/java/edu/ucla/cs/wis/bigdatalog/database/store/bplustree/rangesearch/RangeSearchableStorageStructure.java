package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch;

public interface RangeSearchableStorageStructure<T> {
	
	public int getBytesPerKey();

	public void getTuple(T startKey, T endKey, RangeSearchResult result);

}
