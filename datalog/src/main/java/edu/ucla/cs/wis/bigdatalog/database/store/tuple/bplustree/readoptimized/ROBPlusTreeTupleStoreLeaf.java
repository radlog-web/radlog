package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public interface ROBPlusTreeTupleStoreLeaf {
	public int getHighWaterMark();
	
	public Tuple[][] getTuples();
	
	public ROBPlusTreeTupleStoreLeaf getNext();
}
