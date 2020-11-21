package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import java.util.ArrayList;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public interface BPlusTreeTupleStoreLeaf {
	public int getHighWaterMark();
	
	public ArrayList<Tuple>[] getTuples();
	
	public BPlusTreeTupleStoreLeaf getNext();
}
