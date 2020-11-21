package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;

public interface TupleBPlusTreeMultiStoreStructure 
	extends TupleBPlusTreeStoreStructure<Heap> {	
	
	public int commit();

}
