package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysheap;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public interface BPlusTreeByteKeysHeapValuesPage extends BPlusTreeElement {
	 public byte[] getLeftMostLeafKey();
	
	 public Heap get(byte[] key);
	
	 public void insert(byte[] key, byte[] data, BPlusTreeByteKeysHeapValuesInsertResult result, TypeManager typeManager);
	
	 public boolean delete(byte[] key);
	
	 public boolean delete(byte[] key, byte[] value);

	 public int commit();
}
