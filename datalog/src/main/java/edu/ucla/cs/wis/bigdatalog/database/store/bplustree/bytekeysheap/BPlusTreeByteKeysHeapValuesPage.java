package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysheap;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;

public interface BPlusTreeByteKeysHeapValuesPage extends BPlusTreeElement {
	 public byte[] getLeftMostLeafKey();
	
	 public Heap get(byte[] key);
	
	 public void insert(byte[] key, byte[] data, BPlusTreeByteKeysHeapValuesInsertResult result);
	
	 public boolean delete(byte[] key);
	
	 public boolean delete(byte[] key, byte[] value);

	 public int commit();
}
