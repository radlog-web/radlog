package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysheap;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeMultiStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this b+tree has a heap in the leaf for storing multiple values per key
// this version of the b+tree could be used with:
// 1) unordered heap stores as an index
// 2) as storage - the first column is the key and the remaining n-1 columns are the data

public class BPlusTreeByteKeysHeapValues 
	extends BPlusTreeStoreValueStructure<BPlusTreeByteKeysHeapValuesPage, BPlusTreeByteKeysHeapValuesLeaf, BPlusTreeByteKeysHeapValuesNode> 
	implements TupleBPlusTreeMultiStoreStructure, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected boolean useOrderedHeap;
	protected BPlusTreeByteKeysHeapValuesInsertResult insertResult;
	
	public BPlusTreeByteKeysHeapValues() { super(); }
	
	public BPlusTreeByteKeysHeapValues(int nodeSize, int bytesPerKey, int bytesPerValue, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, boolean useOrderedHeap, TypeManager typeManager) {
		super(nodeSize, bytesPerKey, bytesPerValue, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, typeManager);

		this.useOrderedHeap = useOrderedHeap;
		this.insertResult = new BPlusTreeByteKeysHeapValuesInsertResult();
		this.initialize();
	}
	
	@Override
	protected BPlusTreeByteKeysHeapValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeByteKeysHeapValuesLeaf(this.nodeSize, this.bytesPerKey, this.bytesPerValue, this.useOrderedHeap);
		
		return new BPlusTreeByteKeysHeapValuesNode(this.nodeSize, this.bytesPerKey, this.bytesPerValue, this.useOrderedHeap);
	}
	
	@Override
	public void insert(Tuple tuple) {
		byte[] key = this.getKey(tuple.columns);
		byte[] data = this.getBytes(tuple.columns);
		this.insert(key, data);
	}
		
	public void insert(DbTypeBase[] keyColumns, byte[] data) {
		byte[] key = this.getKey(keyColumns);
		this.insert(key, data);
	}
		
	public void insert(byte[] key, byte[] data) {		
		// case 1 - room to insert in root node
		this.rootNode.insert(key, data, this.insertResult);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage == null)
			return;

		// case 2 we have grow the tree as the root node was split 
		BPlusTreeByteKeysHeapValuesNode newRoot = (BPlusTreeByteKeysHeapValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
		
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	@Override
	public Heap get(DbTypeBase[] keyColumns) {
		byte[] key = this.getKey(keyColumns);
		return this.get(key);
	}
	
	public Heap get(byte[] key) {
		if (this.rootNode.isEmpty())
			return null;
		
		return this.rootNode.get(key);
	}
	
	@Override
	public boolean delete(Tuple tuple) {
		return this.delete(this.getKey(tuple.columns));
	}
	
	public boolean delete(byte[] key) {
		if (this.rootNode.isEmpty())
			return false;
		
		boolean status = this.rootNode.delete(key);
		if (status)
			this.numberOfEntries--;
		return status;
	}	
	
	public boolean delete(byte[] key, byte[] value) {
		if (this.rootNode.isEmpty())
			return false;
		
		boolean status = this.rootNode.delete(key, value);
		if (status) {
			this.numberOfEntries--;
			if (this.numberOfEntries == 0) {
				this.rootNode = null;
				this.rootNode = this.allocatePage();
			}
		}
		return status;
	}
	
	public int commit() {
		if (this.rootNode.isEmpty())
			return 0;
			
		return this.rootNode.commit();
	}
}
