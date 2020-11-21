package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysheap;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeMultiStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this b+tree has a heap in the leaf for storing multiple values per key
// this version of the b+tree could be used with:
// 1) unordered heap stores as an index
// 2) as storage - the first column is the key and the remaining n-1 columns are the data

public class BPlusTreeByteKeysHeapValues 
	extends BPlusTreeStoreValueStructure<BPlusTreeByteKeysHeapValuesPage, BPlusTreeByteKeysHeapValuesLeaf, BPlusTreeByteKeysHeapValuesNode> 
	implements TupleBPlusTreeMultiStoreStructure, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeByteKeysHeapValuesInsertResult insertResult;
	protected int[] keySortOrder;
	protected int[] bytesPerKeyColumn;
	
	public BPlusTreeByteKeysHeapValues() { super(); }
	
	public BPlusTreeByteKeysHeapValues(int nodeSize, int bytesPerKey, int bytesPerValue, int[] keyColumns, int[] keySortOrder,
			DataType[] keyColumnTypes, int[] valueColumns, DataType[] valueColumnTypes, TypeManager typeManager) {
		super(nodeSize, bytesPerKey, bytesPerValue, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, typeManager);

		this.insertResult = new BPlusTreeByteKeysHeapValuesInsertResult();
		
		if (keySortOrder == null || keySortOrder.length == 0)
			throw new DatabaseException("Sort keys and order must be provided to sort!");
		
		this.keySortOrder = keySortOrder;
		this.bytesPerKeyColumn = new int[this.keyColumns.length];
		for (int i = 0; i < this.keyColumns.length; i++)
			this.bytesPerKeyColumn[i] = this.keyColumnTypes[i].getNumberOfBytes();
		this.initialize();
	}
	
	@Override
	protected BPlusTreeByteKeysHeapValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeByteKeysHeapValuesLeaf(this);
		
		return new BPlusTreeByteKeysHeapValuesNode(this);
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
		this.rootNode.insert(key, data, this.insertResult, this.typeManager);
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
		return null;
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
