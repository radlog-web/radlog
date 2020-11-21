package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeByteKeysOnly 
	extends BPlusTreeStoreStructure<BPlusTreeByteKeysOnlyPage, BPlusTreeByteKeysOnlyLeaf, BPlusTreeByteKeysOnlyNode>
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeByteKeysOnlyInsertResult insertResult;
	protected int[] keySortOrder;
	protected int[] bytesPerKeyColumn;
	protected TypeManager typeManager;
	
	public BPlusTreeByteKeysOnly() { super(); }
	
	public BPlusTreeByteKeysOnly(int nodeSize, int bytesPerKey, DataType[] keyColumnTypes, int[] keySortOrder, TypeManager typeManager) {
		super(nodeSize, bytesPerKey, BPlusTreeStoreStructure.getKeyColumns(keyColumnTypes), keyColumnTypes);
		this.insertResult = new BPlusTreeByteKeysOnlyInsertResult();
		if (keySortOrder == null || keySortOrder.length == 0)
			throw new DatabaseException("Sort keys and order must be provided to sort!");
		
		this.keySortOrder = keySortOrder;
		this.bytesPerKeyColumn = new int[this.keyColumns.length];
		for (int i = 0; i < this.keyColumns.length; i++)
			this.bytesPerKeyColumn[i] = this.keyColumnTypes[i].getNumberOfBytes();
		this.typeManager = typeManager;
		this.initialize();
	}

	@Override
	protected BPlusTreeByteKeysOnlyPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeByteKeysOnlyLeaf(this);
		
		return new BPlusTreeByteKeysOnlyNode(this);
	}
	
	public void insert(byte[] key) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult, this.typeManager);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage == null)
			return;

		// case 2 we have grow the tree as the root node was split 
		BPlusTreeByteKeysOnlyNode newRoot = (BPlusTreeByteKeysOnlyNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
		
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public byte[] get(byte[] key) {
		if (this.rootNode.isEmpty())
			return null;
		
		return this.rootNode.get(key);
	}
	
	public boolean delete(byte[] key) {
		if (this.rootNode.isEmpty())
			return false;
		
		boolean status = this.rootNode.delete(key);
		if (status) {
			this.numberOfEntries--;
			if (this.numberOfEntries == 0) {
				this.rootNode = null;
				this.rootNode = this.allocatePage();
			}
		}
		return status;
	}	
}
