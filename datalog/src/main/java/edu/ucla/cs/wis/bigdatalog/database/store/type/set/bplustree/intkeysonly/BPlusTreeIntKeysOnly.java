package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.intkeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeStoreSetStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//this version of the b+tree is for the DbSet type

//THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeIntKeysOnly
	extends BPlusTreeStoreSetStructure<BPlusTreeIntKeysOnlyPage, BPlusTreeIntKeysOnlyLeaf, BPlusTreeIntKeysOnlyNode> 
	implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeIntKeysOnlyInsertResult insertResult;
	
	public BPlusTreeIntKeysOnly() { super(); }
	
	public BPlusTreeIntKeysOnly(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, 4, keyColumns, keyColumnTypes);
		this.insertResult = new BPlusTreeIntKeysOnlyInsertResult();
		this.initialize();
	}

	@Override	
	protected BPlusTreeIntKeysOnlyPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeIntKeysOnlyLeaf(this.nodeSize);
		
		return new BPlusTreeIntKeysOnlyNode(this.nodeSize);
	}
	
	public DbTypeBase insert(DbTypeBase key) {
		this.insert(((EncodedType)key).getKey(), this.insertResult);
		return key;
	}
	
	public void insert(int key, BPlusTreeIntKeysOnlyInsertResult result) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, result);
		if (result.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;

		if (result.newPage == null)
			return;

		// case 2 we have grow the tree as the root node was split 
		BPlusTreeIntKeysOnlyNode newRoot = (BPlusTreeIntKeysOnlyNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = result.newPage;
		newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();		
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public DbTypeBase get(DbTypeBase key) {
		if (this.get(((EncodedType)key).getKey()) == null)
			return null;
		return key;
	}

	public Integer get(int key) {
		if (this.rootNode == null)
			return null;
		
		return this.rootNode.get(key);
	}
	
	@Override
	public boolean delete(DbTypeBase key) {
		return this.delete(((EncodedType)key).getKey());
	}
	
	public boolean delete(int key) {
		if (this.rootNode == null)
			return false;
		
		boolean status = this.rootNode.delete(key); 
		if (status)
			this.numberOfEntries--;
		return status;		
	}	
	
	public void deleteAll() {
		if (this.rootNode == null)
			return;
			
		this.rootNode.deleteAll();
		this.rootNode = null;
		this.initialize();
	}
			
	@Override
	public MemoryMeasurement getSizeOf() {
		return rootNode.getSizeOf();
	} 
}
