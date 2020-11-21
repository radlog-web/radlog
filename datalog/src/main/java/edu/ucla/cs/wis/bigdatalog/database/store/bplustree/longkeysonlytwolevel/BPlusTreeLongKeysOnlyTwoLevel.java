package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonlytwolevel;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version only holds long keys

public class BPlusTreeLongKeysOnlyTwoLevel 
	extends BPlusTreeStoreValueStructure<BPlusTreeLongKeysOnlyTwoLevelPage, BPlusTreeLongKeysOnlyTwoLevelLeaf, BPlusTreeLongKeysOnlyTwoLevelNode>
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeLongKeysOnlyTwoLevelInsertResult insertResult;
	protected BPlusTreeLongKeysOnlyTwoLevelGetResult getResult;
	
	public BPlusTreeLongKeysOnlyTwoLevel() { super(); }
	
	public BPlusTreeLongKeysOnlyTwoLevel(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes, 
			TypeManager typeManager) {
		super(nodeSize, 8, 0, keyColumns, keyColumnTypes, new int[]{}, new DataType[]{}, typeManager);
		
		this.insertResult = new BPlusTreeLongKeysOnlyTwoLevelInsertResult();
		this.getResult = new BPlusTreeLongKeysOnlyTwoLevelGetResult();
		this.initialize();
	}
	
	@Override
	protected BPlusTreeLongKeysOnlyTwoLevelPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeLongKeysOnlyTwoLevelLeaf(this.nodeSize);
		
		return new BPlusTreeLongKeysOnlyTwoLevelNode(this.nodeSize);
	}
	
	public void insert(long key) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult, this.typeManager);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeLongKeysOnlyTwoLevelNode newRoot = (BPlusTreeLongKeysOnlyTwoLevelNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
			
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
	}
	
	public Long get(long key) {
		this.rootNode.get(key, this.getResult);
		if (this.getResult.success)
			return key;
		return null;
	}

	public boolean delete(long key) {
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
