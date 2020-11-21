package edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version only holds long keys and the second of the two integers must be positive 
// used for storing single column relations

// B+ tree implementation with help from
//  http://www.cs.washington.edu/education/courses/cse326/08sp/lectures/11-b-trees.pdf
//  and http://ozark.hendrix.edu/~burch/cs/340/reading/btree/index.html
public class BPlusTreeIntKeysBitmapValues 
	extends BPlusTreeStoreValueStructure<BPlusTreeIntKeysBitmapValuesPage, BPlusTreeIntKeysBitmapValuesLeaf, BPlusTreeIntKeysBitmapValuesNode>
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeIntKeysBitmapValuesInsertResult insertResult;
	protected BPlusTreeIntKeysBitmapValuesGetResult getResult;
	
	public BPlusTreeIntKeysBitmapValues() { super(); }
	
	public BPlusTreeIntKeysBitmapValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes, 
			TypeManager typeManager) {
		super(nodeSize, 4, 0, keyColumns, keyColumnTypes, new int[]{}, new DataType[]{}, typeManager);
		
		this.insertResult = new BPlusTreeIntKeysBitmapValuesInsertResult();
		this.getResult = new BPlusTreeIntKeysBitmapValuesGetResult();
		this.initialize();
	}
		
	@Override
	protected BPlusTreeIntKeysBitmapValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeIntKeysBitmapValuesLeaf(this.nodeSize);
		
		return new BPlusTreeIntKeysBitmapValuesNode(this.nodeSize);
	}
	
	public void insert(long key) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeIntKeysBitmapValuesNode newRoot = (BPlusTreeIntKeysBitmapValuesNode)this.allocatePage();
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
		if (status)
			this.numberOfEntries--;
		return status;
	}	
}
