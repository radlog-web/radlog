package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.raw.BPlusTreeIntKeysOnlyCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version only holds integer keys
// used for storing single column relations

//THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeIntKeysOnly 
	extends BPlusTreeStoreStructure<BPlusTreeIntKeysOnlyPage, BPlusTreeIntKeysOnlyLeaf, BPlusTreeIntKeysOnlyNode>
	implements Serializable {
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
		
	public void insert(int key) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeIntKeysOnlyNode newRoot = (BPlusTreeIntKeysOnlyNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
			
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
	}
	
	public void insert(int key, BPlusTreeIntKeysOnlyInsertResult result) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, result);
		if (result.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (result.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeIntKeysOnlyNode newRoot = (BPlusTreeIntKeysOnlyNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = result.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
			
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
	}
	
	public Integer get(int key) {		
		return this.rootNode.get(key);
	}
	
	public boolean delete(int key) {
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
	
	public void merge(BPlusTreeIntKeysOnly other, boolean print) {
		if (other.isEmpty())
			return;
		if (print) {
			System.out.println("this:" + this.toStringShort());
			System.out.println("other:" + other.toStringShort());
		}
				
		BPlusTreeIntKeysOnlyCursor otherCursor = new BPlusTreeIntKeysOnlyCursor(other);
		BPlusTreeIntKeysOnlyCursor.Result otherResult = otherCursor.new Result();
		int otherStatus = otherCursor.get(otherResult);
		
		while (otherStatus == 0) {
			this.insert(otherResult.value);
			otherStatus = otherCursor.get(otherResult);			
		}
		if (print)
			System.out.println("merge:" + this.toStringShort());
	}
	
	public BPlusTreeIntKeysOnly intersect(BPlusTreeIntKeysOnly other, boolean print) {
		if (this.isEmpty() || other.isEmpty())
			return null;
		
		BPlusTreeIntKeysOnly intersection = new BPlusTreeIntKeysOnly(this.nodeSize, this.keyColumns, this.keyColumnTypes/*, 
				this.deALSConfiguration, this.typeManager*/);
				
		BPlusTreeIntKeysOnlyCursor thisCursor = new BPlusTreeIntKeysOnlyCursor(this);
		BPlusTreeIntKeysOnlyCursor otherCursor = new BPlusTreeIntKeysOnlyCursor(other);
		BPlusTreeIntKeysOnlyCursor.Result thisResult = thisCursor.new Result();
		BPlusTreeIntKeysOnlyCursor.Result otherResult = otherCursor.new Result();
		int thisStatus = thisCursor.get(thisResult);
		int otherStatus = otherCursor.get(otherResult);
			
		while ((thisStatus == 0) && (otherStatus == 0)) {
			// we did not find thisResult.value in other
			if (thisResult.value < otherResult.value) {
				thisStatus = thisCursor.get(thisResult);
			} 
			// we did not find otherResult.value in this
			else if (otherResult.value < thisResult.value) {
				otherStatus = otherCursor.get(otherResult);
			}
			// both contain the item, insert to intersection and move both cursors forward
			else {
				intersection.insert(thisResult.value);
				thisStatus = thisCursor.get(thisResult);
				otherStatus = otherCursor.get(otherResult);
			}
		}

		if (print) {
			System.out.println("this:" + this.toStringShort());
			System.out.println("other:" + other.toStringShort());
			System.out.println("intersection:" + intersection.toStringShort());
		}
		
		return intersection;
	}
	
	public BPlusTreeIntKeysOnly difference(BPlusTreeIntKeysOnly other, boolean print) {
		if (this.isEmpty() || other == null || other.isEmpty())
			return null;
		
		BPlusTreeIntKeysOnly difference = new BPlusTreeIntKeysOnly(this.nodeSize, this.keyColumns, this.keyColumnTypes/*, 
				this.deALSConfiguration, this.typeManager*/);
				
		BPlusTreeIntKeysOnlyCursor thisCursor = new BPlusTreeIntKeysOnlyCursor(this);
		BPlusTreeIntKeysOnlyCursor otherCursor = new BPlusTreeIntKeysOnlyCursor(other);
		BPlusTreeIntKeysOnlyCursor.Result thisResult = thisCursor.new Result();
		BPlusTreeIntKeysOnlyCursor.Result otherResult = otherCursor.new Result();
		int thisStatus = thisCursor.get(thisResult);
		int otherStatus = otherCursor.get(otherResult);
			
		while ((thisStatus == 0) && (otherStatus == 0)) {
			// we did not find thisResult.value in other, so add to difference
			if (thisResult.value < otherResult.value) {
				difference.insert(thisResult.value);
				thisStatus = thisCursor.get(thisResult);
			} 
			// we did not find otherResult.value in this
			else if (otherResult.value < thisResult.value) {
				otherStatus = otherCursor.get(otherResult);
			}
			// we made a match, so move both cursors forward
			else {
				thisStatus = thisCursor.get(thisResult);
				otherStatus = otherCursor.get(otherResult);
			}
		}
		
		// other has been exhausted so everything left from this can be put in to output tree
		while (thisStatus == 0) {
			difference.insert(thisResult.value);
			thisStatus = thisCursor.get(thisResult);			
		}

		if (print) {
			System.out.println("this:" + this.toStringShort());
			System.out.println("other:" + other.toStringShort());
			System.out.println("difference:" + difference.toStringShort());
		}
		
		return difference;
	}
}
