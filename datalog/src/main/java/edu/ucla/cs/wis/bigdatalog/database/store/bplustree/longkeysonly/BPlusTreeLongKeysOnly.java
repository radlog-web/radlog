package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.raw.BPlusTreeLongKeysOnlyCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version only holds long keys

//THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeLongKeysOnly 
	extends BPlusTreeStoreStructure<BPlusTreeLongKeysOnlyPage, BPlusTreeLongKeysOnlyLeaf, BPlusTreeLongKeysOnlyNode>
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeLongKeysOnlyInsertResult insertResult;
	
	public BPlusTreeLongKeysOnly() { super(); }
	
	public BPlusTreeLongKeysOnly(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, 8, keyColumns, keyColumnTypes);
		
		this.insertResult = new BPlusTreeLongKeysOnlyInsertResult();
		this.initialize();
	}
	
	@Override
	protected BPlusTreeLongKeysOnlyPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeLongKeysOnlyLeaf(this.nodeSize);
		
		return new BPlusTreeLongKeysOnlyNode(this.nodeSize);
	}
	
	public void insert(long key) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeLongKeysOnlyNode newRoot = (BPlusTreeLongKeysOnlyNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
			
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
	}
	
	public void insert(long key, BPlusTreeLongKeysOnlyInsertResult insertResult) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, insertResult);
		if (insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (insertResult.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeLongKeysOnlyNode newRoot = (BPlusTreeLongKeysOnlyNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = insertResult.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
			
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
	}
	
	public Long get(long key) {
		return this.rootNode.get(key);
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
	
	public void merge(BPlusTreeLongKeysOnly other, boolean print) {
		if (other.isEmpty())
			return;
		if (print) {
			System.out.println("this:" + this.toStringShort());
			System.out.println("other:" + other.toStringShort());
		}
				
		BPlusTreeLongKeysOnlyCursor otherCursor = new BPlusTreeLongKeysOnlyCursor(other);
		BPlusTreeLongKeysOnlyCursor.Result otherResult = otherCursor.new Result();
		int otherStatus = otherCursor.get(otherResult);
		
		while (otherStatus == 0) {
			this.insert(otherResult.value);
			otherStatus = otherCursor.get(otherResult);			
		}
		if (print)
			System.out.println("merge:" + this.toStringShort());
	}
	
	public BPlusTreeLongKeysOnly intersect(BPlusTreeLongKeysOnly other, boolean print) {
		if (this.isEmpty() || other.isEmpty())
			return null;
		
		BPlusTreeLongKeysOnly intersection = new BPlusTreeLongKeysOnly(this.nodeSize, this.keyColumns, this.keyColumnTypes/*, 
				this.deALSConfiguration, this.typeManager*/);
				
		BPlusTreeLongKeysOnlyCursor thisCursor = new BPlusTreeLongKeysOnlyCursor(this);
		BPlusTreeLongKeysOnlyCursor otherCursor = new BPlusTreeLongKeysOnlyCursor(other);
		BPlusTreeLongKeysOnlyCursor.Result thisResult = thisCursor.new Result();
		BPlusTreeLongKeysOnlyCursor.Result otherResult = otherCursor.new Result();
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
	
	public BPlusTreeLongKeysOnly difference(BPlusTreeLongKeysOnly other, boolean print) {
		if (this.isEmpty() || other == null || other.isEmpty())
			return null;
		
		BPlusTreeLongKeysOnly difference = new BPlusTreeLongKeysOnly(this.nodeSize, this.keyColumns, this.keyColumnTypes/*, 
				this.deALSConfiguration, this.typeManager*/);
				
		BPlusTreeLongKeysOnlyCursor thisCursor = new BPlusTreeLongKeysOnlyCursor(this);
		BPlusTreeLongKeysOnlyCursor otherCursor = new BPlusTreeLongKeysOnlyCursor(other);
		BPlusTreeLongKeysOnlyCursor.Result thisResult = thisCursor.new Result();
		BPlusTreeLongKeysOnlyCursor.Result otherResult = otherCursor.new Result();
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
