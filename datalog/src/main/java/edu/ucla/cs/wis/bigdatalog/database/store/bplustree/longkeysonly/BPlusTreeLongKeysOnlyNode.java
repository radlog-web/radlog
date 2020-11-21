package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeLongKeysOnlyNode 
	extends BPlusTreeNode<BPlusTreeLongKeysOnlyPage, BPlusTreeLongKeysOnlyLeaf> 
	implements BPlusTreeLongKeysOnlyPage, Serializable {
	private static final long serialVersionUID = 1L;

	protected long[] keys;
	
	public BPlusTreeLongKeysOnlyNode() { super(); }
	
	public BPlusTreeLongKeysOnlyNode(int nodeSize) {
		super(nodeSize, 8);
		
		this.keys = new long[this.numberOfKeys]; // M-1 keys
		this.children = new BPlusTreeLongKeysOnlyPage[this.getBranchingFactor()]; // M children
		this.numberOfEntries = 0;
	}

	@Override
	public long getLeftMostLeafKey() {
		return this.children[0].getLeftMostLeafKey();
	}

	@Override
	public void insert(long key, BPlusTreeLongKeysOnlyInsertResult result) {
		int insertAt = 0;

		for (insertAt = 0; insertAt < (this.highWaterMark - 1); insertAt++) {
			if (key < this.keys[insertAt])
				break;
		}
				
		this.children[insertAt].insert(key, result);
		if (result.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (result.newPage == null)
			return;

		// shift right for insertion to the left
		if ((insertAt + 1) < this.highWaterMark)
			System.arraycopy(this.children, insertAt + 1, this.children, insertAt + 2, this.highWaterMark - (insertAt + 1));				

		// shift right for insertion to the left
		if (insertAt < (this.highWaterMark - 1)) 
			System.arraycopy(this.keys, insertAt, this.keys, (insertAt + 1), (this.highWaterMark - 1 - insertAt));

		// we are inserting the page with the right 1/2 of the values from the previous full page (that was split)
		// therefore, it goes to the right of where we inserted
		insertAt++;
		this.children[insertAt] = result.newPage;
		this.keys[insertAt - 1] = this.children[insertAt].getLeftMostLeafKey();		
		this.highWaterMark++;

		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;
	}

	private BPlusTreeLongKeysOnlyPage split() {
		BPlusTreeLongKeysOnlyNode rightNode = new BPlusTreeLongKeysOnlyNode(this.nodeSize);
		// give the right 1/2 of the children to the new node
		int i;
		int splitPoint = (int) Math.ceil(((double)this.children.length) / 2);
		int numberToMove = this.children.length - splitPoint;
		for (i = 0; i < numberToMove; i++) {			
			rightNode.children[i] = this.children[splitPoint + i];
			this.children[splitPoint + i] = null;
			this.keys[splitPoint + i - 1] = 0;
			
			if (i > 0)
				rightNode.keys[i - 1] = rightNode.children[i].getLeftMostLeafKey();

			this.highWaterMark--;
		}
		
		rightNode.highWaterMark = i;
		return rightNode;
	}
	
	@Override
	public Long get(long key) {
		for (int i = 0; i < (this.highWaterMark - 1); i++) {
			if (key < this.keys[i])
				return this.children[i].get(key);
		}

		return this.children[this.highWaterMark - 1].get(key);
	}

	@Override
	public boolean delete(long key) {
		int deleteAt = 0;
		for (deleteAt = 0; deleteAt < (this.highWaterMark - 1); deleteAt++) {
			if (key < this.keys[deleteAt])
				break;
		}
		
		boolean status = this.children[deleteAt].delete(key);
		if (status) {
			this.numberOfEntries--;
			if (this.children[deleteAt].isEmpty()) {
				// remove child and key and shift remaining children 
				for (int j = deleteAt; j < this.highWaterMark; j++)
					this.children[j] = this.children[j + 1];
	
				for (int j = deleteAt; j < (this.highWaterMark - 1); j++)
					this.keys[j] = this.keys[j + 1];
	
				this.highWaterMark--;
			}				
		}
		return status;
	}
	
	@Override
	public void deleteAll() {
		for (int i = 0; i < this.highWaterMark; i++)
			this.children[i].deleteAll();

		this.keys = null;
		this.children = null;
		this.highWaterMark = 0;
		this.numberOfEntries = 0;
	}
	
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer += " ";
		
		output.append(buffer + "# of entries | Max # of entries: [" + this.highWaterMark + " | " + this.children.length + "] ");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Keys: [");
		for (int i = 0; i < this.keys.length; i++) {
			if (i > 0) output.append(", ");
			output.append(this.keys[i]);
		}
		output.append("]\n");
		output.append(buffer + "Entries:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				output.append("\n");
			output.append(buffer + this.children[i].toString(indent + 2));
		}
				
		return output.toString();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.highWaterMark * 8;
			allocated += this.keys.length * 8;
		}
		
		if (this.children != null) {
			MemoryMeasurement sizes;
			for (int i = 0; i < this.highWaterMark; i++) {
				sizes = this.children[i].getSizeOf();
				used += sizes.getUsed();
				allocated += sizes.getAllocated();
			}
		}
		return new MemoryMeasurement(used, allocated);
	}
}