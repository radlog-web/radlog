package edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeIntKeysBitmapValuesNode 
	extends BPlusTreeNode<BPlusTreeIntKeysBitmapValuesPage, BPlusTreeIntKeysBitmapValuesLeaf> 
	implements BPlusTreeIntKeysBitmapValuesPage, Serializable {
	private static final long serialVersionUID = 1L;

	protected int[] keys;
	
	public BPlusTreeIntKeysBitmapValuesNode() { super(); }
	
	public BPlusTreeIntKeysBitmapValuesNode(int nodeSize) {
		super(nodeSize, 4);
		
		this.highWaterMark = 0;
		this.keys = new int[this.numberOfKeys]; // M-1 keys
		this.children = new BPlusTreeIntKeysBitmapValuesPage[this.getBranchingFactor()]; // M children
		this.numberOfEntries = 0;
	}

	@Override
	public int getLeftMostLeafKey() {
		return this.children[0].getLeftMostLeafKey();
	}

	@Override
	public void insert(long key, BPlusTreeIntKeysBitmapValuesInsertResult result) {
		int upperKey = (int)(key >> 32);
		int insertAt = 0;
		for (insertAt = 0; insertAt < (this.highWaterMark - 1); insertAt++) {
			if (upperKey < this.keys[insertAt])
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

	private BPlusTreeIntKeysBitmapValuesPage split() {
		BPlusTreeIntKeysBitmapValuesNode rightNode = new BPlusTreeIntKeysBitmapValuesNode(this.nodeSize);
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
	public void get(long key, BPlusTreeIntKeysBitmapValuesGetResult result) {
		int upperKey = (int)(key >> 32);
		for (int i = 0; i < (this.highWaterMark - 1); i++) {
			if (upperKey < this.keys[i]) {
				this.children[i].get(key, result);
				return;
			}
		}
		
		this.children[this.highWaterMark - 1].get(key, result);
	}

	@Override
	public boolean delete(long key) {
		int upperKey = (int)(key >> 32);
		int deleteAt = 0;
		for (deleteAt = 0; deleteAt < (this.highWaterMark - 1); deleteAt++) {
			if (upperKey < this.keys[deleteAt])
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
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer += " ";
		
		retval.append(buffer + "# of entries | Max # of entries: [" + this.highWaterMark + " | " + this.children.length + "] ");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Keys: [");
		for (int i = 0; i < this.keys.length; i++) {
			if (i > 0) retval.append(", ");
			retval.append(this.keys[i]);
		}
		retval.append("]\n");
		retval.append(buffer + "Entries:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append("\n");
			retval.append(buffer + this.children[i].toString(indent + 2));
		}
				
		return retval.toString();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.highWaterMark * 4;
			allocated += this.keys.length * 4;
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