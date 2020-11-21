package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreePage;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class LongKeyBPlusTreeKeyIndexNode 
	extends BPlusTreePage 
	implements LongKeyBPlusTreeKeyIndexPage, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;

	protected long[] keys;
	protected LongKeyBPlusTreeKeyIndexPage[] children;
	protected int numberOfEntries;
	
	public LongKeyBPlusTreeKeyIndexNode(int nodeSize) {
		super(nodeSize, 8);

		this.keys = new long[this.numberOfKeys]; // M-1 keys
		this.children = new LongKeyBPlusTreeKeyIndexPage[this.getBranchingFactor()]; // M children
		this.numberOfEntries = 0;
	}
	
	@Override
	public int getHeight() {
		if (this.isEmpty())
			return 1;
		
		// all levels at same height in the tree
		return this.children[0].getHeight() + 1;
	}
	
	@Override
	public boolean isEmpty() {
		return (this.highWaterMark == 0);
	}
	
	@Override
	public boolean hasOverflow() {		
		return (this.highWaterMark == this.children.length); 
	}
	
	public int getNumberOfEntries() { return this.numberOfEntries; }

	@Override
	public long getLeftMostLeafKey() {
		return this.children[0].getLeftMostLeafKey();
	}

	@Override
	public void insert(long key, LongKeyBPlusTreeKeyIndexResult result) {
		int insertAt = 0;
		for (insertAt = 0; insertAt < (this.highWaterMark - 1); insertAt++) {
			if (key < this.keys[insertAt])
				break;
		}

		this.children[insertAt].insert(key, result);
		if (result.success)
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

	private LongKeyBPlusTreeKeyIndexPage split() {
		LongKeyBPlusTreeKeyIndexNode rightNode = new LongKeyBPlusTreeKeyIndexNode(this.nodeSize);
		// give the right 1/2 of the children to the new node
		int i;
		int splitPoint = (int) Math.ceil(((double)this.children.length) / 2);
		int numberToMove = this.children.length - splitPoint;
		
		System.arraycopy(this.children, splitPoint, rightNode.children, 0, numberToMove);
		
		for (i = 0; i < numberToMove; i++) {
			this.children[splitPoint + i] = null;
			this.keys[splitPoint + i - 1] = Long.MIN_VALUE;
			
			if (i > 0)
				rightNode.keys[i - 1] = rightNode.children[i].getLeftMostLeafKey();
			
			this.highWaterMark--;
		}
		
		rightNode.highWaterMark = i;
		return rightNode;
	}
	
	@Override
	public boolean get(long key) {
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
					
				if (deleteAt < (this.highWaterMark - 1))
					System.arraycopy(this.keys, (deleteAt + 1), this.keys, deleteAt, (this.highWaterMark - (deleteAt+1)));
				//for (int j = deleteAt; j < (this.highWaterMark - 1); j++)	this.keys[j] = this.keys[j + 1];
				
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
	}
	
	public LongKeyBPlusTreeKeyIndexLeaf getFirstChild() {
		if (this.children == null || this.children[0] == null)
			return null;
		
		if (this.getHeight() > 1)
			return ((LongKeyBPlusTreeKeyIndexNode)this.children[0]).getFirstChild();
		
		return (LongKeyBPlusTreeKeyIndexLeaf) this.children[0];
	}
	
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer += " ";
		
		retval.append(buffer + "# of entries | Max # of entries: " + this.highWaterMark + " | " + this.children.length + "\n");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Keys: [");
		for (int i = 0; i < this.keys.length; i++) {
			if (i > 0) retval.append(", ");
			retval.append(this.keys[i]);
		}
		retval.append("]\n");
		retval.append(buffer + "Entries:\n");
		for (int i = 0; i < this.highWaterMark; i++)
			retval.append(buffer + this.children[i].toString(indent + 2) + "\n");
				
		return retval.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < this.highWaterMark; i++)
			output.append(this.children[i].toStringShort());
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