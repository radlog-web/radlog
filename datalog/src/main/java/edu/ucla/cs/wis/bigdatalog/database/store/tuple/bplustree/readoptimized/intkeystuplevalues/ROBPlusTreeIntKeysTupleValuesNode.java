package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.intkeystuplevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class ROBPlusTreeIntKeysTupleValuesNode
	extends BPlusTreeNode<ROBPlusTreeIntKeysTupleValuesPage, ROBPlusTreeIntKeysTupleValuesLeaf> 
	implements ROBPlusTreeIntKeysTupleValuesPage, Serializable {
	private static final long serialVersionUID = 1L;

	protected int[] keys;
	
	public ROBPlusTreeIntKeysTupleValuesNode() { super(); }
	
	public ROBPlusTreeIntKeysTupleValuesNode(int nodeSize) {
		super(nodeSize, 4);

		this.highWaterMark = 0;
		this.keys = new int[this.numberOfKeys]; // M-1 keys
		this.children = new ROBPlusTreeIntKeysTupleValuesPage[this.getBranchingFactor()]; // M children
		this.numberOfEntries = 0;
	}
	
	//@Override
	//public int getNumberOfKeys() { return (this.nodeSize / 4); }

	@Override
	public int getLeftMostLeafKey() {
		return this.children[0].getLeftMostLeafKey();
	}

	@Override
	public Pair<ROBPlusTreeIntKeysTupleValuesPage, Boolean> insert(int key, Tuple tuple) {
		int insertAt = 0;
		for (insertAt = 0; insertAt < (this.highWaterMark - 1); insertAt++) {
			if (key < this.keys[insertAt]) 
				break;
		}

		Pair<ROBPlusTreeIntKeysTupleValuesPage, Boolean> retval = this.children[insertAt].insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
		
		if (retval.getFirst() == null)
			return retval;

		// shift right for insertion to the left
		if ((insertAt + 1) < this.highWaterMark)
			System.arraycopy(this.children, insertAt + 1, this.children, insertAt + 2, this.highWaterMark - (insertAt + 1));				
		// shift right for insertion to the left
		
		if (insertAt < (this.highWaterMark - 1)) 
			System.arraycopy(this.keys, insertAt, this.keys, (insertAt + 1), (this.highWaterMark - 1 - insertAt));
		
		// we are inserting the page with the right 1/2 of the values from the previous full page (that was split)
		// therefore, it goes to the right of where we inserted
		insertAt++;
		this.children[insertAt] = retval.getFirst(); 
		this.keys[insertAt - 1] = this.children[insertAt].getLeftMostLeafKey();	
		this.highWaterMark++;

		if (this.hasOverflow())
			return new Pair<>(this.split(), retval.getSecond());
			
		return new Pair<>(null, retval.getSecond());
	}

	private ROBPlusTreeIntKeysTupleValuesPage split() {
		ROBPlusTreeIntKeysTupleValuesNode rightNode = new ROBPlusTreeIntKeysTupleValuesNode(this.nodeSize);
		// give the right 1/2 of the children to the new node
		int i;
		int splitPoint = (int) Math.ceil(((double)this.children.length) / 2);
		int numberToMove = this.children.length - splitPoint;
		
		System.arraycopy(this.children, splitPoint, rightNode.children, 0, numberToMove);
		
		for (i = 0; i < numberToMove; i++) {
			this.children[splitPoint + i] = null;
			this.keys[(splitPoint + i - 1)] = 0;
			
			if (i > 0)
				rightNode.keys[i - 1] = rightNode.children[i].getLeftMostLeafKey();
			
			this.highWaterMark--;
		}
		
		rightNode.highWaterMark = i;
		return rightNode;
	}
	
	@Override
	public void get(int key, ROBPlusTreeGetResult result) {
		for (int i = 0; i < (this.highWaterMark - 1); i++) {
			if (key < this.keys[i]) {
				this.children[i].get(key, result);
				return;
			}
		}

		this.children[this.highWaterMark - 1].get(key, result);
		
		/*
		int lowEnd = 0;
		int highEnd = this.highWaterMark - 2;
		int midPoint = lowEnd + ((highEnd - lowEnd) / 2);
		while (lowEnd <= highEnd) {
			//System.out.println("searching for " + key + " lowEnd " + lowEnd + " midpoint " + midPoint + "("+this.keys[midPoint]+") highEnd " + highEnd);
			if (key < this.keys[midPoint]) {
				highEnd = midPoint - 1;
			} else if (key > this.keys[midPoint]) {
				lowEnd = midPoint + 1;
			} else {
				midPoint++;
				break;
			}
			midPoint = lowEnd + ((highEnd - lowEnd) / 2);
		}
		//System.out.println("looking in midpoint " + midPoint + "("+this.keys[midPoint]+") highwatermark " + this.highWaterMark);
		this.children[midPoint].get(key, result);		*/
	}

	@Override
	public boolean delete(int key) {
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
		this.numberOfEntries = 0;
		this.highWaterMark = 0;		
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer += " ";
		
		//retval.append(buffer + this.pageType.name() + "(PageId: " + this.pageId + ")\n");
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