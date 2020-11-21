package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeIntKeysOnlyLeaf 
	extends BPlusTreeLeaf<BPlusTreeIntKeysOnlyLeaf> 
	implements BPlusTreeIntKeysOnlyPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	private int[] keys;
	
	public BPlusTreeIntKeysOnlyLeaf() { super(); }
	
	public BPlusTreeIntKeysOnlyLeaf(int nodeSize) {
		super(nodeSize, 4);

		this.keys = new int[this.numberOfKeys];
	}
	
	public int[] getKeys() { return this.keys; }
	
	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.keys[0];
	}
	
	public void insert(int key, BPlusTreeIntKeysOnlyInsertResult result) {				
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if (key == this.keys[i]) {
				result.newPage = null;
				result.status = BPlusTreeOperationStatus.NO_CHANGE;
				return;
			}
				
			if (key < this.keys[i])
				break;
		}

		if (i != this.highWaterMark)
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
		
		this.keys[i] = key;
		this.highWaterMark++;
		
		result.status = BPlusTreeOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;		
	}
		
	private BPlusTreeIntKeysOnlyPage split() {
		BPlusTreeIntKeysOnlyLeaf rightLeaf = new BPlusTreeIntKeysOnlyLeaf(this.nodeSize);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint, rightLeaf.keys, 0, numberToMove);

		this.highWaterMark -= numberToMove;
			
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	@Override
	public Integer get(int key) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key)
				return key;
			
			if (key < this.keys[i])
				break;
		}
		return null;
	}
	
	public int getAt(int position) {
		if (position >= this.keys.length)
			return -1;
		return this.keys[position];
	}
	
	@Override
	public boolean delete(int key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (this.keys[deleteAt] == key) {
				// move all after this position, down 1 position
				System.arraycopy(this.keys, (deleteAt + 1), this.keys, deleteAt, (this.highWaterMark - deleteAt));
								
				this.highWaterMark--;
				status = true;
				break;
			}
		}
		return status;
	}

	@Override
	public void deleteAll() {
		this.keys = null;
		this.next = null;
		this.highWaterMark = 0;
	}
			
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		retval.append(buffer + "# of keys | Max # of keys: [" + this.highWaterMark + " | " + this.keys.length + "] ");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "keys: ");
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(", ");
			retval.append(this.keys[i]);
		}
		return retval.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			output.append(this.keys[i]);
			output.append("]");
		}
		return output.toString();
	}	
	
	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.highWaterMark * 4;
			allocated += this.keys.length * 4;
		}
				
		return new MemoryMeasurement(used, allocated);
	}
}
