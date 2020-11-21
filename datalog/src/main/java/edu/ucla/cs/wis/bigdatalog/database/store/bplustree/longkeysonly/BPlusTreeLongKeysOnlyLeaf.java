package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeLongKeysOnlyLeaf 
	extends BPlusTreeLeaf<BPlusTreeLongKeysOnlyLeaf> 
	implements BPlusTreeLongKeysOnlyPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	private long[] keys;
	
	public BPlusTreeLongKeysOnlyLeaf() { super(); }
	
	public BPlusTreeLongKeysOnlyLeaf(int nodeSize) {
		super(nodeSize, 8);

		this.keys = new long[this.numberOfKeys];
	}
	
	public long[] getKeys() { return this.keys; }

	@Override
	public long getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Long.MIN_VALUE;
		
		return this.keys[0];
	}
	
	public void insert(long key, BPlusTreeLongKeysOnlyInsertResult result) {				
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
		
	private BPlusTreeLongKeysOnlyPage split() {
		BPlusTreeLongKeysOnlyLeaf rightLeaf = new BPlusTreeLongKeysOnlyLeaf(this.nodeSize);
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
	public Long get(long key) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key)
				return key;
			
			if (this.keys[i] > key)
				return null;
		}
		return null;
	}
		
	@Override
	public boolean delete(long key) {
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
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: [" + this.highWaterMark + " | " + this.keys.length + "] ");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "keys: ");
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				output.append(", ");
			output.append(this.keys[i]);
		}
		return output.toString();
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
			used += this.highWaterMark * 8;
			allocated += this.keys.length * 8;
		}
				
		return new MemoryMeasurement(used, allocated);
	}
}
