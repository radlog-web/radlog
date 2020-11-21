package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreePage;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class LongKeyBPlusTreeKeyIndexLeaf 
	extends BPlusTreePage 
	implements LongKeyBPlusTreeKeyIndexPage, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	private long[] keys;
	private LongKeyBPlusTreeKeyIndexLeaf next;
	
	public LongKeyBPlusTreeKeyIndexLeaf(int nodeSize) {
		super(nodeSize, 8);
	
		this.keys = new long[this.numberOfKeys];	
		if (this.keys.length < 2)
			throw new DatabaseException("Size of leaf must be large enough to hold 2 keys.");
	}
	
	public long[] getKeys() { return this.keys; }
	
	@Override
	public int getHeight() { return 0; }
	
	@Override
	public boolean isEmpty() { return (this.highWaterMark == 0); }
	
	@Override
	public boolean hasOverflow() { return (this.highWaterMark == this.numberOfKeys); }

	@Override
	public long getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Long.MIN_VALUE;
		
		return this.keys[0];
	}
	
	public void insert(long key, LongKeyBPlusTreeKeyIndexResult result) {				
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				result.newPage = null;
				result.success = false;
				return;
			}
							
			if (this.keys[i] > key)
				break;
		}

		if (i != this.highWaterMark)
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));		
		
		this.keys[i] = key;
		this.highWaterMark++;
		
		result.success = true;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;				
	}
		
	private LongKeyBPlusTreeKeyIndexPage split() {
		LongKeyBPlusTreeKeyIndexLeaf rightLeaf = new LongKeyBPlusTreeKeyIndexLeaf(this.nodeSize);
		// give the right 1/2 of the children to the new leaf
		//int i;
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
	
	public boolean get(long key) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key)
				return true;
			
			if (this.keys[i] > key)
				return false;
		}
		return false;
	}
	
	@Override
	public boolean delete(long key) {
		int deleteAt;
		for (deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (this.keys[deleteAt] == key) {
				// move all after this position, down 1 position
				//for (int j = deleteAt; j < this.highWaterMark; j++) this.keys[j] = this.keys[j + 1];
				System.arraycopy(this.keys, (deleteAt + 1), this.keys, deleteAt, (this.highWaterMark - deleteAt));
			
				this.highWaterMark--;
				return true;
			}
		}
		return false;	// didn't find it
	}

	@Override
	public void deleteAll() {
		this.keys = null;
		this.next = null;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer += " ";
		
		retval.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length / this.bytesPerKey+ "\n");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			retval.append(buffer + "[");
			retval.append(this.keys[i]);
			retval.append("]\n");
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
		return new MemoryMeasurement(this.highWaterMark * 8, this.keys.length * 8);
	}
}
