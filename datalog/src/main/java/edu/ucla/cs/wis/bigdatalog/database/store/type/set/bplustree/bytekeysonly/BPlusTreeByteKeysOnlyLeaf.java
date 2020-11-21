package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.bytekeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeByteKeysOnlyLeaf 
	extends BPlusTreeLeaf<BPlusTreeByteKeysOnlyLeaf>
	implements BPlusTreeByteKeysOnlyPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected byte[] keys;
	
	public BPlusTreeByteKeysOnlyLeaf() { super(); }
	
	public BPlusTreeByteKeysOnlyLeaf(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);

		this.keys = new byte[this.numberOfKeys * this.bytesPerKey];		
	}
		
	public byte[] getKeys() { return this.keys; }
		
	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}

	public void insert(byte[] key, BPlusTreeByteKeysOnlyInsertResult result) {				
		int compare;
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			// if we have an exact match, update the value for the key
			if (compare == 0) {
				result.newPage = null;
				result.status = BPlusTreeOperationStatus.NO_CHANGE;
				return;
			}
							
			if (compare < 0)
				break;
		}

		if (i != this.highWaterMark)
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
				
		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);
		
		this.highWaterMark++;
		
		result.status = BPlusTreeOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;		
	}
		
	private BPlusTreeByteKeysOnlyPage split() {
		BPlusTreeByteKeysOnlyLeaf rightLeaf = new BPlusTreeByteKeysOnlyLeaf(this.nodeSize, this.bytesPerKey);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
				
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
		
		this.highWaterMark -= numberToMove;
		
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	public byte[] get(byte[] key) {
		int compareResult;
		for (int i = 0; i < this.highWaterMark; i++) {			
			compareResult = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compareResult == 0)
				return key;
			
			if (compareResult < 0)
				return null;
		}
		return null;
	}
	
	@Override
	public boolean delete(byte[] key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth				
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - (deleteAt + 1)) * this.bytesPerKey);
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
		
		retval.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length / this.bytesPerKey+ "\n");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			retval.append(buffer + "[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				retval.append(this.keys[(i * this.bytesPerKey) + j]);
			retval.append("]\n");
		}
		return retval.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("]");
		}
		return output.toString();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.highWaterMark * this.bytesPerKey;
			allocated += this.keys.length;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
