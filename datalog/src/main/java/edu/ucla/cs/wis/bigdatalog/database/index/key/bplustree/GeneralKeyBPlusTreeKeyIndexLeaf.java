package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreePage;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class GeneralKeyBPlusTreeKeyIndexLeaf 
	extends BPlusTreePage
	implements GeneralKeyBPlusTreeKeyIndexPage, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	private byte[] keys;
	private GeneralKeyBPlusTreeKeyIndexLeaf next;
	
	public GeneralKeyBPlusTreeKeyIndexLeaf(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);

		this.keys = new byte[this.nodeSize];	
		if (this.keys.length < 2)
			throw new DatabaseException("Size of leaf must be large enough to hold 2 pairs of key/values");
	}
	
	public byte[] getKeys() { return this.keys; }
	
	@Override
	public int getHeight() { return 0; }
	
	@Override
	public boolean isEmpty() { return (this.highWaterMark == 0); }
	
	@Override
	public boolean hasOverflow() { return (this.highWaterMark == this.numberOfKeys); }

	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.keys == null || this.isEmpty())
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}

	public void insert(byte[] key, GeneralKeyBPlusTreeKeyIndexResult result) {				
		int compare;
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			// already have the key
			if (compare == 0) {
				result.newPage = null;
				result.success = false;
				return;
			}
							
			if (compare < 0)
				break;
		}

		if (i != this.highWaterMark)
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);

		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);

		this.highWaterMark++;
		result.success = true;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;
	}
		
	private GeneralKeyBPlusTreeKeyIndexPage split() {
		GeneralKeyBPlusTreeKeyIndexLeaf rightLeaf = new GeneralKeyBPlusTreeKeyIndexLeaf(this.nodeSize, this.bytesPerKey);
		// give the right 1/2 of the children to the new leaf
		//int i;
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
	
	public boolean get(byte[] key) {
		for (int i = 0; i < this.highWaterMark; i++) {			
			int compareResult = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compareResult == 0)
				return true;
			
			if (compareResult < 0)
				return false;
		}
		return false;
	}
	
	@Override
	public boolean delete(byte[] key) {
		int deleteAt;
		for (deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys after this position, left one key or value worth					
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - deleteAt) * this.bytesPerKey);

				this.highWaterMark--;
				return true;
			}
		}
		return false;
	}

	@Override
	public void deleteAll() {
		//this.initialize();
		this.keys = null;
		this.next = null;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		//retval.append(buffer + this.pageType.name() + "(PageId: " + this.pageId + ")\n");
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
		return new MemoryMeasurement(this.highWaterMark * this.bytesPerKey, this.keys.length);
	}
}
