package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.general;

import java.io.Serializable;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.OrderedTupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndexLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class GeneralKeyBPlusTreeSecondaryIndexLeaf 
	extends BPlusTreeSecondaryIndexLeaf<GeneralKeyBPlusTreeSecondaryIndexLeaf>
	implements GeneralKeyBPlusTreeSecondaryIndexPage, Serializable {
	private static final long serialVersionUID = 1L;
		
	protected byte[] keys;	
			
	public GeneralKeyBPlusTreeSecondaryIndexLeaf(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);

		// leaf size determines the number of keys
		// same number of keys and address arrays		
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey];
		if (this.keys.length < 2)
			throw new DatabaseException("Size of leaf must be large enough to hold 2 pairs of key/values");
		this.entryArray = new OrderedTupleAddressArray[this.numberOfKeys];
	}
	
	public byte[] getKeys() { return this.keys; }
	
	@Override
	public boolean hasOverflow() { return (this.highWaterMark == this.keys.length / this.bytesPerKey); }

	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.keys == null || this.isEmpty())
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}

	public void insert(byte[] key, int data, GeneralKeyBPlusTreeSecondaryIndexResult result) {				
		int compare;
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			// if we have an exact match put in array
			if (compare == 0) {
				result.newPage = null;
				if (this.entryArray[i].put(data)) {
					result.success = true;
					return;
				}
				result.success = false;				
				return;
			}
				
			if (compare < 0)
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
			System.arraycopy(this.entryArray, i, this.entryArray, (i+1), (this.highWaterMark - i));			
			this.entryArray[i] = null;
		}

		// add key to key storage
		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);

		// add new heap if first entry for key
		if (this.entryArray[i] == null)
			this.entryArray[i] = new OrderedTupleAddressArray();
		
		this.entryArray[i].put(data);		
		this.highWaterMark++;
		
		result.success = true;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;
	}

	private GeneralKeyBPlusTreeSecondaryIndexPage split() {
		GeneralKeyBPlusTreeSecondaryIndexLeaf rightLeaf = new GeneralKeyBPlusTreeSecondaryIndexLeaf(this.nodeSize, this.bytesPerKey);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
		// move values to new leaf		
		for (int i = 0; i < numberToMove; i++) {
			rightLeaf.entryArray[i] = this.entryArray[splitPoint + i];
			this.entryArray[splitPoint + i] = null;
			this.highWaterMark--;
		}
		
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	public TupleAddressArray get(byte[] key) {
		for (int i = 0; i < this.highWaterMark; i++) {			
			int compareResult = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compareResult == 0)
				return this.entryArray[i];
			
			if (compareResult < 0)
				return null;
		}
		return null;
	}
	
	@Override
	public boolean delete(byte[] key, int address) {
		boolean status = false;
		boolean removeValue = false;
		for (int i = 0; i < this.highWaterMark; i++) {
			if (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0) {
				// remove the value from the key's array
				status = this.entryArray[i].remove(address);
				if (status && this.entryArray[i].isEmpty())
					removeValue = true;
					
				break;
			}
		}
		
		// if last value removed, remove key
		if (removeValue)
			this.delete(key);
		
		return status;
	}
	
	public boolean delete(byte[] key) {
		int deleteAt;
		for (deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth				
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - deleteAt) * this.bytesPerKey);
				System.arraycopy(this.entryArray, (deleteAt + 1), this.entryArray, deleteAt, (this.highWaterMark - deleteAt));
				// move all after this position, down 1 position				
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
		
		for (int i = 0 ; i < this.highWaterMark; i++)
			this.entryArray[i].deleteAll();
		
		this.entryArray = null;
		this.highWaterMark = 0;
	}
	
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + (this.keys.length / this.bytesPerKey) + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Keys:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
			output.append(this.entryArray[i].toString());
			output.append("]\n");
		}
		return output.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
			output.append(this.entryArray[i]);
			output.append("]");
		}
		return output.toString();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.bytesPerKey * this.highWaterMark;
			allocated += this.keys.length;
		}
		
		if (this.entryArray != null) {
			MemoryMeasurement sizes;
			for (int i = 0; i < this.highWaterMark; i++) {
				sizes = this.entryArray[i].getSizeOf();
				used += sizes.getUsed();
				allocated += sizes.getAllocated();
			}
		}
		return new MemoryMeasurement(used, allocated);
	}
}
