package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.integerkey;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.index.secondary.OrderedTupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndexLeaf;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class IntegerKeyBPlusTreeSecondaryIndexLeaf 
	extends BPlusTreeSecondaryIndexLeaf<IntegerKeyBPlusTreeSecondaryIndexLeaf> 
	implements IntegerKeyBPlusTreeSecondaryIndexPage, Serializable {
	private static final long serialVersionUID = 1L;
		
	private int[] keys;
		
	public IntegerKeyBPlusTreeSecondaryIndexLeaf(int nodeSize) {
		super(nodeSize, 4);

		// leaf size determines the number of keys
		// same number of keys and address arrays
		this.keys = new int[this.numberOfKeys];
		if (this.keys.length < 2)
			throw new DatabaseException("Size of leaf must be large enough to hold 2 pairs of key/values");
		this.entryArray = new OrderedTupleAddressArray[this.numberOfKeys];
	}
	
	public int[] getKeys() { return this.keys; }

	@Override
	public int getLeftMostLeafKey() {
		if (this.keys == null || this.isEmpty())
			return Integer.MIN_VALUE;
		
		return this.keys[0];
	}

	public void insert(int key, int data, IntegerKeyBPlusTreeSecondaryIndexResult result) {				
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {				
				result.newPage = null;
				if (this.entryArray[i].put(data)) {
					result.success = true;
					return;
				}
				result.success = false;
				return;
			}
				
			if (this.keys[i] > key)
				break;
		}

		if (i != this.highWaterMark) {
			// shift values right to add new one
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.entryArray, i, this.entryArray, (i+1), (this.highWaterMark - i));

			/*for (int j = this.highWaterMark; j > i; j--) {
				this.keys[j] = this.keys[j - 1];
				this.entryArray[j] = this.entryArray[j - 1];
			}*/
			
			this.entryArray[i] = null;
		}

		// add new heap if first entry for key
		if (this.entryArray[i] == null)
			this.entryArray[i] = new OrderedTupleAddressArray();
		
		this.keys[i] = key;
		this.entryArray[i].put(data);
		this.highWaterMark++;
		
		result.success = true;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;
	}

	private IntegerKeyBPlusTreeSecondaryIndexPage split() {
		IntegerKeyBPlusTreeSecondaryIndexLeaf rightLeaf = new IntegerKeyBPlusTreeSecondaryIndexLeaf(this.nodeSize);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil((double)this.keys.length / 2);
		int numberToMove = this.keys.length - splitPoint;		
		// move keys and values to new leaf		
		for (int i = 0; i < numberToMove; i++) {
			rightLeaf.entryArray[i] = this.entryArray[splitPoint + i];
			rightLeaf.keys[i] = this.keys[splitPoint + i];
			this.keys[splitPoint + i] = Integer.MIN_VALUE;
			this.entryArray[splitPoint + i] = null;
			this.highWaterMark--;
		}
		
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		
		return rightLeaf;
	}
	
	public TupleAddressArray get(int key) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key)
				return this.entryArray[i];
			
			if (this.keys[i] > key)
				return null;
		}
		return null;
	}
		
	@Override
	public boolean delete(int key, int address) {
		boolean status = false;
		boolean removeValue = false;
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				// remove the value from the key's addresses
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
	
	public boolean delete(int key) {
		int deleteAt;
		for (deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (this.keys[deleteAt] == key) {
				// move all after this position, down 1 position
				for (int j = deleteAt; j < this.highWaterMark; j++) {
					this.keys[j] = this.keys[j + 1];
					this.entryArray[j] = this.entryArray[j + 1];
				}
				
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
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		//retval.append(buffer + this.pageType.name() + "(PageId: " + this.pageId + ")\n");
		retval.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length + "\n");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Keys:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			retval.append(buffer + "[");
			retval.append(this.keys[i]);
			retval.append("|");
			retval.append(this.entryArray[i].toString());
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
			used += this.highWaterMark * 4; // 4 bytes per integer
			allocated += this.keys.length * 4;
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
