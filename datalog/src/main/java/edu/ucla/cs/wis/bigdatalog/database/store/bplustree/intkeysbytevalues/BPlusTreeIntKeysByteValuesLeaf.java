package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysbytevalues;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeGeneralLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeIntKeysByteValuesLeaf 
	extends BPlusTreeGeneralLeaf<BPlusTreeIntKeysByteValuesLeaf> 
	implements BPlusTreeIntKeysByteValuesPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	private int[] keys;
	
	public BPlusTreeIntKeysByteValuesLeaf() { super(); }
	
	public BPlusTreeIntKeysByteValuesLeaf(int nodeSize, int bytesPerValue) {
		super(nodeSize, 4, bytesPerValue);

		this.keys = new int[this.numberOfKeys];
		this.values = new byte[this.numberOfKeys * this.bytesPerValue];
	}
	
	public int[] getKeys() { return this.keys; }

	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.keys[0];
	}

	public void insert(int key, byte[] value, BPlusTreeIntKeysByteValuesInsertResult result) {	
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			// if we have an exact match, update the value for the key
			if (this.keys[i] == key) {
				System.arraycopy(this.values, i * this.bytesPerValue, result.oldValue, 0, this.bytesPerValue);
				System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);
				result.newPage = null;
				result.status = BPlusTreeOperationStatus.UPDATE;
				return;
			}
				
			if (key < this.keys[i])
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.values, i * this.bytesPerValue, this.values, (i+1) * this.bytesPerValue, (this.highWaterMark - i) * this.bytesPerValue);
		}

		this.keys[i] = key;
		System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);
		
		this.highWaterMark++;
		
		result.status = BPlusTreeOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;	
	}
		
	private BPlusTreeIntKeysByteValuesPage split() {
		BPlusTreeIntKeysByteValuesLeaf rightLeaf = new BPlusTreeIntKeysByteValuesLeaf(this.nodeSize, this.bytesPerValue);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint, rightLeaf.keys, 0, numberToMove);
		// move values to new leaf
		System.arraycopy(this.values, splitPoint * this.bytesPerValue, rightLeaf.values, 0, numberToMove * this.bytesPerValue);
		
		this.highWaterMark -= numberToMove;
			
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	@Override
	public byte[] get(int key) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key)
				return Arrays.copyOfRange(this.values, (i * this.bytesPerValue), ((i + 1) * this.bytesPerValue));
						
			if (this.keys[i] > key)
				return null;
		}
		
		return null;
	}

	@Override
	public void get(int startKey, int endKey, BPlusTreeIntKeysByteValuesGetResultRange result) {
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if (startKey <= this.keys[i] && this.keys[i] <= endKey) {
				result.success = true;
				result.index = i;
				result.leaf = this;
				return;
			}
			
			if (endKey < this.keys[i])
				break;
		}
		
		/*if (i < this.highWaterMark) {
			result.success = false;		
		} else {
			if (this.next == null) {
				result.success = false;
			} else {
				this.next.get(startKey, endKey, result);
			}
		}*/
		result.success = false;		
	}
	
	@Override
	public boolean delete(int key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (this.keys[deleteAt] == key) {
				// move all keys and values after this position, left one key or value worth
				System.arraycopy(this.keys, (deleteAt + 1), this.keys, deleteAt, (this.highWaterMark - deleteAt));
				System.arraycopy(this.values, (deleteAt + 1) * this.bytesPerValue, this.values, deleteAt * this.bytesPerValue, (this.highWaterMark - deleteAt) * this.bytesPerValue);

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
		this.values = null;
		this.next = null;
		this.highWaterMark = 0;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length + "\n");
		output.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.values.length / this.bytesPerValue + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "[");
			output.append(this.keys[i]);
			output.append("|");
			for (int j = 0; j < this.bytesPerValue; j++)
				output.append(this.values[(i * this.bytesPerValue) + j]);
			output.append("]\n");
		}
		return output.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			output.append(this.keys[i]);
			output.append("|");
			for (int j = 0; j < this.bytesPerValue; j++)
				output.append(this.values[(i * this.bytesPerValue) + j]);
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
		
		if (this.values != null) {
			used += this.highWaterMark * this.bytesPerValue;
			allocated += this.values.length;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
