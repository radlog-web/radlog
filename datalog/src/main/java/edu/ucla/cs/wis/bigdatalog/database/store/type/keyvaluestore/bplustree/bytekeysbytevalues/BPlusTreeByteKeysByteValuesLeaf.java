package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.bytekeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeGeneralLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLongLongLong;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeByteKeysByteValuesLeaf 
	extends BPlusTreeGeneralLeaf<BPlusTreeByteKeysByteValuesLeaf> 
	implements BPlusTreeByteKeysByteValuesPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected byte[] keys;

	public BPlusTreeByteKeysByteValuesLeaf() { super(); }
	
	public BPlusTreeByteKeysByteValuesLeaf(int nodeSize, int bytesPerKey, int bytesPerValue) {
		super(nodeSize, bytesPerKey, bytesPerValue);
		
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey];
		this.values = new byte[this.numberOfKeys * this.bytesPerValue];		
	}
	
	public byte[] getKeys() { return this.keys; }
	
	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.keys == null || this.isEmpty())
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}

	public void insert(byte[] key, byte[] value, BPlusTreeByteKeysByteValuesInsertResult result) {				
		int compare;
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			// if we have an exact match, update the value for the key
			if (compare == 0) {
				System.arraycopy(this.values, i * this.bytesPerValue, result.oldValue, 0, this.bytesPerValue);
				System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);
				result.newPage = null;
				result.status = KeyValueOperationStatus.UPDATE;
				return;
			}
				
			if (compare < 0)
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
			System.arraycopy(this.values, i * this.bytesPerValue, this.values, (i+1) * this.bytesPerValue, (this.highWaterMark - i) * this.bytesPerValue);
		}
	
		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);
		System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);
		
		this.highWaterMark++;
		
		result.oldValue = null;
		result.status = KeyValueOperationStatus .NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;
	}
		
	private BPlusTreeByteKeysByteValuesPage split() {
		BPlusTreeByteKeysByteValuesLeaf rightLeaf = new BPlusTreeByteKeysByteValuesLeaf(this.nodeSize, this.bytesPerKey, this.bytesPerValue);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
		// move values to new leaf
		System.arraycopy(this.values, splitPoint * this.bytesPerValue, rightLeaf.values, 0, numberToMove * this.bytesPerValue);
		
		this.highWaterMark -= numberToMove;
				
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	public void get(byte[] key, BPlusTreeByteKeysByteValuesGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {			
			int compareResult = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compareResult == 0) {
				System.arraycopy(this.values, (i * this.bytesPerValue), result.value, 0, this.bytesPerValue);
				result.success = true;
				return;
			}
			
			if (compareResult < 0)
				break;
		}
		result.success = false;
	}
	
	@Override
	public boolean delete(byte[] key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth				
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - (deleteAt + 1)) * this.bytesPerKey);
				System.arraycopy(this.values, (deleteAt + 1) * this.bytesPerValue, this.values, deleteAt * this.bytesPerValue, (this.highWaterMark - (deleteAt + 1)) * this.bytesPerValue);
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
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length / this.bytesPerKey+ "\n");
		output.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.values.length / this.bytesPerValue + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
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
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
						
			byte[] value = new byte[this.bytesPerValue];
			System.arraycopy(this.values, (i * this.bytesPerValue), value, 0, this.bytesPerValue);
			
			switch (this.bytesPerValue) {
				case 4:
					output.append(DbInteger.create(ByteArrayHelper.getBytesAsInt(value, 0)).toString());
					break;
				case 8:				
					output.append(DbLong.create(ByteArrayHelper.getBytesAsLong(value, 0)).toString());
					break;
				case 16:
					output.append(DbLongLong.create(value).toString());				
					break;
				default:
					output.append(DbLongLongLongLong.create(value).toString());
			}
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
		
		if (this.values != null) {
			used += this.highWaterMark * this.bytesPerValue;
			allocated += this.values.length;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
