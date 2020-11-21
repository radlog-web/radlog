package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class IntegerKeyBucket extends Bucket implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;

	private int[] keys;
	
	public IntegerKeyBucket() { super(); }
	
	public IntegerKeyBucket(int bytesPerValue) {	
		super(bytesPerValue);		
		this.initialize();
	}
	
	protected void initialize() {
		super.initialize();
		this.keys = new int[INITIAL_SIZE];
	}
	
	public int[] getKeys() { return this.keys; }
			
	public void setEntries(int[] keys, long[] hashes, byte[] values) { 		
		// if the entries being added are few, copy them to existing array which should be 'initialSize'
		if (hashes.length < this.hashes.length) {
			for (int i = 0; i < hashes.length; i++) {
				this.hashes[i] = hashes[i];
				this.keys[i] = keys[i];
			}

			System.arraycopy(values, 0, this.values, 0, values.length);
		} else {
			this.keys = keys;
			this.hashes = hashes;
			this.values = values;
		}
		this.highWaterMark = hashes.length;
	}
		
	public void put(int key, long hash, byte[] value, HashTablePutResult result) {
		// we keep an ordered list, so we can search it linearly and quit early
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				result.status = KeyValueOperationStatus.UPDATE;
				System.arraycopy(this.values, i * this.bytesPerValue, result.oldValue, 0, this.bytesPerValue);
				System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);
				return; 
			}
				
			if (this.keys[i] > key)
				break;
		}

		// if we reached here, we're adding another key to this bucket
		// if bucket is full, we need to grow it first
		if (this.isFull())
			this.grow();

		// only shift right if we're not already at the end
		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.hashes, i , this.hashes, (i+1) , (this.highWaterMark - i));
			System.arraycopy(this.values, i * this.bytesPerValue, this.values, (i+1) * this.bytesPerValue, (this.highWaterMark - i) * this.bytesPerValue);
		}
				
		System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);
		
		this.hashes[i] = hash;
		this.keys[i] = key;
		
		this.highWaterMark++;
		result.status = KeyValueOperationStatus.NEW;
	}
	
	public void putFromSplit(int key, long hash, byte[] value) {
		if (this.isFull())
			this.grow();
				
		int offset = this.highWaterMark * this.bytesPerValue;
		for (int i = 0; i < value.length; i++)
			this.values[offset + i] = value[i];
		
		this.hashes[this.highWaterMark] = hash;
		this.keys[this.highWaterMark] = key;

		this.highWaterMark++;
	}
	
	protected void grow() {
		int newSize = this.hashes.length;
		if (newSize < 1)
			newSize = 1;
		else if (newSize < FAST_GROW_SIZE)
			newSize *= 2;
		else
			newSize *= 1.25;
				
		long[] newHashes = new long[newSize];
		int [] newKeys = new int[newSize];
		byte[] newValues = new byte[this.bytesPerValue * newSize];
				
		System.arraycopy(this.keys, 0, newKeys, 0, this.keys.length);
		System.arraycopy(this.hashes, 0, newHashes, 0, this.hashes.length);
		System.arraycopy(this.values, 0, newValues, 0, this.values.length);
		
		this.keys = newKeys;
		this.hashes = newHashes;
		this.values = newValues;
	}

	public void get(int key, HashTableGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				System.arraycopy(this.values, i * this.bytesPerValue, result.value, 0, this.bytesPerValue);
				result.success = true;
				return;
			}

			if (this.keys[i] > key)
				break;
		}
		result.success = false;
	}

	public boolean remove(int key) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				System.arraycopy(this.keys, (i + 1), this.keys, i, (this.highWaterMark - (i + 1)));
				System.arraycopy(this.hashes, (i + 1), this.hashes, i, (this.highWaterMark - (i + 1)));
				
				int lengthToShift = (this.highWaterMark * this.bytesPerValue) - ((i + 1) * this.bytesPerValue);
				System.arraycopy(this.values, (i + 1) * this.bytesPerValue, this.values, i * this.bytesPerValue, lengthToShift);

				this.highWaterMark--;
					
				return true;				 
			}
			
			if (this.keys[i] > key)
				return false;
		}
		return false;
	}
	
	public void remove(LinkedList<Integer> positionsOfKeysToRemove) {
		if (positionsOfKeysToRemove.size() == 0)
			return;
		
		Integer positionToRemove;
		int lengthToShift;
		
		while ((positionToRemove = positionsOfKeysToRemove.poll()) != null) {
			lengthToShift = (this.highWaterMark * this.bytesPerValue) - ((positionToRemove + 1) * this.bytesPerValue);
			if (lengthToShift > 0) {
				System.arraycopy(this.values, (positionToRemove + 1) * this.bytesPerValue, this.values, positionToRemove * this.bytesPerValue, lengthToShift);
				System.arraycopy(this.keys, (positionToRemove + 1), this.keys, positionToRemove, (this.highWaterMark - (positionToRemove + 1)));
				System.arraycopy(this.hashes, (positionToRemove + 1), this.hashes, positionToRemove, (this.highWaterMark - (positionToRemove + 1)));
			}			
			
			this.highWaterMark--;
		}
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("# keys: " + this.highWaterMark + "\nkeys:[" + Arrays.toString(this.keys) + "] \nhashes:[" + Arrays.toString(this.hashes) + "]\n");

		if (this.highWaterMark > 0) {
			for (int i = 0; i < this.highWaterMark; i++) {
				if (i > 0)
					retval.append(",");
				for (int j = 0; j < this.bytesPerValue; j++)
					retval.append(this.values[j + (i * this.bytesPerValue)]);
			}
			retval.append("]\n");
		}

		return retval.toString();
	}
	
	public String toStringShort() {
		StringBuilder retval = new StringBuilder();
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(",");
			retval.append("[" + this.keys[i] + ":");
			if (this.bytesPerValue == 4)
				retval.append(ByteArrayHelper.getBytesAsInt(this.values, this.bytesPerValue * i));
			else
				retval.append(ByteArrayHelper.getBytesAsLong(this.values, this.bytesPerValue * i));
			retval.append("]");
		}
		return retval.toString();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		
		if (this.hashes != null) {
			used += this.highWaterMark * 8; // 8 bytes per long
			allocated += this.hashes.length * 8; // 8 bytes per long
		}
		
		if (this.keys != null) {
			used += this.highWaterMark * 4; // 4 bytes per int
			allocated += this.keys.length * 4; // 4 bytes per int
		}
	
		if (this.values != null) {
			used += this.highWaterMark * this.bytesPerValue;
			allocated += this.values.length;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
