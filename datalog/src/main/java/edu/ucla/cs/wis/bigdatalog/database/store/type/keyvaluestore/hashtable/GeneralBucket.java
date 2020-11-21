package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class GeneralBucket 
	extends Bucket 
	implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	private int bytesPerKey;
	private byte[] keys;

	public GeneralBucket() { super(); }
	
	public GeneralBucket(int bytesPerKey, int bytesPerValue) {	
		super(bytesPerValue);
		this.bytesPerKey = bytesPerKey;
		this.initialize();
	}
	
	protected void initialize() {
		super.initialize();
		this.keys = new byte[INITIAL_SIZE * this.bytesPerKey];	
	}
	
	public byte[] getKeys() { return this.keys; }
			
	public void setEntries(byte[] keys, long[] hashes, byte[] values) { 		
		// if the entries being added are few, copy them to existing array which should be 'initialSize'
		if (hashes.length < this.hashes.length) {
			for (int i = 0; i < hashes.length; i++)
				this.hashes[i] = hashes[i];

			System.arraycopy(keys, 0, this.keys, 0, keys.length);
			System.arraycopy(values, 0, this.values, 0, values.length);
		} else {
			this.keys = keys;
			this.hashes = hashes;
			this.values = values;
		}
		this.highWaterMark = hashes.length;
	}
		
	public void put(byte[] key, long hash, byte[] value, HashTablePutResult result) {
		// we keep an ordered list, so we can search it linearly and quit early
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			// search with hash since its faster than with byte array
			if (this.hashes[i] == hash) {
				if (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0) {
					result.status = KeyValueOperationStatus.UPDATE;
					System.arraycopy(this.values, i * this.bytesPerValue, result.oldValue, 0, this.bytesPerValue);
					System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);
					return; 
				}
			}
				
			if (this.hashes[i] > hash)
				break;
		}

		// if we reached here, we're adding another key to this bucket
		// if bucket is full, we need to grow it first
		if (this.isFull())
			this.grow();

		// only shift right if we're not already at the end
		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
			System.arraycopy(this.values, i * this.bytesPerValue, this.values, (i+1) * this.bytesPerValue, (this.highWaterMark - i) * this.bytesPerValue);
			System.arraycopy(this.hashes, i, this.hashes, (i+1), (this.highWaterMark - i));
		}
		
		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);
		System.arraycopy(value, 0, this.values, i * this.bytesPerValue, this.bytesPerValue);		
		this.hashes[i] = hash;
		
		this.highWaterMark++;
		result.status = KeyValueOperationStatus.NEW;
	}
	
	public void putFromSplit(byte[] key, long hash, byte[] value) {
		if (this.isFull())
			this.grow();

		int offset = this.highWaterMark * this.bytesPerKey;
		for (int i = 0; i < key.length; i++)
			this.keys[offset + i] = key[i];
		
		int offset2 = this.highWaterMark * this.bytesPerValue;
		for (int i = 0; i < value.length; i++)
			this.values[offset2 + i] = value[i];
		
		this.hashes[this.highWaterMark] = hash;

		this.highWaterMark++;
	}
	
	protected void grow() {
		int newSize = this.hashes.length;
		if (newSize < 1)
			newSize = 0;
		else if (newSize < FAST_GROW_SIZE)
			newSize *= 2;
		else
			newSize *= 1.25;
				
		byte[] newKeys = new byte[this.bytesPerKey * newSize];
		long[] newHashes = new long[newSize];
		byte[] newValues = new byte[this.bytesPerValue * newSize];

		System.arraycopy(this.keys, 0, newKeys, 0, this.keys.length);
		System.arraycopy(this.hashes, 0, newHashes, 0, this.hashes.length);
		System.arraycopy(this.values, 0, newValues, 0, this.values.length);
		
		this.keys = newKeys;
		this.hashes = newHashes;
		this.values = newValues;
	}

	public void get(byte[] key, long hash, HashTableGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.hashes[i] == hash) {
				if (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0) {
					System.arraycopy(this.values, i * this.bytesPerValue, result.value, 0, this.bytesPerValue);
					result.success = true;
					return;
				}									
			}
			
			if (this.hashes[i] > hash)
				break;			
		}
		result.success = false;
	}

	public boolean remove(byte[] key, long hash) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.hashes[i] == hash) {
				if (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0) {							
					System.arraycopy(this.hashes, (i + 1), this.hashes, i * this.bytesPerKey, (this.highWaterMark - (i + 1)));
					
					int lengthToShift = (this.highWaterMark * this.bytesPerKey) - ((i + 1) * this.bytesPerKey);
					System.arraycopy(this.keys, (i + 1) * this.bytesPerKey, this.keys, i * this.bytesPerKey, lengthToShift);
					
					int lengthToShift2 = (this.highWaterMark * this.bytesPerValue) - ((i + 1) * this.bytesPerValue);
					System.arraycopy(this.values, (i + 1) * this.bytesPerValue, this.values, i * this.bytesPerValue, lengthToShift2);
					
					this.highWaterMark--;
					
					return true;
				} 
			}
			
			if (this.hashes[i] > hash)
				return false;
		}
		return false;
	}
	
	public void remove(LinkedList<Integer> positionsOfKeysToRemove) {
		if (positionsOfKeysToRemove.size() == 0)
			return;
		
		Integer positionToRemove;
		int lengthToShift;
		int lengthToShift2;
		
		while ((positionToRemove = positionsOfKeysToRemove.poll()) != null) {
			lengthToShift = (this.highWaterMark * this.bytesPerKey) - ((positionToRemove + 1) * this.bytesPerKey);
			lengthToShift2 = (this.highWaterMark * this.bytesPerValue) - ((positionToRemove + 1) * this.bytesPerValue);
			if (lengthToShift > 0) {
				System.arraycopy(this.keys, (positionToRemove + 1) * this.bytesPerKey, this.keys, positionToRemove * this.bytesPerKey, lengthToShift);
				System.arraycopy(this.values, (positionToRemove + 1) * this.bytesPerValue, this.values, positionToRemove * this.bytesPerValue, lengthToShift2);
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
	
	@Override
	public String toStringShort() {
		StringBuilder retval = new StringBuilder();
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(",");
			retval.append("[");
			for (int j = 0; j < this.bytesPerKey; j++)
				retval.append(this.keys[j + (i * this.bytesPerKey)]);
			retval.append(":");
			for (int j = 0; j < this.bytesPerValue; j++)
				retval.append(this.values[j + (i * this.bytesPerValue)]);
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
