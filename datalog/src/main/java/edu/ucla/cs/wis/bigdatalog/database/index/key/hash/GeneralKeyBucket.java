package edu.ucla.cs.wis.bigdatalog.database.index.key.hash;

import java.util.Arrays;
import java.util.LinkedList;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

/* APS 7/9/2014
 * GeneralKeyBucket keeps the entires ordered by hash[] since byte[] key[] comparison is more expensive.  
 * Hash is also kept for faster bucket splitting. */
public class GeneralKeyBucket implements MemorySize {
	private static final int FAST_GROW_SIZE = 8;
	private static final int INITIAL_SIZE = 0;
	private int highWaterMark;
	private int bytesPerKey;
	private byte[] keys;
	private long[] hashes;
	
	public GeneralKeyBucket(int bytesPerKey) {
		this.bytesPerKey = bytesPerKey;
		this.initialize();
	}
	
	private void initialize() {
		this.highWaterMark = 0;
		this.keys = new byte[INITIAL_SIZE * this.bytesPerKey];
		this.hashes = new long[INITIAL_SIZE];
	}

	public boolean isEmpty() { return (this.highWaterMark == 0); }

	public boolean isFull() { return (this.highWaterMark == (this.keys.length / this.bytesPerKey)); }
	
	public int getNumberOfKeys() { return this.highWaterMark; }

	public byte[] getKeys() { return this.keys; }
	
	public long[] getHashes() { return this.hashes; }

	public void setEntries(byte[] keys, /*int*/long hashes[]) {				
		if (hashes.length < this.hashes.length) {
			for (int i = 0; i < hashes.length; i++)
				this.hashes[i] = hashes[i];
			System.arraycopy(keys, 0, this.keys, 0, keys.length);
		} else {
			this.keys = keys;
			this.hashes = hashes;			
		}

		this.highWaterMark = keys.length / this.bytesPerKey;
	}

	public boolean put(byte[] key, long hash) {		
		// we keep an ordered list, so we can search it and know when to stop looking
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			// if this is true, we already have the key
			if (this.hashes[i] == hash) {
				if (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0) {
					return false;
				}
			}

			if (this.hashes[i] > hash)
				break;		
		}
		
		if (this.isFull())
			this.grow();
		
		// only shift right if we're not already at the end
		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
			System.arraycopy(this.hashes, i, this.hashes, (i+1), (this.highWaterMark - i));
		}
		
		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);
		this.hashes[i] = hash;

		this.highWaterMark++;
		return true;
	}
	
	private void grow() {
		int newSize = this.hashes.length;
		if (newSize < 1)
			newSize = 1;
		else if (newSize < FAST_GROW_SIZE)
			newSize *= 2;
		else
			newSize *= 1.25;
		
		byte[] newKeys = new byte[newSize * this.bytesPerKey];
		System.arraycopy(this.keys, 0, newKeys, 0, this.keys.length);
		
		long[] newHashes = new long[newSize];
		System.arraycopy(this.hashes, 0, newHashes, 0, this.highWaterMark);
		
		this.keys = newKeys;
		this.hashes = newHashes;
	}

	public boolean get(byte[] key, long hash) {
		for (int i = 0; i < this.highWaterMark; i++) {	
			if (this.hashes[i] == hash) {
				if (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0) {			
					return true;
				}
			}
			
			if (this.hashes[i] > hash)
				return false;
		}
		return false;
	}

	public boolean remove(byte[] key, long hash) {
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if ((this.hashes[i] == hash) && (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0)) {
				this.hashes[i] = 0;

				int lengthToShift = (this.highWaterMark * this.bytesPerKey) - ((i + 1) * this.bytesPerKey);
				System.arraycopy(this.keys, (i + 1) * this.bytesPerKey, this.keys, i * this.bytesPerKey, lengthToShift);
				System.arraycopy(this.hashes, (i + 1), this.hashes, i, (this.highWaterMark - (i+1)));
				this.highWaterMark--;
				return true;
			}
		}
		return false;
	}

	public void removeKeys(LinkedList<Integer> positionsOfKeysToRemove) {
		if (positionsOfKeysToRemove.size() == 0)
			return;
		
		Integer positionToRemove;
		int lengthToShift;
		
		while ((positionToRemove = positionsOfKeysToRemove.poll()) != null) {
			lengthToShift = (this.highWaterMark * this.bytesPerKey) - ((positionToRemove + 1) * this.bytesPerKey);
			if (lengthToShift > 0)
				System.arraycopy(this.keys, (positionToRemove + 1) * this.bytesPerKey, this.keys, positionToRemove * this.bytesPerKey, lengthToShift);
			System.arraycopy(this.hashes, (positionToRemove + 1), this.hashes, positionToRemove, this.highWaterMark - (positionToRemove + 1));
			
			this.highWaterMark--;
		}
	}
	
	public void putFromSplit(byte[] key, long hash) {
		if (this.isFull())
			this.grow();

		int offset = this.highWaterMark * this.bytesPerKey;
		for (int i = 0; i < key.length; i++)
			this.keys[offset + i] = key[i];
		this.hashes[this.highWaterMark] = hash;
		this.highWaterMark++;
	}

	public void clear() {
		this.initialize();
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("# keys: " + this.highWaterMark + " keys:[" + Arrays.toString(this.keys) + "] hashes: [" + Arrays.toString(this.hashes) + "]");
		return retval.toString();
	}
	
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.bytesPerKey * this.highWaterMark;
			allocated += this.keys.length;
		}
		if (this.hashes != null) {
			used += this.highWaterMark * 8;
			allocated += this.hashes.length * 8; // 8 bytes per long
		}
		return new MemoryMeasurement(used, allocated);
	}
}
