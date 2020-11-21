package edu.ucla.cs.wis.bigdatalog.database.index.key.hash;

import java.util.Arrays;
import java.util.LinkedList;

import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

/* APS 7/9/2014
 * LongKeyBucket keeps the entires ordered by key[].  Hash is not used for comparison, but kept for faster bucket splitting. */
public class LongKeyBucket implements MemorySize {
	private static final int FAST_GROW_SIZE = 8;
	private static final int INITIAL_SIZE = 0;
	private int highWaterMark;
	private long[] keys;
	private long[] hashes;
	
	public LongKeyBucket() {
		this.initialize();
	}
	
	private void initialize() {
		this.highWaterMark = 0;
		this.keys = new long[INITIAL_SIZE];
		this.hashes = new long[INITIAL_SIZE];
	}

	public boolean isEmpty() { return (this.highWaterMark == 0); }

	public boolean isFull() { return (this.highWaterMark == this.keys.length); }
	
	public int getNumberOfKeys() { return this.highWaterMark; }

	public long[] getKeys() { return this.keys; }
	
	public long[] getHashes() { return this.hashes; }

	public void setEntries(long[] keys, long hashes[]) {			
		if (keys.length < this.keys.length) {
			for (int i = 0; i < keys.length; i++) {
				this.hashes[i] = hashes[i];
				this.keys[i] = keys[i];
			}
		} else {
			this.keys = keys;
			this.hashes = hashes;			
		}

		this.highWaterMark = keys.length;
	}

	public boolean put(long key, long hash) {	
		// we keep an ordered list, so we can search it and know when to stop looking
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			// if this is true, we already have the key
			if (this.keys[i] == key)
				return false;

			if (this.keys[i] > key)
				break;		
		}
		
		if (this.isFull())
			this.grow();
		
		// only shift right if we're not already at the end
		if (i != this.highWaterMark) {
			// shift everything right one position up until 'i'
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.hashes, i, this.hashes, (i+1), (this.highWaterMark - i));
		}

		this.keys[i] = key;
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
		
		long[] newKeys = new long[newSize];		
		long[] newHashes = new long[newSize];

		System.arraycopy(this.keys, 0, newKeys, 0, this.highWaterMark);
		System.arraycopy(this.hashes, 0, newHashes, 0, this.highWaterMark);		
		
		this.keys = newKeys;
		this.hashes = newHashes;
	}

	public boolean get(long key, long hash) {
		for (int i = 0; i < this.highWaterMark; i++) {	
			if (this.keys[i] == key)			
				return true;
			
			if (this.keys[i] > key)
				return false;			
		}
		return false;
	}

	public boolean remove(long key, long hash) {
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				System.arraycopy(this.keys, (i + 1), this.keys, i , (this.highWaterMark - (i+1)));
				System.arraycopy(this.hashes, (i + 1), this.hashes, i, (this.highWaterMark - (i+1)));

				this.highWaterMark--;
				return true;
			}
		}
		return false;
	}
	
	public void remove(LinkedList<Integer> positionsOfKeysToRemove) {
		if (positionsOfKeysToRemove.size() == 0)
			return;
		
		Integer positionToRemove;
		
		while ((positionToRemove = positionsOfKeysToRemove.poll()) != null) {
			System.arraycopy(this.keys, (positionToRemove + 1), this.keys, positionToRemove, this.highWaterMark - (positionToRemove + 1));
			System.arraycopy(this.hashes, (positionToRemove + 1), this.hashes, positionToRemove, this.highWaterMark - (positionToRemove + 1));


			this.highWaterMark--;
		}
	}
	
	public void putFromSplit(long key, long hash) {
		if (this.isFull())
			this.grow();

		this.hashes[this.highWaterMark] = hash;
		this.keys[this.highWaterMark] = key;
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
			used += 8 * this.highWaterMark;
			allocated += this.keys.length * 8; // 8 bytes per long
		}
		if (this.hashes != null) {
			used += this.highWaterMark * 8;
			allocated += this.hashes.length * 8; // 8 bytes per long
		}
		return new MemoryMeasurement(used, allocated);
	}
}
