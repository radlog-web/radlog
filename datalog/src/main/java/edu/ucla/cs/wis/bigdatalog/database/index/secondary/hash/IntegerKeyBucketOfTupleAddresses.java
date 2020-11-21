package edu.ucla.cs.wis.bigdatalog.database.index.secondary.hash;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.OrderedTupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.UnorderedTupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class IntegerKeyBucketOfTupleAddresses implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;

	private static final int FAST_GROW_SIZE = 8;
	private static final boolean USE_ORDERED_ARRAY = true;
	private static final int INITIAL_NUMBER_OF_KEYS = 0;
	private int highWaterMark;
	private long[] hashes;
	private int[] keys;
	private TupleAddressArray[] entryArray;

	public IntegerKeyBucketOfTupleAddresses() {	
		this.initialize();
	}
	
	private void initialize() {
		this.highWaterMark = 0;
		this.keys = new int[INITIAL_NUMBER_OF_KEYS];	
		this.hashes = new long[INITIAL_NUMBER_OF_KEYS];
				
		if (USE_ORDERED_ARRAY)
			this.entryArray = new OrderedTupleAddressArray[INITIAL_NUMBER_OF_KEYS];
		else
			this.entryArray = new UnorderedTupleAddressArray[INITIAL_NUMBER_OF_KEYS];
	}
	
	public boolean isEmpty() { return (this.highWaterMark == 0); }
	
	public boolean isFull() { return (this.highWaterMark == this.entryArray.length); }

	public int getNumberOfKeys() { return this.highWaterMark; }
	
	public int[] getKeys() { return this.keys; }
	
	public long[] getHashes() { return this.hashes; }
	
	public TupleAddressArray[] getEntries() { return this.entryArray; }
			
	public void setEntries(int[] keys, long[] hashes, TupleAddressArray[] entries) { 		
		// if the entries being added are few, copy them to existing array which should be 'initialSize'
		if (keys.length < this.keys.length) {
			for (int i = 0; i < keys.length; i++) {
				this.keys[i] = keys[i];
				this.hashes[i] = hashes[i];
				this.entryArray[i] = entries[i];
			}
		} else {
			this.keys = keys;
			this.hashes = hashes;
			this.entryArray = entries;			
		}
		this.highWaterMark = keys.length;
	}
	
	public int getNumberOfTupleAddresses() {
		int numberOfTupleAddresses = 0;

		for (int i = 0; i < this.highWaterMark; i++)
			numberOfTupleAddresses += this.entryArray[i].getNumberOfAddresses();
				
		return numberOfTupleAddresses;
	}
	
	public boolean put(int key, long hash, int address) {
		// we keep an ordered list by key
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key)		
				return this.entryArray[i].put(address);
							
			if (this.keys[i] > key)
				break;
		}

		// if we reached here, we're adding another key to this bucket
		// if bucket is full, we need to grow it first
		if (this.isFull())
			this.grow();

		// only shift right if we're not already at the end
		if (i != this.highWaterMark) {
			// shift everything right one position up until 'i'
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.hashes, i, this.hashes, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.entryArray, i, this.entryArray, (i+1), (this.highWaterMark - i));
		}
		
		// always replace the array that used to be here - the keys are in the array - crap design me
		if (USE_ORDERED_ARRAY)
			this.entryArray[i] = new OrderedTupleAddressArray();			
		else
			this.entryArray[i] = new UnorderedTupleAddressArray();			

		this.keys[i] = key;
		this.hashes[i] = hash;
		this.entryArray[i].put(address);
		
		this.highWaterMark++;
		return true;
	}
	
	public void putFromSplit(int key, long hash, TupleAddressArray entries) {
		if (this.isFull())
			this.grow();

		this.keys[this.highWaterMark] = key;
		this.hashes[this.highWaterMark] = hash;
		this.entryArray[this.highWaterMark] = entries;
		this.highWaterMark++;
	}
	
	private void grow() {
		int newSize = this.entryArray.length;
		if (newSize < 1)
			newSize = 1;
		else if (newSize < FAST_GROW_SIZE)
			newSize *= 2;
		else
			newSize *= 1.25;
		
		TupleAddressArray[] newEntryArray;
		if (USE_ORDERED_ARRAY)
			newEntryArray = new OrderedTupleAddressArray[newSize];
		else
			newEntryArray = new UnorderedTupleAddressArray[newSize];
		
		int[] newKeys = new int[newSize];
		long[] newHashes = new long[newSize];
		System.arraycopy(this.keys, 0, newKeys, 0, this.highWaterMark);
		System.arraycopy(this.hashes, 0, newHashes, 0, this.highWaterMark);
		System.arraycopy(this.entryArray, 0, newEntryArray, 0, this.highWaterMark);

		this.keys = newKeys;
		this.hashes = newHashes;
		this.entryArray = newEntryArray;
	}

	public TupleAddressArray get(int key) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key)
				return entryArray[i];
			
			if (this.keys[i] > key)
				return null;
		}
		return null;
	}

	public boolean remove(int key, int address) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				if (!this.entryArray[i].remove(address))
					throw new DatabaseException("Can not remove from bucket!");
				
				// if the entry is empty, we remove it from the list
				if (this.entryArray[i].isEmpty()) {
					this.hashes[i] = 0;
					this.entryArray[i] = null;
					
					System.arraycopy(this.keys, (i + 1), this.keys, i , (this.highWaterMark - (i+1)));
					System.arraycopy(this.hashes, (i + 1), this.hashes, i, (this.highWaterMark - (i+1)));
					System.arraycopy(this.entryArray, (i + 1), this.entryArray, i, (this.highWaterMark - (i+1)));	

					this.highWaterMark--;
				}
				
				return true;
			} else if (this.keys[i] > key) {
				return false;
			}
		}
		return false;
	}
	
	public void remove(HashSet<Integer> positionsOfKeysToRemove) {
		if (positionsOfKeysToRemove.size() == 0)
			return;
		
		int newSize = this.highWaterMark - positionsOfKeysToRemove.size();
		
		if (newSize > 0) {
			int[] newKeys = new int[newSize];
			long[] newHashes = new long[newSize];
			TupleAddressArray[] newEntryArray = new TupleAddressArray[newSize];
			
			int j = 0;
			for (int i = 0; i < this.highWaterMark; i++) {
				if (!positionsOfKeysToRemove.contains(i)) {
					newKeys[j] = this.keys[i];
					newHashes[j] = this.hashes[i];
					newEntryArray[j] = this.entryArray[i];
					j++;
				}
			}
			
			this.keys = newKeys;
			this.hashes = newHashes;
			this.entryArray = newEntryArray;
			this.highWaterMark -= positionsOfKeysToRemove.size();
		} else {
			this.initialize();
		}
	}
	
	public void clear() {
		this.initialize();
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("# keys: " + this.highWaterMark + ", # addresses: " + this.getNumberOfTupleAddresses() + " keys:[" + Arrays.toString(this.keys) + "] hashes:[" + Arrays.toString(this.hashes) + "]");

		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(",");		
			retval.append(this.entryArray[i].toString());
		}
		retval.append("]");

		return retval.toString();
	}
	
	public String toString(AddressedTupleStore tupleStore) {
		StringBuilder retval = new StringBuilder();
		retval.append("# keys: " + this.highWaterMark + ", # addresses: " + this.getNumberOfTupleAddresses() + " keys:[" + Arrays.toString(this.keys) + "] hashes:[" + Arrays.toString(this.hashes) + "]");

		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(",");
			retval.append(this.entryArray[i].toString(tupleStore));
		}
		retval.append("]");

		return retval.toString();
	}
		
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		
		if (this.hashes != null) {
			used += this.highWaterMark * 8; // 8 bytes per long
			allocated += this.hashes.length * 8; // 8 bytes per long
		}
		
		if (this.keys != null) {
			used += this.highWaterMark * 4; // 4 bytes per integer
			allocated += this.keys.length * 4; // 4 bytes per integer
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
