package edu.ucla.cs.wis.bigdatalog.database.store.keyvalue;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class BucketOfKeyValues implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final int HASH = 0;
	public static final int KEY = 1;
	public static final int VALUE_ADDRESS = 2; 

	// int[HASH] - hash for key
	// int[KEY] - key
	// int[VALUE_ADDRESS] - value's address in valueStore
	private int size;
	private List<long[]> list;
	
	public BucketOfKeyValues() {
		this.size = 0;
		this.list = new LinkedList<>();
	}
	
	public int getSize() { return this.size; }
	
	public int getNumberOfEntries() { return this.list.size(); }
		
	public long[] getEntry(int position) { return this.list.get(position); }
	
	public void put(long hash, long key, long valueAddress) {
		// we keep an ordered list, so we can search it
		int position = 0;
		for (long[] entry : this.list) {
			if (entry[HASH] == hash && entry[KEY] == key)
				entry[VALUE_ADDRESS] = valueAddress;
			if (entry[HASH] > hash)
				break;
			position++;
		}
	
		this.list.add(position, new long[]{hash, key, valueAddress});
		size++;
	}
	
	public void prepend(long[] entry) {
		this.list.add(0, entry);
		this.size++;
	}
	
	public long get(long hash, long key) {
		for (long[] entry : this.list) {
			if (entry[HASH] == hash && entry[KEY] == key)
				return entry[VALUE_ADDRESS];
			if (entry[HASH] > hash)
				return -1;
		}
		return -1;
	}

	public boolean remove(long hash, long key) {
		long[] entry;
		for (int i = this.list.size() - 1; i >= 0; i--) {
			entry = this.list.get(i);
			if (entry[HASH] == hash && entry[KEY] == key) {
				this.list.remove(i);			
				size--;
				return true;
			} else if (entry[HASH] < hash){
				return false;
			}
		}
		return false;
	}
	
	// for bucket splitting
	public void remove(long[] entry) {
		this.list.remove(entry);
		this.size--;				
	}
		
	public void clear() {
		this.list.clear();
		this.size = 0;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("size: " + size + ", list:[");

		int i = 0;
		for (long[] entry : this.list) {
			if (i > 0)
				retval.append(",");
			retval.append("hash: " + String.valueOf(entry[HASH]) + ", key: " + String.valueOf(entry[KEY]) + ", value address: " + String.valueOf(entry[VALUE_ADDRESS]));
			i++;
		}
		retval.append("]");
		
		return retval.toString();
	}
}
