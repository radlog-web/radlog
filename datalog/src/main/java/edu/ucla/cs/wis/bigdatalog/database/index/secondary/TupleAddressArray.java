package edu.ucla.cs.wis.bigdatalog.database.index.secondary;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public abstract class TupleAddressArray implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	protected static final int FAST_GROW_SIZE = 8;
	private static final int INITIAL_SIZE = 0;
	
	protected 	int[] addressArray;
	protected 	int highWaterMark;
	
	public TupleAddressArray() {
		this.initialize();	
	}
		
	private void initialize() {
		this.addressArray = new int[INITIAL_SIZE];
		this.highWaterMark = 0;
	}
	
	// return an integer array of addresses
	public int[] getAddresses() { 
		return Arrays.copyOf(this.addressArray, this.highWaterMark);
		//return this.addressArray; 
	}
	
	public boolean isEmpty() { return (this.highWaterMark == 0); }
	
	public boolean isFull() { return (this.highWaterMark == this.addressArray.length); }
	
	public int getNumberOfAddresses() { return this.highWaterMark; }	
	
	protected void grow() {
		int newSize = this.addressArray.length;
		// slow down the rate of growth if the array is getting large
		if (newSize < 1)
			newSize = 1;
		else if (newSize < FAST_GROW_SIZE)
			newSize *= 2;
		else
			newSize *= 1.25;
		
		int[] newAddressArray = new int[newSize];
		
		for (int i = 0; i < this.addressArray.length; i++)
			newAddressArray[i] = this.addressArray[i];
		
		this.addressArray = newAddressArray;
	}
	
	public void deleteAll() {
		this.addressArray = null;
		this.highWaterMark = 0;		
	}
	
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.addressArray != null) {
			used += this.highWaterMark * 4; // 4 bytes per integer
			allocated += this.addressArray.length * 4; // 4 bytes per integer
		}
		return new MemoryMeasurement(used, allocated);
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("size: " + this.getNumberOfAddresses() + " => [");
		
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(", ");
			retval.append(this.addressArray[i]);
		}
		retval.append("]");		
		return retval.toString();
	}

	public String toString(AddressedTupleStore tupleStore) {
		StringBuilder retval = new StringBuilder();
		retval.append("size: " + this.getNumberOfAddresses() + " => [");
		
		AddressedTuple tuple = (AddressedTuple) tupleStore.getEmptyTuple();
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(", ");
			if (tupleStore.get(this.addressArray[i], tuple) > 0)
				retval.append(tuple.toString());
		}
		retval.append("]");
		return retval.toString();
	}
	
	abstract public boolean put(int address); 
	
	abstract public boolean get(int address);
	
	abstract public boolean remove(int address);
}