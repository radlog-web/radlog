package edu.ucla.cs.wis.bigdatalog.database.index.secondary;

import java.io.Serializable;

public class OrderedTupleAddressArray extends TupleAddressArray implements Serializable {
	private static final long serialVersionUID = 1L;

	//public OrderedTupleAddressArray() {super();}
		
	public boolean get(int address) {
		//System.out.println("getting address " + address);
		if (this.isEmpty())
			return false;

		if (address > this.addressArray[this.highWaterMark - 1])
			return false;
		
		// otherwise, we check the array using a binary search
		int lowEnd = 0;
		int highEnd = this.highWaterMark - 1;
		int midPoint; 
		while (lowEnd <= highEnd) {
			// if we find the value already in the array, exit
			midPoint = lowEnd + ((highEnd - lowEnd) / 2);
			//System.out.println("searching for " + address + " lowEnd " + lowEnd + " midpoint " + midPoint + " highEnd " + highEnd);
			if (this.addressArray[midPoint] > address)
				highEnd = midPoint - 1;
			else if (this.addressArray[midPoint] < address)
				lowEnd = midPoint + 1;	
			else
				return true;
		}
		return false;
	}
	
	public boolean put(int address) {		
		//System.out.println("putting address " + address);
		// if we already have the address in our array, we stop
		if (this.get(address)) 
			return false;
		
		// expand array if full
		if (this.isFull())
			this.grow();
		
		// get position to insert at
		// move other addresses 1 position to the right of insertion point
		// insert new address
		// if array was full, push last address to next overflow array

		int position = 0;
		for (; position < this.highWaterMark; position++)
			if (this.addressArray[position] > address)
				break;
			
		// shift everything at position and after 1 space right, but do not overflow
		for (int i = this.highWaterMark; i > position; i--)
			this.addressArray[i] = this.addressArray[i - 1];

		this.addressArray[position] = address;
		//if (this.highWaterMark == 2)System.out.println("two");

		this.highWaterMark++;
		return true;
	}
	
	public boolean remove(int address) {
		int position = 0;
		for (; position < this.highWaterMark; position++) {
			if (this.addressArray[position] == address)
				break;
			
			// if we've passed the value, we won't find it, so exit
			if (this.addressArray[position] > address)
				return false;
		}

		if (position < this.highWaterMark) {
			int end = this.highWaterMark;
					
			if (this.highWaterMark == this.addressArray.length)
				end--;
			
			// shift all addresses after this one 1 position to the left
			for (int i = position; i < end; i++)
				this.addressArray[i] = this.addressArray[i + 1];

			this.highWaterMark--;
			return true;
		}
		
		return false;
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
}