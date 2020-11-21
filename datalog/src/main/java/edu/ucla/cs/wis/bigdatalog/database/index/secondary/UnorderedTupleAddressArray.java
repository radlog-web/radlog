package edu.ucla.cs.wis.bigdatalog.database.index.secondary;

import java.io.Serializable;

public class UnorderedTupleAddressArray extends TupleAddressArray implements Serializable {
	private static final long serialVersionUID = 1L;

	//public UnorderedTupleAddressArray() { super(); }
		
	public boolean get(int address) {
		//System.out.println("getting address " + address);
		if (this.isEmpty())
			return false;
		
		for (int i = 0; i < this.highWaterMark; i++)
			if (this.addressArray[i] == address)
				return true;
				
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
	
		// insert new address at highwatermark
		this.addressArray[this.highWaterMark++] = address;
		return true;
	}
	
	public boolean remove(int address) {
		int position = 0;
		for (; position < this.highWaterMark; position++)
			if (this.addressArray[position] == address)
				break;
		
		if (position < this.highWaterMark) {
			int end = this.highWaterMark;
					
			if (this.isFull())
				end--;
			
			// shift all addresses after this one 1 position to the left
			for (int i = position; i < end; i++)
				this.addressArray[i] = this.addressArray[i + 1];

			this.highWaterMark--;
			return true;
		}
		
		return false;
	}
}