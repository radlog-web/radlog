package edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap;

import java.io.Serializable;
import java.util.BitSet;

public class IntBitMap implements Serializable {
	private static final long serialVersionUID = 1L;
	public BitSet negativeNumbers;
	public BitSet positiveNumbers;
	public boolean readingPositiveNumbers;
	
	public IntBitMap() { 
		//this.negativeNumbers = new BitSet();
		//this.positiveNumbers = new BitSet();
	}
	
	public void set(int key) {
		if (key < 0) {
			if (this.negativeNumbers == null)
				this.negativeNumbers = new BitSet();
			this.negativeNumbers.set(-key);
		} else {
			if (this.positiveNumbers == null)
				this.positiveNumbers = new BitSet();
			this.positiveNumbers.set(key);
		}
	}
	
	public boolean get(int key) {
		if (key < 0) {
			if (this.negativeNumbers == null)
				return false;
			return this.negativeNumbers.get(-key);
		}
		if (this.positiveNumbers == null)
			return false;
		return this.positiveNumbers.get(key);
	}

	public int getNumberOfEntries() {
		int numberOfEntries = 0;
		if (this.negativeNumbers != null)
			numberOfEntries += this.negativeNumbers.cardinality();
		
		if (this.positiveNumbers != null)
			numberOfEntries += this.positiveNumbers.cardinality();
		
		return numberOfEntries;
	}
	
	public boolean isEmpty() {
		boolean isEmpty = true;
		if (this.negativeNumbers != null)
			isEmpty = this.negativeNumbers.isEmpty();
		
		if (isEmpty && this.positiveNumbers != null)
			isEmpty = this.positiveNumbers.isEmpty();
		
		return isEmpty;
	}
	
	public void clear(int key) {
		if (key < 0) {
			if (this.negativeNumbers != null)
				this.negativeNumbers.clear(key);
		} else {
			if (this.positiveNumbers != null)
				this.positiveNumbers.clear(key);
		}
	}
	
	public void clear() {
		if (this.negativeNumbers != null)
			this.negativeNumbers.clear();
		if (this.positiveNumbers != null)
			this.positiveNumbers.clear();
	}
	
	public void deleteAll() {
		this.negativeNumbers = null;
		this.positiveNumbers = null;
	}
	
	public int length() {
		int length = 0;
		if (this.negativeNumbers != null)
			length = this.negativeNumbers.length();
		
		if (this.positiveNumbers != null)
			length += this.positiveNumbers.length();
		return length;	 
	}
	
	public int size() { 
		int size = 0;
		if (this.negativeNumbers != null)
			size = this.negativeNumbers.size();
		
		if (this.positiveNumbers != null)
			size += this.positiveNumbers.size();
		return size;	 
	}	 
		
	public String toString() {
		StringBuilder output = new StringBuilder();
		if (this.negativeNumbers != null)
			output.append(this.negativeNumbers.toString());
		
		if (this.positiveNumbers != null) {
			if (output.length() > 0)
				output.append(" ");
			output.append(this.positiveNumbers.toString());
		}
		return output.toString();
	}
	
	public int nextSetBit(int fromIndex) {
		if (this.readingPositiveNumbers) {
			if (this.negativeNumbers != null) {
				int setBit = this.negativeNumbers.nextSetBit(fromIndex);
				if (setBit > -1)
					return -setBit;				
			}
			
			this.readingPositiveNumbers = true;
		}
		
		if (this.positiveNumbers == null)
			return -1;
		
		return this.positiveNumbers.nextSetBit(fromIndex);		
	}
}
