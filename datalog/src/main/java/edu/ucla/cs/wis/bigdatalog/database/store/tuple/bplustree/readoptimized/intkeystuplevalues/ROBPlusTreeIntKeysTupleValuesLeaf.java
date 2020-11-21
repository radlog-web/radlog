package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.intkeystuplevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeTupleStoreLeaf;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class ROBPlusTreeIntKeysTupleValuesLeaf 
	extends BPlusTreeLeaf<ROBPlusTreeIntKeysTupleValuesLeaf> 
	implements ROBPlusTreeIntKeysTupleValuesPage, ROBPlusTreeTupleStoreLeaf, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int[] keys;
	protected Tuple[][] tuples;
	
	public ROBPlusTreeIntKeysTupleValuesLeaf() { super(); }
	
	public ROBPlusTreeIntKeysTupleValuesLeaf(int nodeSize) {
		super(nodeSize, 4);

		// add 1 to the number of entries to hold an overflow that causes a split
		this.highWaterMark = 0;
		this.keys = new int[this.numberOfKeys];
		this.tuples = new Tuple[this.numberOfKeys][];
	}
	
	public int[] getKeys() { return this.keys; }
	
	@Override
	public Tuple[][] getTuples() { return this.tuples; }
	
	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.keys[0];
	}

	public Pair<ROBPlusTreeIntKeysTupleValuesPage, Boolean> insert(int key, Tuple tuple) {	
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			//compare = ByteArrayHelper.compare(key, this.keys, i * this.keySize, this.keySize);
			// if we have an exact match, update the value for the key
			if (this.keys[i] == key) {
				boolean found = false;
				for (int j = 0; j < this.tuples[i].length; j++) {
					if (this.tuples[i][j].equals(tuple)) {
						found = true;
						break;
					}
				}
				
				if (!found) {
					Tuple[] temp = new Tuple[this.tuples[i].length + 1];
					System.arraycopy(this.tuples[i], 0, temp, 0, this.tuples[i].length);
					temp[this.tuples[i].length] = Tuple.copy(tuple);
					this.tuples[i] = temp;
					return new Pair<>(null, true);
				}
				return new Pair<>(null, false);
			}
				
			if (key < this.keys[i])
				break;
		}

		if (i != this.highWaterMark) {
			//for (int j = this.highWaterMark; j > i; j--)this.keys[j] = this.keys[j - 1];
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.tuples, i, this.tuples, (i+1), (this.highWaterMark - i));
		}

		this.keys[i] = key;
		this.tuples[i] = new Tuple[1];
		this.tuples[i][0] = Tuple.copy(tuple);

		this.highWaterMark++;
		if (this.hasOverflow())
			return new Pair<>(this.split(), true);
			
		return new Pair<>(null, true);
	}
		
	private ROBPlusTreeIntKeysTupleValuesPage split() {
		ROBPlusTreeIntKeysTupleValuesLeaf rightLeaf = new ROBPlusTreeIntKeysTupleValuesLeaf(this.nodeSize);
		// give the right 1/2 of the children to the new leaf
		//int i;
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint, rightLeaf.keys, 0, numberToMove);
		// move values to new leaf
		System.arraycopy(this.tuples, splitPoint, rightLeaf.tuples, 0, numberToMove);
		
		this.highWaterMark -= numberToMove;
	
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	public void get(int key, ROBPlusTreeGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				result.success = true;
				result.tuples = this.tuples[i];
				return; 
			}	
		}
		result.success = false;
		/*
		int lowEnd = 0;
		int highEnd = this.highWaterMark - 1;
		int midPoint; 
		while (lowEnd <= highEnd) {
			// if we find the value already in the array, exit
			midPoint = lowEnd + ((highEnd - lowEnd) / 2);
			//System.out.println("searching for " + address + " lowEnd " + lowEnd + " midpoint " + midPoint + " highEnd " + highEnd);
			if (key < this.keys[midPoint])
				highEnd = midPoint - 1;
			else if (key > this.keys[midPoint])
				lowEnd = midPoint + 1;
			else {
				result.success = true;
				result.tuples = this.tuples[midPoint];
				return;
			}
		}		
		result.success = false;
		*/
	}
	
	@Override
	public boolean delete(int key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			//if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.keySize, this.keySize) == 0) {
			if (this.keys[deleteAt] == key) {
				//this.keys[deleteAt] = null;

				// move all keys and values after this position, left one key or value worth
				for (int j = deleteAt; j < this.highWaterMark; j++)
					this.keys[j] = this.keys[(j + 1)];
				
				//for (int a = 0; a < this.keySize; a++)
				//	this.keys[(j * this.keySize) + a] = this.keys[((j + 1) * this.keySize) + a];
				System.arraycopy(this.tuples, (deleteAt + 1), this.tuples, deleteAt, (this.highWaterMark - deleteAt));
				//for (int b = 0; b < this.bytesPerValue; b++)
				//	this.values[(j * this.bytesPerValue) + b] = this.values[((j + 1) * this.bytesPerValue) + b];
				//}				
				this.highWaterMark--;
				status = true;
				break;
			}
		}
		return status;
	}

	@Override
	public void deleteAll() {
		this.highWaterMark = 0;
		this.keys = null;
		this.tuples = null;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		//retval.append(buffer + this.pageType.name() + "(PageId: " + this.pageId + ")\n");
		retval.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length + "\n");
		retval.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.tuples.length + "\n");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			retval.append(buffer + "[");
			retval.append(this.keys[i]);
			retval.append("|");
			for (int j = 0; j < this.tuples[i].length; j++) {
				if (j > 0)
					retval.append(",");
				retval.append(this.tuples[i][j]);
			}
			retval.append("]\n");
		}
		return retval.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			output.append(this.keys[i]);
			output.append("|");
			//byte[] value = new byte[16];
			//System.arraycopy(this.values, (i * this.bytesPerValue), value, 0, 16);
			//output.append(DbLongLong.create(value).toString());
			for (int j = 0; j < this.tuples[i].length; j++) {
				if (j > 0)
					output.append(",");
				output.append(this.tuples[i][j]);
			}
			
			output.append("]");
		}
		return output.toString();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.highWaterMark * 4;
			allocated += this.keys.length * 4;
		}
		
		if (this.tuples != null) {
			used += this.highWaterMark * 4; // this is weak
			allocated += this.tuples.length * 4;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
