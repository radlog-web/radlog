package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.bytekeystuplevalues;

import java.io.Serializable;
import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeTupleStoreLeaf;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class ROBPlusTreeByteKeysTupleValuesLeaf 
	extends BPlusTreeLeaf<ROBPlusTreeByteKeysTupleValuesLeaf> 
	implements ROBPlusTreeByteKeysTupleValuesPage, ROBPlusTreeTupleStoreLeaf, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected byte[] 	keys;
	protected Tuple[][] tuples;
	
	public ROBPlusTreeByteKeysTupleValuesLeaf() { super(); }
	
	public ROBPlusTreeByteKeysTupleValuesLeaf(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);
		
		// add 1 to the number of entries to hold an overflow that causes a split
		this.highWaterMark = 0;
		this.keys = new byte[this.nodeSize];		
		this.tuples = new Tuple[this.numberOfKeys][];
	}
	
	public byte[] getKeys() { return this.keys; }
	
	@Override
	public Tuple[][] getTuples() { return this.tuples; }
	
	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
		return key;
	}

	public Pair<ROBPlusTreeByteKeysTupleValuesPage, Boolean> insert(byte[] key, Tuple tuple) {	
		int compare;
		int i;	
		for (i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			// if we have an exact match, update the value for the key
			if (compare == 0) {
				boolean found = false;
				for (Tuple match : this.tuples[i]) {
					if (match.equals(tuple)) {
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
				
			if (compare < 0)
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
			System.arraycopy(this.tuples, i, this.tuples, (i+1), (this.highWaterMark - i));
		}

		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);

		this.tuples[i] = new Tuple[1];
		this.tuples[i][0] = Tuple.copy(tuple);

		this.highWaterMark++;
		if (this.hasOverflow())
			return new Pair<>(this.split(), true);
			
		return new Pair<>(null, true);
	}
		
	private ROBPlusTreeByteKeysTupleValuesPage split() {
		ROBPlusTreeByteKeysTupleValuesLeaf rightLeaf = new ROBPlusTreeByteKeysTupleValuesLeaf(this.nodeSize, this.bytesPerKey);
		// give the right 1/2 of the children to the new leaf
		//int i;
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
		// move values to new leaf
		System.arraycopy(this.tuples, splitPoint, rightLeaf.tuples, 0, numberToMove);
		
		this.highWaterMark -= numberToMove;
	
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	public void get(byte[] key, ROBPlusTreeGetResult result) {
		int compare;
		for (int i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compare == 0) {
				result.success = true;
				result.tuples = this.tuples[i];
				return;
			}	
		}
		result.success = false;
	}
	
	@Override
	public boolean delete(byte[] key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - deleteAt) * this.bytesPerKey);			
				System.arraycopy(this.tuples, (deleteAt + 1), this.tuples, deleteAt, (this.highWaterMark - deleteAt));

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
			for (int j = 0; j < this.bytesPerKey; j++) 
				retval.append(this.keys[(i * this.bytesPerKey) + j]);
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
			used += this.highWaterMark * this.bytesPerKey;
			allocated += this.keys.length;
		}
		
		if (this.tuples != null) {
			used += 4+8; // 4 bytes for array length, 8 bytes for pointer
			used += this.highWaterMark * 8; // 8 bytes for each pointer
			allocated += 4+8; // 4 bytes for array length, 8 bytes for pointer
			allocated += this.tuples.length * 8;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
