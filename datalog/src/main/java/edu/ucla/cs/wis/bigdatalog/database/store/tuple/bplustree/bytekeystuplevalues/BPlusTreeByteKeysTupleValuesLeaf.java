package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.bytekeystuplevalues;

import java.io.Serializable;
import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStoreLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeByteKeysTupleValuesLeaf 
	extends BPlusTreeLeaf<BPlusTreeByteKeysTupleValuesLeaf> 
	implements BPlusTreeByteKeysTupleValuesPage, BPlusTreeTupleStoreLeaf, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected byte[] 	keys;
	protected ArrayList<Tuple>[] tuples;
	
	public BPlusTreeByteKeysTupleValuesLeaf() { super(); }
	
	public BPlusTreeByteKeysTupleValuesLeaf(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);
			
		this.keys = new byte[this.nodeSize];		
		this.tuples = new ArrayList[this.numberOfKeys];
	}
	
	public byte[] getKeys() { return this.keys; }
	
	@Override
	public ArrayList<Tuple>[] getTuples() { return this.tuples; }
	
	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.keys == null || this.isEmpty())
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);

		return key;
	}

	public Pair<BPlusTreeByteKeysTupleValuesPage, Boolean> insert(byte[] key, Tuple tuple) {	
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
					this.tuples[i].add(Tuple.copy(tuple));
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

		this.tuples[i] = new ArrayList<>();
		this.tuples[i].add(Tuple.copy(tuple));

		this.highWaterMark++;
		if (this.hasOverflow())
			return new Pair<>(this.split(), true);
			
		return new Pair<>(null, true);
	}
		
	private BPlusTreeByteKeysTupleValuesPage split() {
		BPlusTreeByteKeysTupleValuesLeaf rightLeaf = new BPlusTreeByteKeysTupleValuesLeaf(this.nodeSize, this.bytesPerKey);
		// give the right 1/2 of the children to the new leaf
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
	
	public void get(byte[] key, BPlusTreeTupleValuesGetResult result) {
		int compare;
		for (int i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compare == 0) {
				result.status = true;			
				result.tuples = this.tuples[i];
				return;
			}
			
			if (compare < 0)
				break;		
		}
		result.status = false;
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
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length + "\n");
		output.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.tuples.length + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
			for (int j = 0; j < this.tuples[i].size(); j++) {
				if (j > 0)
					output.append(",");
				output.append(this.tuples[i].get(j));
			}
			output.append("]\n");
		}
		return output.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			output.append(this.keys[i]);
			output.append("|");
			for (int j = 0; j < this.tuples[i].size(); j++) {
				if (j > 0)
					output.append(",");
				output.append(this.tuples[i].get(j));
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
			
			for (int i = 0; i < this.highWaterMark; i++) {
				MemoryMeasurement mmTuple;
				for (Tuple tuple : this.tuples[i]) {
					mmTuple = tuple.getSizeOf();
					used += mmTuple.getUsed();
					allocated += mmTuple.getAllocated();
				}
			}
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
