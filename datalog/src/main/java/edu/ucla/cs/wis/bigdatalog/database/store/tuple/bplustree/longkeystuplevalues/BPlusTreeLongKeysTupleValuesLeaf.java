package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.longkeystuplevalues;

import java.io.Serializable;
import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStoreLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeLongKeysTupleValuesLeaf 
	extends BPlusTreeLeaf<BPlusTreeLongKeysTupleValuesLeaf> 
	implements BPlusTreeLongKeysTupleValuesPage, BPlusTreeTupleStoreLeaf, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected long[] keys;
	protected ArrayList<Tuple>[] tuples;
	
	public BPlusTreeLongKeysTupleValuesLeaf() { super(); }
	
	public BPlusTreeLongKeysTupleValuesLeaf(int nodeSize) {
		super(nodeSize, 8);

		this.keys = new long[this.numberOfKeys];	
		this.tuples = new ArrayList[this.numberOfKeys];
	}
	
	public long[] getKeys() { return this.keys; }
	
	@Override
	public ArrayList<Tuple>[] getTuples() { return this.tuples; }

	@Override
	public long getLeftMostLeafKey() {
		if (this.keys == null || this.isEmpty())
			return Long.MIN_VALUE;
		
		return this.keys[0];
	}

	public Pair<BPlusTreeLongKeysTupleValuesPage, Boolean> insert(long key, Tuple tuple) {	
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			// if we have an exact match, update the value for the key
			if (this.keys[i] == key) {
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
				
			if (key < this.keys[i])
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.tuples, i, this.tuples, (i+1), (this.highWaterMark - i));
		}

		this.keys[i] = key;
		this.tuples[i] = new ArrayList<>();
		this.tuples[i].add(Tuple.copy(tuple));

		this.highWaterMark++;
		if (this.hasOverflow())
			return new Pair<>(this.split(), true);
			
		return new Pair<>(null, true);
	}
		
	private BPlusTreeLongKeysTupleValuesPage split() {
		BPlusTreeLongKeysTupleValuesLeaf rightLeaf = new BPlusTreeLongKeysTupleValuesLeaf(this.nodeSize);
		// give the right 1/2 of the children to the new leaf
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
	
	public void get(long key, BPlusTreeTupleValuesGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				result.status = true;
				result.tuples = this.tuples[i];
				return; 
			}
			
			if (key < this.keys[i])
				break;		
		}
		result.status = false;
	}
	
	@Override
	public boolean delete(long key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (this.keys[deleteAt] == key) {
				// move all keys and values after this position, left one key or value worth
				System.arraycopy(this.keys, (deleteAt + 1), this.keys, deleteAt, (this.highWaterMark - deleteAt));				
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
			output.append(this.keys[i]);
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
			used += this.highWaterMark * 8;
			allocated += this.keys.length * 8;
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
