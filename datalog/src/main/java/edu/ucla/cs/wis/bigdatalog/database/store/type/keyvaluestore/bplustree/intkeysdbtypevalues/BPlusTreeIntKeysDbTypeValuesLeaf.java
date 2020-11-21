package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeIntKeysDbTypeValuesLeaf 
	extends BPlusTreeLeaf<BPlusTreeIntKeysDbTypeValuesLeaf> 
	implements BPlusTreeIntKeysDbTypeValuesPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	private int[] keys;
	private DbTypeBase[] values;
	
	public BPlusTreeIntKeysDbTypeValuesLeaf() { super(); }
	
	public BPlusTreeIntKeysDbTypeValuesLeaf(int nodeSize) {
		super(nodeSize, 4);
		
		this.keys = new int[this.numberOfKeys];
		this.values = new DbTypeBase[this.numberOfKeys];
	}
	
	public int[] getKeys() { return this.keys; }
	
	public DbTypeBase[] getValues() { return this.values; }
	
	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.keys[0];
	}

	public void insert(int key, DbTypeBase value, BPlusTreeIntKeysDbTypeValuesInsertResult result) {	
		int i;		
		for (i = 0; i < this.highWaterMark; i++) {
			// if we have an exact match, update the value for the key
			if (this.keys[i] == key) {
				result.oldValue = this.values[i];
				this.values[i] = value;
				result.newPage = null;
				result.status = KeyValueOperationStatus.UPDATE;
				return;
			}
				
			if (key < this.keys[i])
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.values, i, this.values, (i+1), (this.highWaterMark - i));
		}

		this.keys[i] = key;
		this.values[i] = value;

		this.highWaterMark++;
		
		result.status = KeyValueOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;
	}
		
	private BPlusTreeIntKeysDbTypeValuesPage split() {
		BPlusTreeIntKeysDbTypeValuesLeaf rightLeaf = new BPlusTreeIntKeysDbTypeValuesLeaf(this.nodeSize);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint, rightLeaf.keys, 0, numberToMove);
		// move values to new leaf
		System.arraycopy(this.values, splitPoint, rightLeaf.values, 0, numberToMove);
		
		this.highWaterMark -= numberToMove;

		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	@Override
	public void get(int key, BPlusTreeIntKeysDbTypeValuesGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {		
			if (key == this.keys[i]) {
				result.success = true;
				result.value = this.values[i];
				return; 
			}

			if (key < this.keys[i])
				break;	
		}
		result.success = false;
	}
	
	@Override
	public boolean delete(int key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (this.keys[deleteAt] == key) {
				// move all keys and values after this position, left one key or value worth
				System.arraycopy(this.keys, (deleteAt + 1), this.keys, deleteAt, (this.highWaterMark - deleteAt));
				System.arraycopy(this.values, (deleteAt + 1), this.values, deleteAt, (this.highWaterMark - deleteAt));
			
				this.highWaterMark--;
				status = true;
				break;
			}
		}
		return status;
	}

	@Override
	public void deleteAll() {
		this.keys = null;
		this.next = null;
		this.highWaterMark = 0;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length + "\n");
		output.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.values.length + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "[");
			output.append(this.keys[i]);
			output.append("|");
			output.append(this.values[i]);
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
			output.append(this.values[i]);
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
		
		if (this.values != null) {
			int size = 0;
			if (this.values[0] != null)
				size = this.values[0].getDataType().getNumberOfBytes();
			used += this.highWaterMark * size;
			allocated += this.values.length;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
	/*
	@Override
	public DbLongLong sumAllValues() {
		DbLongLong sum = DbLongLong.create(0);
		for (int i = 0; i < this.highWaterMark; i++)
			sum = sum.add(this.values[i]);		
		return sum;
	}*/
}
