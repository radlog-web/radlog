package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class AggregatorBPlusTreeIntKeysIntValuesLeaf 
	extends BPlusTreeLeaf<AggregatorBPlusTreeIntKeysIntValuesLeaf> 
	implements AggregatorBPlusTreeIntKeysIntValuesPage, Serializable { 
	
	private static final long serialVersionUID = 1L;
	
	private int[] keys;
	private int[] values;
	
	protected AggregateFunctionType aggregateType;

	public AggregatorBPlusTreeIntKeysIntValuesLeaf() { super(); }
	
	public AggregatorBPlusTreeIntKeysIntValuesLeaf(int nodeSize, AggregateFunctionType aggregateType) {
		super(nodeSize, 4);
		
		this.aggregateType = aggregateType;
		this.highWaterMark = 0;
		// same number of keys and values
		this.keys = new int[this.numberOfKeys];		
		this.values = new int[this.numberOfKeys];
	}
	
	public int[] getKeys() { return this.keys; }
	
	public int[] getValues() { return this.values; }
	
	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.keys[0];
	}

	@Override
	public void insert(int key, int value, AggregatorBPlusTreeIntKeysIntValuesInsertResult result) {	
		int i;		
		boolean success = false;
		for (i = 0; i < this.highWaterMark; i++) {
			// if we have an exact match, do aggregation and update the value for the key
			if (this.keys[i] == key) {
				int oldValue = this.values[i];
				//int newValue = this.aggregator.doAggregation(oldValue, value);
				//int newValue = this.doAggregation(this.values[i], value);
				switch (this.aggregateType) {
					case FSSUM:
						success = ((value > 0) && (value > oldValue));
						break;
					case MAX:
					case FSMAX:
						success = (value > oldValue);
						break;
					case MIN:
					case FSMIN:
						success = (value < oldValue);
						break;
					default: //case SUM:
						value = value + oldValue;
						success = true;
						break;
				}
				
				// if aggregation changes value, update and return old value
				//if (newValue != oldValue) {
				if (success) {
					this.values[i] = value;
					result.newPage = null;
					result.status = AggregatorInsertStatus.UPDATE;
					return; 
				}
				result.newPage = null;
				result.status = AggregatorInsertStatus.FAIL;
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
		//if (this.aggregator instanceof IntNeedsInitialization)
		//	this.values[i] = ((IntNeedsInitialization)this.aggregator).doAggregation(value);
		/*if (this.aggregator instanceof Count)
			this.values[i] = 1;
		else
			this.values[i] = value;
		*/
		//this.values[i] = this.doAggregationNoOld(value);
		if (this.aggregateType == AggregateFunctionType.FSSUM)
			this.values[i] = (value > 0) ? value : 0;			
		else
			/*case MAX: case FSMAX: case MIN: case FSMIN: case SUM:*/	
			this.values[i] = value;
	
		this.highWaterMark++;
		result.status = AggregatorInsertStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		
		result.newPage = null;
	}
		
	private AggregatorBPlusTreeIntKeysIntValuesPage split() {
		AggregatorBPlusTreeIntKeysIntValuesLeaf rightLeaf = new AggregatorBPlusTreeIntKeysIntValuesLeaf(this.nodeSize, this.aggregateType);
		// give the right 1/2 of the children to the new leaf
		//int i;
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
	public void get(int key, AggregatorBPlusTreeIntKeysIntValuesGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				result.value = this.values[i];
				result.success = true;
				return;
			}
			
			if (this.keys[i] > key)
				break;
		}
		result.success = false;
	}
	
	@Override
	public void get(int startKey, int endKey, AggregatorBPlusTreeIntKeysIntValuesGetResultRange result) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (startKey <= this.keys[i] && this.keys[i] <= endKey) {
				result.success = true;
				result.index = i;
				result.leaf = this;
				return;
			}				
			
			if (endKey < this.keys[i])
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
		this.values = null;
		this.highWaterMark = 0;
		this.next = null;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		retval.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length + "\n");
		retval.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.values.length + "\n");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			retval.append(buffer + "[");
			retval.append(this.keys[i]);
			retval.append("|");
			retval.append(this.values[i]);
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
			used += this.highWaterMark * 4;
			allocated += this.values.length * 4;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
