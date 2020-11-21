package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes.DbTypeAggregatorHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes.DbTypeNeedsInitialization;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class AggregatorBPlusTreeByteKeysDbTypeValuesLeaf 
	extends BPlusTreeLeaf<AggregatorBPlusTreeByteKeysDbTypeValuesLeaf> 
	implements AggregatorBPlusTreeByteKeysDbTypeValuesPage, Serializable { 
	private static final long serialVersionUID = 1L;
	
	private byte[] keys;
	protected DbTypeBase[] values;
	protected DbTypeAggregatorHelper aggregator;

	public AggregatorBPlusTreeByteKeysDbTypeValuesLeaf() { super(); }
	
	public AggregatorBPlusTreeByteKeysDbTypeValuesLeaf(int nodeSize, int bytesPerKey, DbTypeAggregatorHelper aggregator) {
		super(nodeSize, bytesPerKey);

		this.highWaterMark = 0;
		// same number of keys and values
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey];		
		this.values = new DbNumericType[this.numberOfKeys];
		this.aggregator = aggregator;
	}
	
	public byte[] getKeys() { return this.keys; }
	
	public DbTypeBase[] getValues() { return this.values; }
	
	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}

	@Override
	public void insert(byte[] key, DbTypeBase value, AggregatorBPlusTreeByteKeysDbTypeValuesInsertResult result) {
		int i;
		int compare;
		for (i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			// if we have an exact match, do aggregation and update the value for the key
			if (compare == 0) {
				DbTypeBase oldValue = this.values[i];
				DbTypeBase newValue = this.aggregator.doAggregation(oldValue, value);
				// if aggregation changes value, update and return old value
				if (newValue != oldValue) {
					this.values[i] = newValue;
					result.newPage = null;
					result.status = AggregatorInsertStatus.UPDATE;
					return;
				} else if (this.aggregator.aggregateType == AggregateFunctionType.COUNT_DISTINCT) {
					this.values[i] = newValue;
					result.newPage = null;
					result.status = AggregatorInsertStatus.UPDATE;
					return;
				}
				result.newPage = null;
				result.status = AggregatorInsertStatus.FAIL;
				return;
			}
				
			if (compare < 0)
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
			System.arraycopy(this.values, i, this.values, (i+1), (this.highWaterMark - i));
		}

		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);
		if (this.aggregator instanceof DbTypeNeedsInitialization)
			this.values[i] = ((DbTypeNeedsInitialization)this.aggregator).doAggregation(value);
		else
			this.values[i] = value.copy(); 
		this.highWaterMark++;
		
		result.status = AggregatorInsertStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();			
			return;
		}
		result.newPage = null;		
	}
		
	private AggregatorBPlusTreeByteKeysDbTypeValuesPage split() {
		AggregatorBPlusTreeByteKeysDbTypeValuesLeaf rightLeaf 
			= new AggregatorBPlusTreeByteKeysDbTypeValuesLeaf(this.nodeSize, this.bytesPerKey, this.aggregator);
		// give the right 1/2 of the children to the new leaf

		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
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
	public void get(byte[] key, AggregatorBPlusTreeByteKeysDbTypeValuesGetResult result) {
		int compare;
		for (int i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compare == 0) {
				result.success = true;				
				result.value = this.values[i];
				return;
			}
			
			if (compare < 0)
				break;
		}
		result.success = false;
	}
	
	@Override
	public void get(byte[] startKey, byte[] endKey, AggregatorBPlusTreeByteKeysDbTypeValuesGetResultRange result) {
		int compareStart;
		int compareEnd = 0;
		for (int i = 0; i < this.highWaterMark; i++) {
			compareStart = ByteArrayHelper.compare(startKey, this.keys, i * this.bytesPerKey, startKey.length);						
			//startKey <= this.keys[i] && this.keys[i] <= endKey
			if (compareStart <= 0) {
				compareEnd = ByteArrayHelper.compare(endKey, this.keys, i * this.bytesPerKey, endKey.length);
				if (compareEnd >= 0) {
					result.success = true;
					result.index = i;
					result.leaf = this;
					return;
				}
			}				
			
			if (compareEnd < 0)
				break;
		}
		result.success = false;		
	}	
	
	@Override
	public boolean delete(byte[] key) {
		//System.out.println("bplustree leaf delete("+key+") @ " +this.getHeight());
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - deleteAt) * this.bytesPerKey);
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
			for (int j = 0; j < this.bytesPerKey; j++) 
				retval.append(this.keys[(i * this.bytesPerKey) + j]);
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
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
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
			used += this.highWaterMark * this.bytesPerKey;
			allocated += this.keys.length * this.bytesPerKey;
		}
		
		if (this.values != null) {
			int sizeOf = 8;
			if (this.values.length > 0 && this.values[0] != null)
				sizeOf = this.values[0].getDataType().getNumberOfBytes();
			
			used += this.highWaterMark * sizeOf;
			allocated += this.values.length * sizeOf;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
