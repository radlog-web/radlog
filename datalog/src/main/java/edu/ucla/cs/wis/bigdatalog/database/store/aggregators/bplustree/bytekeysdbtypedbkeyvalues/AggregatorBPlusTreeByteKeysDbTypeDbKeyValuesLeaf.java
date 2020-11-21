package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf 
	extends BPlusTreeLeaf<AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf> 
	implements AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesPage, Serializable { 
	private static final long serialVersionUID = 1L;
	
	protected DataType totalValueType;
	protected DataType keyValueType;
	
	protected byte[] keys;
	
	protected DbNumericType[] totalValues;
	protected DbKeyValueStore[] keyValues;
	
	protected AggregateFunctionType aggregateType;
	
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf() { super(); }
	
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf(int nodeSize, int bytesPerKey, DataType totalValueType, 
			DataType keyValueType, AggregateFunctionType aggregateType) {
		super(nodeSize, bytesPerKey);
				
		if (!(aggregateType == AggregateFunctionType.FSCNT) || (aggregateType == AggregateFunctionType.FSSUM))
			throw new DatabaseException("BPlusTreeLeafLongKeysForFSCount should only be used with fscnt or fssum.");
		
		this.totalValueType = totalValueType;
		this.keyValueType = keyValueType; 
		this.aggregateType = aggregateType;
		
		this.highWaterMark = 0;
		// same number of keys and values
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey];
		this.totalValues = new DbNumericType[this.numberOfKeys];
		this.keyValues = new DbKeyValueStore[this.numberOfKeys];
	}
	
	public DbNumericType[] getTotalValues() { return this.totalValues; }
	
	public DbKeyValueStore[] getKeyValues() { return this.keyValues; }
	
	public byte[] getKeys() { return this.keys; }
		
	@Override
	public int getNumberOfKeys() { 
		if (this.totalValueType == null || this.keyValueType == null)
			return super.getNumberOfKeys();
		return (this.nodeSize / (this.bytesPerKey + this.totalValueType.getNumberOfBytes() + this.keyValueType.getNumberOfBytes())); 
	}
	
	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}

	@Override
	public void insert(byte[] key, DbTypeBase subKey, DbNumericType newPartialValue, 
			AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesInsertResult result, TypeManager typeManager) {	
		int i;		
		int compare;	
		for (i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			// if we have an exact match, do aggregation and update the value for the key
			if (compare == 0) {
				DbKeyValueStore group = this.keyValues[i];
				DbNumericType oldPartialValue = (DbNumericType) group.get(subKey);
 	
				if (oldPartialValue == null) {
					group.put(subKey, newPartialValue);
				} else if (newPartialValue.greaterThan(oldPartialValue)) {
					//System.out.println("\\\\\\new value "+newPartialValue+" greater than " + oldPartialValue+"\\\\\\\"");
					group.put(subKey, newPartialValue);
				} else if (newPartialValue.equals(oldPartialValue)) {
					result.newPage = null;
					result.status = AggregatorInsertStatus.NO_CHANGE;
					result.newTotalResult = this.totalValues[i];
					return;
				} else {
					result.newPage = null;
					result.status = AggregatorInsertStatus.FAIL;
					result.newTotalResult = null;
					return;
				}				

				DbNumericType oldTotalValue = this.totalValues[i];
				DbNumericType newTotalValue;
				/*if (newPartialValue instanceof DbFloat) {
					if (oldPartialValue == null)
						newTotalValue = DbFloat.create(oldTotalValue.getFloatValue() + newPartialValue.getFloatValue());
					else
						newTotalValue = DbFloat.create(oldTotalValue.getFloatValue() + (newPartialValue.getFloatValue() - oldPartialValue.getFloatValue()));
				} else {*/
				if (oldPartialValue == null)
					newTotalValue = oldTotalValue.add(newPartialValue);
				else
					newTotalValue = oldTotalValue.add(newPartialValue.subtract(oldPartialValue));					
				//}

				this.totalValues[i] = newTotalValue;
				result.newPage = null;
				result.status = AggregatorInsertStatus.UPDATE;
				result.newTotalResult = newTotalValue;
				return;
			}

			if (compare < 0)
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i * this.bytesPerKey, this.keys, (i+1) * this.bytesPerKey, (this.highWaterMark - i) * this.bytesPerKey);
			System.arraycopy(this.totalValues, i, this.totalValues, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.keyValues, i, this.keyValues, (i+1), (this.highWaterMark - i));
		}
		
		System.arraycopy(key, 0, this.keys, i * this.bytesPerKey, this.bytesPerKey);			 
		this.totalValues[i] = (DbNumericType) newPartialValue.copy();

		//DbKeyValueStore newGroup = this.createNewGroup(subKey);
		DbKeyValueStore newGroup = typeManager.createKeyValueStore(subKey.getDataType(), this.totalValueType);
		
		newGroup.put(subKey, newPartialValue);
		this.keyValues[i] = newGroup;
		
		this.highWaterMark++;

		result.status = AggregatorInsertStatus.NEW;
		result.newTotalResult = newPartialValue;
		
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		
		result.newPage = null;
	}
		
	private AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesPage split() {
		AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf rightLeaf = 
				new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf(this.nodeSize, this.bytesPerKey, this.totalValueType, 
						this.keyValueType, this.aggregateType);
		// give the right 1/2 of the children to the new leaf

		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
		// move values to new leaf
		System.arraycopy(this.totalValues, splitPoint, rightLeaf.totalValues, 0, numberToMove);		
		// move keyvalues to new leaf
		System.arraycopy(this.keyValues, splitPoint, rightLeaf.keyValues, 0, numberToMove);
		
		this.highWaterMark -= numberToMove;
			
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	@Override
	public void get(byte[] key, AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResult result) {
		int compare;
		for (int i = 0; i < this.highWaterMark; i++) {
			compare = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compare == 0) {
				result.success = true;
				result.value = this.totalValues[i];
				return;
			}
			
			if (compare < 0)
				break;
		}
		result.success = false;
	}
	
	@Override
	public void get(byte[] startKey, byte[] endKey, AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResultRange result) {
		int compareStart;
		int compareEnd = 0;
		for (int i = 0; i < this.highWaterMark; i++) {
			compareStart = ByteArrayHelper.compare(startKey, this.keys, i * this.bytesPerKey, startKey.length);			
			//if (startKey <= this.keys[i] && this.keys[i] <= endKey) {
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
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - deleteAt) * this.bytesPerKey);
				System.arraycopy(this.totalValues, (deleteAt + 1), this.totalValues, deleteAt, (this.highWaterMark - deleteAt));
				System.arraycopy(this.keyValues, (deleteAt + 1), this.keyValues, deleteAt, (this.highWaterMark - deleteAt));
							
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
		this.totalValues = null;
		this.keyValues = null;
		this.highWaterMark = 0;
		this.next = null;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length + "\n");
		output.append(buffer + "# of total values | Max # of total values: " + this.highWaterMark + " | " + this.totalValues.length + "\n");
		output.append(buffer + "# of key values | Max # of key values: " + this.highWaterMark + " | " + this.keyValues.length + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
			output.append(this.totalValues[i]);
			output.append("|");
			output.append(this.keyValues[i]);
			output.append("]\n");
		}
		return output.toString();
	}

	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
			output.append(this.totalValues[i]);
			output.append("|");
			output.append(this.keyValues[i]);
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
		
		if (this.totalValues != null) {
			used += this.highWaterMark * this.totalValueType.getNumberOfBytes();
			allocated += this.totalValues.length * this.totalValueType.getNumberOfBytes();
		}
		
		if (this.keyValues != null) {
			MemoryMeasurement keyValueMM;
			for (int i = 0; i < this.highWaterMark; i++) {
				keyValueMM = this.keyValues[i].getSizeOf();
				used += keyValueMM.getUsed(); 
				allocated += keyValueMM.getAllocated();
			}
		}
		
		return new MemoryMeasurement(used, allocated);
	}
		
	/*private DbKeyValueStore createNewGroup(DbTypeBase subKey) {
		DbKeyValueStore newGroup = deALSContext.getDatabase().getTypeManager().createKeyValueStore(subKey.getDataType(), 
				this.totalValueType);
		
		if (newGroup == null)
			throw new DatabaseException("Uknown keyvaluetype selected.");
		return newGroup;
	}*/
}
