package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues;

import java.io.Serializable;

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

public class AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf 
	extends BPlusTreeLeaf<AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf> 
	implements AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesPage, Serializable { 
	private static final long serialVersionUID = 1L;
	
	protected DataType totalValueType;
	protected DataType keyValueType;
	
	protected int[] keys;
	
	protected DbNumericType[] totalValues;
	protected DbKeyValueStore[] keyValues;
	
	protected AggregateFunctionType aggregateType;
	
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf() { super(); }
	
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf(int nodeSize, DataType totalValueType, 
			DataType keyValueType, AggregateFunctionType aggregateType) {
		super(nodeSize, 4);
				
		if (!((aggregateType == AggregateFunctionType.FSCNT) || (aggregateType == AggregateFunctionType.FSSUM)))
			throw new DatabaseException("AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf should only be used with fscnt or fssum.");
		
		this.totalValueType = totalValueType;
		this.keyValueType = keyValueType; 
		this.aggregateType = aggregateType;
		
		this.highWaterMark = 0;
		// same number of keys and values
		this.keys = new int[this.numberOfKeys];
		this.totalValues = new DbNumericType[this.numberOfKeys];
		this.keyValues = new DbKeyValueStore[this.numberOfKeys];
	}
	
	public DbTypeBase[] getTotalValues() { return this.totalValues; }
	
	public DbKeyValueStore[] getKeyValues() { return this.keyValues; }
	
	public int[] getKeys() { return this.keys; }
		
	@Override
	public int getNumberOfKeys() { 
		if (this.totalValueType == null || this.keyValueType == null)
			return super.getNumberOfKeys();
		return (this.nodeSize / (this.bytesPerKey + this.totalValueType.getNumberOfBytes() + this.keyValueType.getNumberOfBytes())); 
	}
	
	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.keys[0];
	}

	@Override
	public void insert(int key, DbTypeBase subKey, DbNumericType newPartialValue, 
			AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesInsertResult result, TypeManager typeManager) {	
		int i;		
			
		for (i = 0; i < this.highWaterMark; i++) {
			// if we have an exact match, do aggregation and update the value for the key
			if (this.keys[i] == key) {
				DbKeyValueStore group = this.keyValues[i];
				DbNumericType oldPartialValue = (DbNumericType) group.get(subKey);
 	
				if (oldPartialValue == null) {
					//System.out.println("no old value");
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
				/*if (newPartialValue.getDataType() == DataType.FLOAT) {
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

			if (key < this.keys[i])
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.keys, i, this.keys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.totalValues, i, this.totalValues, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.keyValues, i, this.keyValues, (i+1), (this.highWaterMark - i));
		}
		
		this.keys[i] = key;			 
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
		
	private AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesPage split() {
		AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf rightLeaf = 
				new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf(this.nodeSize, this.totalValueType, 
						this.keyValueType, this.aggregateType);
		// give the right 1/2 of the children to the new leaf

		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint, rightLeaf.keys, 0, numberToMove);
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
	public void get(int key, AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResult result) {
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.keys[i] == key) {
				result.success = true;
				result.value = this.totalValues[i];
				return;
			}
			
			if (this.keys[i] > key)
				break;
		}
		result.success = false;
	}
	
	@Override
	public void get(int startKey, int endKey, AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResultRange result) {
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
			output.append(this.keys[i]);
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
			output.append(this.keys[i]);
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
			used += this.highWaterMark * 4;
			allocated += this.keys.length * 4;
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
