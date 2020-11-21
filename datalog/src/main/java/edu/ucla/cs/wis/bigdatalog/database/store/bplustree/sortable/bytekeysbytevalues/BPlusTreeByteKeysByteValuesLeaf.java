package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysbytevalues;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeByteKeysLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeGeneralLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeByteKeysByteValuesLeaf 
	extends BPlusTreeGeneralLeaf<BPlusTreeByteKeysByteValuesLeaf> 
	implements BPlusTreeByteKeysByteValuesPage, BPlusTreeByteKeysLeaf, Serializable {
	private static final long serialVersionUID = 1L;
	
	private byte[] keys;
	private BPlusTreeByteKeysByteValues tree;
	
	public BPlusTreeByteKeysByteValuesLeaf() { super(); }
	
	public BPlusTreeByteKeysByteValuesLeaf(BPlusTreeByteKeysByteValues tree) {
		super(tree.getNodeSize(), tree.getBytesPerKey(), tree.getBytesPerValue());
		
		this.tree = tree;
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey];
		this.values = new byte[this.numberOfKeys * this.bytesPerValue];
	}
	
	public byte[] getKeys() { return this.keys; }
	
	public int getNumberOfKeys() { return (this.nodeSize / (this.bytesPerKey + this.bytesPerValue)); }
	
	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}

	@Override
	public void insert(byte[] key, byte[] value, BPlusTreeByteKeysByteValuesInsertResult result, TypeManager typeManager) {	
		int compare = 0;
		int insertAt;		
		boolean stop = false;		
		int positionInKey = 0;
		DbTypeBase leafKeyColumn, keyColumn;
		
		for (insertAt = 0; insertAt < this.highWaterMark; insertAt++) {
			for (int i = 0; i < this.tree.keySortOrder.length && !stop; i++) {
				if (i == 0)
					positionInKey = 0;
				else
					positionInKey += this.tree.bytesPerKeyColumn[i-1];
			
				leafKeyColumn = DbTypeBase.loadFrom(this.tree.getKeyColumnTypes()[i], this.keys, (insertAt * this.bytesPerKey) + positionInKey, typeManager);
				keyColumn = DbTypeBase.loadFrom(this.tree.getKeyColumnTypes()[i], key, positionInKey, typeManager);

				compare = keyColumn.compare(leafKeyColumn);
				/*if (this.tree.getKeyColumnTypes()[i] == DataType.STRING) {	
					DbString item = (DbString) DbTypeBase.loadFrom(DataType.STRING, this.keys, (insertAt * this.bytesPerKey) + positionInKey);
					DbString keyColumn = (DbString) DbTypeBase.loadFrom(DataType.STRING, key, positionInKey);

					compare = keyColumn.compare(item);
				} else {				
					compare = ByteArrayHelper.compare(ByteArrayHelper.getBytesAsBytes(key, positionInKey), 
							this.keys, (insertAt * this.bytesPerKey) + positionInKey, this.tree.bytesPerKeyColumn[i]);
				}*/

				if (compare < 0) {
					if (this.tree.keySortOrder[i] == 0)					 
						stop = true;
					break;
				} else if (compare  > 0) {
					if (this.tree.keySortOrder[i] == 1)
						stop = true;
					break;
				}			
			}
			
			if (compare == 0) {
				System.arraycopy(this.values, insertAt * this.bytesPerValue, result.oldValue, 0, this.bytesPerValue);
				if (this.bytesPerValue > 0)
					System.arraycopy(value, 0, this.values, insertAt * this.bytesPerValue, this.bytesPerValue);
				result.newPage = null;
				result.status = BPlusTreeOperationStatus.UPDATE;
				return;
			}
			
			if (stop)
				break;
		}

		if (insertAt != this.highWaterMark) {
			System.arraycopy(this.keys, insertAt * this.bytesPerKey, this.keys, (insertAt+1) * this.bytesPerKey, (this.highWaterMark - insertAt) * this.bytesPerKey);
			if (this.bytesPerValue > 0)
				System.arraycopy(this.values, insertAt * this.bytesPerValue, this.values, (insertAt+1) * this.bytesPerValue, (this.highWaterMark - insertAt) * this.bytesPerValue);
		}

		System.arraycopy(key, 0, this.keys, insertAt * this.bytesPerKey, this.bytesPerKey);
		if (this.bytesPerValue > 0)
			System.arraycopy(value, 0, this.values, insertAt * this.bytesPerValue, this.bytesPerValue);

		this.highWaterMark++;
		
		result.status = BPlusTreeOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;		
	}
		
	private BPlusTreeByteKeysByteValuesPage split() {
		BPlusTreeByteKeysByteValuesLeaf rightLeaf = new BPlusTreeByteKeysByteValuesLeaf(this.tree);
		// give the right 1/2 of the children to the new leaf
		//int i;
		int splitPoint = (int)Math.ceil(((double)(this.getNumberOfKeys()) / 2));
		int numberToMove = this.getNumberOfKeys() - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
		// move values to new leaf
		if (this.bytesPerValue > 0)
			System.arraycopy(this.values, splitPoint * this.bytesPerValue, rightLeaf.values, 0, numberToMove * this.bytesPerValue);
		
		this.highWaterMark -= numberToMove;
			
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	@Override
	public byte[] get(byte[] key) {
		int compareResult;
		for (int i = 0; i < this.highWaterMark; i++) {			
			compareResult = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compareResult == 0) {
				if (this.bytesPerValue > 0)
					return Arrays.copyOfRange(this.values, (i * this.bytesPerValue), ((i + 1) * this.bytesPerValue));
				
				return key;
			}
						
			if (compareResult < 0)
				return null;
		}
		return null;
	}
	
	@Override
	public void get(byte[] startKey, byte[] endKey, BPlusTreeByteKeysByteValuesGetResultRange result) {
		int compareStart;
		int compareEnd = 0;
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			compareStart = ByteArrayHelper.compare(startKey, this.keys, i * this.bytesPerKey, this.bytesPerKey);
			//startKey <= this.keys[i] && this.keys[i] <= endKey
			if (compareStart <= 0) {
				compareEnd = ByteArrayHelper.compare(endKey, this.keys, i * this.bytesPerKey, this.bytesPerKey);
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
		
		if (i < this.highWaterMark) {
			result.success = false;		
		} else {
			if (this.next == null) {
				result.success = false;
			} else {
				this.next.get(startKey, endKey, result);
			}
		}
	}
	
	@Override
	public boolean delete(byte[] key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth				
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - deleteAt) * this.bytesPerKey);
				if (this.bytesPerValue > 0)
					System.arraycopy(this.values, (deleteAt + 1) * this.bytesPerValue, this.values, deleteAt * this.bytesPerValue, (this.highWaterMark - deleteAt) * this.bytesPerValue);
				
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
		this.next = null;
		this.highWaterMark = 0;
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + this.keys.length / this.bytesPerKey+ "\n");
		output.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.values.length / this.bytesPerValue + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "[");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
			for (int j = 0; j < this.bytesPerValue; j++)
				output.append(this.values[(i * this.bytesPerValue) + j]);
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
			for (int j = 0; j < this.bytesPerValue; j++)
				output.append(this.values[(i * this.bytesPerValue) + j]);
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
		
		if ((this.bytesPerValue > 0) && (this.values != null)) {
			used += this.highWaterMark * this.bytesPerValue;
			allocated += this.values.length;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
