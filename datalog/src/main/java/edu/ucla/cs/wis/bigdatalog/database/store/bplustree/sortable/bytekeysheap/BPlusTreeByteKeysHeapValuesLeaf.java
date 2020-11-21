package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysheap;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeByteKeysLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeMultiValueLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.UnorderedHeap;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeByteKeysHeapValuesLeaf 
	extends BPlusTreeMultiValueLeaf<BPlusTreeByteKeysHeapValuesLeaf> 
	implements BPlusTreeByteKeysHeapValuesPage, BPlusTreeByteKeysLeaf, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeByteKeysHeapValues tree;
	protected byte[] keys;
	
	public BPlusTreeByteKeysHeapValuesLeaf() { super(); }
	
	public BPlusTreeByteKeysHeapValuesLeaf(BPlusTreeByteKeysHeapValues tree) {
		super(tree.getNodeSize(), tree.getBytesPerKey(), tree.getBytesPerValue(), false);
		this.tree = tree;
		// same number of keys and values
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey];
	}
	
	public byte[] getKeys() { return this.keys; }

	@Override
	public byte[] getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return null;
		
		byte[] key = new byte[this.bytesPerKey];
		System.arraycopy(this.keys, 0, key, 0, this.bytesPerKey);
				
		return key;
	}
	
	@Override
	public void insert(byte[] key, byte[] data, BPlusTreeByteKeysHeapValuesInsertResult result, TypeManager typeManager) {				
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
				
				if (compare < 0) {
					if (this.tree.keySortOrder[i] == 0)					 
						stop = true;
					break;
				} else if (compare > 0) {
					if (this.tree.keySortOrder[i] == 1)
						stop = true;
					break;
				}			
			}
			
			//compare = ByteArrayHelper.compare(key, this.keys, insertAt * this.bytesPerKey, this.bytesPerKey);
			// if we have an exact match put in heap
			if (compare == 0) {
				// only insert if unique
				if (this.values[insertAt].exists(data) < 0) {
					this.values[insertAt].add(data);
					result.newPage = null;
					result.status = BPlusTreeOperationStatus.NEW;
					return;
				}
				result.newPage = null;
				result.status = BPlusTreeOperationStatus.FAIL;
				return;
			}
				
			if (stop)
				break;
		}

		if (insertAt != this.highWaterMark) {
			System.arraycopy(this.keys, insertAt * this.bytesPerKey, this.keys, (insertAt+1) * this.bytesPerKey, (this.highWaterMark - insertAt) * this.bytesPerKey);
			// shift values right to add new one
			System.arraycopy(this.values, insertAt, this.values, (insertAt+1), (this.highWaterMark - insertAt));			
			this.values[insertAt] = null;
		}

		// add key to key storage
		System.arraycopy(key, 0, this.keys, insertAt * this.bytesPerKey, this.bytesPerKey);

		// add new heap if first entry for key
		if (this.values[insertAt] == null)
			this.values[insertAt] = new UnorderedHeap(data.length, this.bytesPerValue);
		
		this.values[insertAt].add(data);
		this.highWaterMark++;
		
		result.status = BPlusTreeOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;	
	}

	private BPlusTreeByteKeysHeapValuesPage split() {
		BPlusTreeByteKeysHeapValuesLeaf rightLeaf = new BPlusTreeByteKeysHeapValuesLeaf(this.tree);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.keys, splitPoint * this.bytesPerKey, rightLeaf.keys, 0, numberToMove * this.bytesPerKey);
		// move values to new leaf		
		for (int i = 0; i < numberToMove; i++) {
			rightLeaf.values[i] = this.values[splitPoint + i];
			this.values[splitPoint + i] = null;
			this.highWaterMark--;
		}
		
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	public Heap get(byte[] key) {
		int compareResult;
		for (int i = 0; i < this.highWaterMark; i++) {			
			compareResult = ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey);
			if (compareResult == 0)
				return this.values[i];
			
			if (compareResult < 0)
				return null;
		}
		return null;
	}
	
	@Override
	public boolean delete(byte[] key) {
		boolean status = false;
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, deleteAt * this.bytesPerKey, this.bytesPerKey) == 0) {
				// move all keys and values after this position, left one key or value worth				
				System.arraycopy(this.keys, (deleteAt + 1) * this.bytesPerKey, this.keys, deleteAt * this.bytesPerKey, (this.highWaterMark - deleteAt) * this.bytesPerKey);
				// move all after this position, down 1 position
				System.arraycopy(this.values, (deleteAt + 1), this.values, deleteAt, (this.highWaterMark - deleteAt));
				
				this.highWaterMark--;
				status = true;
				break;
			}
		}
		return status;
	}
	
	@Override
	public boolean delete(byte[] key, byte[] value) {
		boolean status = false;
		boolean removeValue = false;
		for (int i = 0; i < this.highWaterMark; i++) {
			if (ByteArrayHelper.compare(key, this.keys, i * this.bytesPerKey, this.bytesPerKey) == 0) {
				// remove the value from the key's heap
				int address = this.values[i].exists(value);
				this.values[i].delete(address);
				if (this.values[i].getNumberOfEntries() == 0)
					removeValue = true;
				status = true;	
				break;
			}
		}
		
		// if last value removed, remove key
		if (removeValue)
			this.delete(key);
		
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
	public int commit() {
		if (this.bytesPerValue == 0)
			return 0;
		
		int numberCommitted = 0;
		for (int i = 0; i < this.highWaterMark; i++) 
			numberCommitted += this.values[i].commit();
		
		return numberCommitted;
	}
	
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "# of keys | Max # of keys: " + this.highWaterMark + " | " + (this.keys.length / this.bytesPerKey) + "\n");
		output.append(buffer + "# of values | Max # of values: " + this.highWaterMark + " | " + this.values.length / this.bytesPerValue + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Key/Values:\n");
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append(buffer + "  [");
			for (int j = 0; j < this.bytesPerKey; j++) 
				output.append(this.keys[(i * this.bytesPerKey) + j]);
			output.append("|");
			output.append(this.values[i].toString());
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
			output.append(this.values[i].toString());
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
		
		if (this.values != null) {
			MemoryMeasurement sizes;
			for (int i = 0; i < this.highWaterMark; i++) {
				sizes = this.values[i].getSizeOf();
				used += sizes.getUsed();
				allocated += sizes.getAllocated();
			}
		}
		
		return new MemoryMeasurement(used, allocated);
	}
}
