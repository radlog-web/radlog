package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonlytwolevel;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnlyInsertResult;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeLongKeysOnlyTwoLevelLeaf 
	extends BPlusTreeLeaf<BPlusTreeLongKeysOnlyTwoLevelLeaf> 
	implements BPlusTreeLongKeysOnlyTwoLevelPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	private int[] upperKeys;
	private BPlusTreeIntKeysOnly[] lowerKeys;
	private BPlusTreeIntKeysOnlyInsertResult subKeyTreeInsertResult;
	
	public BPlusTreeLongKeysOnlyTwoLevelLeaf() { super(); }
	
	public BPlusTreeLongKeysOnlyTwoLevelLeaf(int nodeSize) {
		super(nodeSize, 4);

		this.highWaterMark = 0;
		this.upperKeys = new int[this.numberOfKeys];
		this.lowerKeys = new BPlusTreeIntKeysOnly[this.numberOfKeys];
		this.subKeyTreeInsertResult = new BPlusTreeIntKeysOnlyInsertResult();
	}
	
	public int[] getKeys() { return this.upperKeys; }

	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.upperKeys[0];
	}
	
	@Override
	public void insert(long key, BPlusTreeLongKeysOnlyTwoLevelInsertResult result, /*DeALSConfiguration deALSConfiguration, */TypeManager typeManager) {
		int upperKey = (int)(key >> 32);
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if (upperKey == this.upperKeys[i]) {
				this.lowerKeys[i].insert((int)key, this.subKeyTreeInsertResult);
				result.status = this.subKeyTreeInsertResult.status;
				return;
			}
			
			if (upperKey < this.upperKeys[i])
				break;
		}

		if (i != this.highWaterMark) {
			System.arraycopy(this.upperKeys, i, this.upperKeys, (i+1), (this.highWaterMark - i));
			System.arraycopy(this.lowerKeys, i, this.lowerKeys, (i+1), (this.highWaterMark - i));
		}

		this.upperKeys[i] = upperKey;
		
		BPlusTreeIntKeysOnly newLowerKey = new BPlusTreeIntKeysOnly(this.nodeSize, new int[]{0}, new DataType[]{DataType.INT}/*, 
				deALSConfiguration, typeManager*/);
		newLowerKey.insert((int)key);
		this.lowerKeys[i] = newLowerKey;		
		
		this.highWaterMark++;
		
		result.status = BPlusTreeOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();			
			return;
		}
		result.newPage = null;
	}
		
	private BPlusTreeLongKeysOnlyTwoLevelPage split() {
		BPlusTreeLongKeysOnlyTwoLevelLeaf rightLeaf = new BPlusTreeLongKeysOnlyTwoLevelLeaf(this.nodeSize);
		// give the right 1/2 of the children to the new leaf
		int splitPoint = (int)Math.ceil(((double)this.numberOfKeys / 2));
		int numberToMove = this.numberOfKeys - splitPoint;
		
		// move keys to new leaf
		System.arraycopy(this.upperKeys, splitPoint, rightLeaf.upperKeys, 0, numberToMove);
		// move values to new leaf
		System.arraycopy(this.lowerKeys, splitPoint, rightLeaf.lowerKeys, 0, numberToMove);
		
		// break references to the moved lowerkeys 
		for (int i = 0; i < numberToMove; i++) 
			this.lowerKeys[splitPoint + i] = null;
		
		this.highWaterMark -= numberToMove;
			
		if (this.next != null)
			rightLeaf.next = this.next;
		
		this.next = rightLeaf;
		
		rightLeaf.highWaterMark = numberToMove;
		return rightLeaf;
	}
	
	@Override
	public void get(long key, BPlusTreeLongKeysOnlyTwoLevelGetResult result) {
		int upperKey = (int)(key >> 32);
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.upperKeys[i] == upperKey) {			
				result.success = (this.lowerKeys[i].get((int)key) != null);
				return;
			}
			
			if (this.upperKeys[i] > upperKey)
				break;			
		}
		result.success = false;
	}
		
	@Override
	public boolean delete(long key) {
		boolean status = false;
		int upperKey = (int)(key >> 32);
		for (int deleteAt = 0; deleteAt < this.highWaterMark; deleteAt++) {
			if (this.upperKeys[deleteAt] == upperKey) {
				
				this.lowerKeys[deleteAt].delete((int)key);
				// if lower key tree is now empty remove it
				if (this.lowerKeys[deleteAt].isEmpty()) {
					// move all after this position, down 1 position
					System.arraycopy(this.upperKeys, (deleteAt + 1), this.upperKeys, deleteAt, (this.highWaterMark - deleteAt));				
					System.arraycopy(this.lowerKeys, (deleteAt + 1), this.lowerKeys, deleteAt, (this.highWaterMark - deleteAt));								
					this.highWaterMark--;
				}
				status = true;
				break;
			}
		}
		return status;
	}

	@Override
	public void deleteAll() {
		this.upperKeys = null;
		for (int i = 0; i < this.highWaterMark; i++)
			this.lowerKeys[i].deleteAll();
		this.lowerKeys = null;
		this.next = null;
		this.highWaterMark = 0;
	}
			
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		output.append(buffer + "First Level: # of keys | Max # of keys: [" + this.highWaterMark + " | " + this.upperKeys.length + "] ");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "keys: ");
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				output.append(", ");
			output.append(this.upperKeys[i]);
			output.append(this.lowerKeys[i]);
		}
		return output.toString();
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.highWaterMark; i++) {
			output.append("[");
			output.append(this.upperKeys[i]);
			output.append("|");
			output.append(this.lowerKeys[i]);
			output.append("]");
		}
		return output.toString();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.upperKeys != null) {
			used += this.highWaterMark * 4;
			allocated += this.upperKeys.length * 4;
		}
		
		if (this.lowerKeys != null) {
			MemoryMeasurement subKeyMM;
			for (int i = 0; i < this.highWaterMark; i++) {
				subKeyMM = this.lowerKeys[i].getSizeOf();
				used += subKeyMM.getUsed();
				allocated += subKeyMM.getAllocated();
			}
		}
				
		return new MemoryMeasurement(used, allocated);
	}
}
