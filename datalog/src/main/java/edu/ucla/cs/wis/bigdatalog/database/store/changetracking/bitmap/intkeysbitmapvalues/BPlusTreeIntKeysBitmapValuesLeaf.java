package edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues;

import java.io.Serializable;
import java.util.BitSet;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeIntKeysBitmapValuesLeaf 
	extends BPlusTreeLeaf<BPlusTreeIntKeysBitmapValuesLeaf> 
	implements BPlusTreeIntKeysBitmapValuesPage, Serializable {
	private static final long serialVersionUID = 1L;
	
	private int[] upperKeys;
	private BitSet[] lowerKeys;
	
	public BPlusTreeIntKeysBitmapValuesLeaf() { super(); }
	
	public BPlusTreeIntKeysBitmapValuesLeaf(int nodeSize) {
		super(nodeSize, 4);

		this.highWaterMark = 0;
		this.upperKeys = new int[this.numberOfKeys];
		this.lowerKeys = new BitSet[this.numberOfKeys];
	}
	
	public int[] getKeys() { return this.upperKeys; }
	
	public BitSet[] getValues() { return this.lowerKeys; }

	@Override
	public int getLeftMostLeafKey() {
		if (this.highWaterMark == 0)
			return Integer.MIN_VALUE;
		
		return this.upperKeys[0];
	}
	
	public void insert(long key, BPlusTreeIntKeysBitmapValuesInsertResult result) {
		int upperKey = (int)(key >> 32);
		int i;
		for (i = 0; i < this.highWaterMark; i++) {
			if (upperKey == this.upperKeys[i]) {
				if (this.lowerKeys[i].get((int)key)) {
					result.status = BPlusTreeOperationStatus.NO_CHANGE;
				} else {
					this.lowerKeys[i].set((int)key);
					result.status = BPlusTreeOperationStatus.NEW;
				}
				result.newPage = null;
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
		BitSet newLowerKeys = new BitSet();
		newLowerKeys.set((int)key);
		this.lowerKeys[i] = newLowerKeys;		
		
		this.highWaterMark++;
		
		result.status = BPlusTreeOperationStatus.NEW;
		if (this.hasOverflow()) {
			result.newPage = this.split();			
			return;
		}
		result.newPage = null;
	}
		
	private BPlusTreeIntKeysBitmapValuesPage split() {
		BPlusTreeIntKeysBitmapValuesLeaf rightLeaf = new BPlusTreeIntKeysBitmapValuesLeaf(this.nodeSize);
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
	public void get(long key, BPlusTreeIntKeysBitmapValuesGetResult result) {
		int upperKey = (int)(key >> 32);
		for (int i = 0; i < this.highWaterMark; i++) {
			if (this.upperKeys[i] == upperKey) {			
				result.success = this.lowerKeys[i].get((int)key);
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
				
				this.lowerKeys[deleteAt].clear((int)key);
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
			this.lowerKeys[i].clear();
		this.lowerKeys = null;
		this.next = null;
		this.highWaterMark = 0;
	}
			
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer+= " ";
		
		retval.append(buffer + "First Level: # of keys | Max # of keys: [" + this.highWaterMark + " | " + this.upperKeys.length + "] ");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "keys: ");
		for (int i = 0; i < this.highWaterMark; i++) {
			if (i > 0)
				retval.append(", ");
			retval.append(this.upperKeys[i]);
			retval.append(this.lowerKeys[i]);
		}
		return retval.toString();
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
			for (int i = 0; i < this.highWaterMark; i++) {
				used += 4 + this.lowerKeys[i].length();
				allocated += 4 + this.lowerKeys[i].size();
			}
		}
				
		return new MemoryMeasurement(used, allocated);
	}
}
