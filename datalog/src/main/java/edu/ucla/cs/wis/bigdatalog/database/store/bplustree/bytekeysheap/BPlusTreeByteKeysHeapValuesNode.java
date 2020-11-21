package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysheap;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class BPlusTreeByteKeysHeapValuesNode
	extends BPlusTreeNode<BPlusTreeByteKeysHeapValuesPage, BPlusTreeByteKeysHeapValuesLeaf> 
	implements BPlusTreeByteKeysHeapValuesPage, Serializable {
	private static final long serialVersionUID = 1L;

	protected byte[] keys;
	protected int bytesPerValue;
	protected boolean useOrderedHeap;
	
	public BPlusTreeByteKeysHeapValuesNode() { super(); }
	
	public BPlusTreeByteKeysHeapValuesNode(int nodeSize, int bytesPerKey, int bytesPerValue, boolean useOrderedHeap) {
		super(nodeSize, bytesPerKey);

		this.bytesPerValue = bytesPerValue;
		this.useOrderedHeap = useOrderedHeap;
	
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey]; // M-1 keys
		this.children = new BPlusTreeByteKeysHeapValuesPage[this.getBranchingFactor()]; // M children
		this.numberOfEntries = 0;
	}
	
	@Override
	public byte[] getLeftMostLeafKey() {
		return this.children[0].getLeftMostLeafKey();
	}

	@Override
	public void insert(byte[] key, byte[] data, BPlusTreeByteKeysHeapValuesInsertResult result) {
		int insertAt = 0;
		for (insertAt = 0; insertAt < (this.highWaterMark - 1); insertAt++) {
			if (ByteArrayHelper.compare(key, this.keys, (insertAt * this.bytesPerKey), this.bytesPerKey) < 0) 
				break;
		}
		
		this.children[insertAt].insert(key, data, result);
		if (result.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;
		
		if (result.newPage == null)
			return;

		// shift right for insertion to the left
		if ((insertAt + 1) < this.highWaterMark)
			System.arraycopy(this.children, insertAt + 1, this.children, insertAt + 2, this.highWaterMark - (insertAt + 1));				
		// shift right for insertion to the left
		if (insertAt < (this.highWaterMark - 1)) 
			System.arraycopy(this.keys, insertAt * this.bytesPerKey, this.keys, (insertAt + 1) * this.bytesPerKey, (this.highWaterMark - 1 - insertAt) * this.bytesPerKey);
		
		// we are inserting the page with the right 1/2 of the values from the previous full page (that was split)
		// therefore, it goes to the right of where we inserted
		insertAt++;
		this.children[insertAt] = result.newPage; 
			
		byte[] newLeftKey = this.children[insertAt].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, this.keys, (insertAt - 1) * this.bytesPerKey, this.bytesPerKey);

		this.highWaterMark++;

		if (this.hasOverflow()) {
			result.newPage = this.split();
			return;
		}
		result.newPage = null;		
	}

	private BPlusTreeByteKeysHeapValuesPage split() {
		BPlusTreeByteKeysHeapValuesNode rightNode = new BPlusTreeByteKeysHeapValuesNode(this.nodeSize, this.bytesPerKey, this.bytesPerValue, this.useOrderedHeap);
		// give the right 1/2 of the children to the new node
		int i;
		int splitPoint = (int) Math.ceil(((double)this.children.length) / 2);
		int numberToMove = this.children.length - splitPoint;
		
		System.arraycopy(this.children, splitPoint, rightNode.children, 0, numberToMove);
		
		for (i = 0; i < numberToMove; i++) {
			this.children[splitPoint + i] = null;
			for (int a = 0; a < this.bytesPerKey; a++)
				this.keys[((splitPoint + i - 1) * this.bytesPerKey) + a ] = 0;
			
			if (i > 0) {
				byte[] newLeftKey = rightNode.children[i].getLeftMostLeafKey();
				System.arraycopy(newLeftKey, 0, rightNode.keys, ((i - 1) * this.bytesPerKey), this.bytesPerKey);
			}

			this.highWaterMark--;
		}
		
		rightNode.highWaterMark = i;
		return rightNode;
	}
	
	@Override
	public Heap get(byte[] key) {
		for (int i = 0; i < (this.highWaterMark - 1); i++) {
			if (ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey) < 0)
				return this.children[i].get(key);
		}
		
		if (this.children[0] == null)
			return null;

		return this.children[this.highWaterMark - 1].get(key);
	}

	@Override
	public boolean delete(byte[] key) {
		int deleteAt = 0;
		for (deleteAt = 0; deleteAt < (this.highWaterMark - 1); deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, (deleteAt * this.bytesPerKey), this.bytesPerKey) < 0)
				break;
		}
		
		boolean status = this.children[deleteAt].delete(key);
		if (status) {
			this.numberOfEntries--;
		
			if (this.children[deleteAt].isEmpty())
				this.removeEmptyKey(deleteAt);
		}
		return status;
	}
	
	@Override
	public boolean delete(byte[] key, byte[] value) {
		int deleteAt = 0;
		for (deleteAt = 0; deleteAt < (this.highWaterMark - 1); deleteAt++) {
			if (ByteArrayHelper.compare(key, this.keys, (deleteAt * this.bytesPerKey), this.bytesPerKey) < 0)
				break;
		}
		
		boolean status = this.children[deleteAt].delete(key, value);
		if (status) {
			this.numberOfEntries--;
		
			if (this.children[deleteAt].isEmpty())
				this.removeEmptyKey(deleteAt);
		}
		return status;		
	}
	
	private void removeEmptyKey(int deleteAt) {
		// remove child and key and shift remaining children 
		for (int j = deleteAt; j < this.highWaterMark; j++)
			this.children[j] = this.children[j + 1];

		if (deleteAt < (this.highWaterMark - 1))
			System.arraycopy(this.keys, ((deleteAt + 1)* this.bytesPerKey), this.keys, (deleteAt* this.bytesPerKey), ((this.highWaterMark - deleteAt) * this.bytesPerKey));

		this.highWaterMark--;		
	}
	
	@Override
	public void deleteAll() {
		for (int i = 0; i < this.highWaterMark; i++)
			this.children[i].deleteAll();
		
		this.keys = null;
		this.children = null;
		this.highWaterMark = 0;
		this.numberOfEntries = 0;
	}
	
	@Override
	public int commit() {
		int numberCommitted = 0;
		for (int i = 0; i < this.highWaterMark; i++)
			numberCommitted += this.children[i].commit();
		
		return numberCommitted;
	}
	
	@Override
	public String toString(int indent) {
		StringBuilder output = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer += " ";

		output.append(buffer + "# of entries | Max # of entries: " + this.highWaterMark + " | " + this.children.length + "\n");
		output.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		output.append(buffer + "Keys: [");
		for (int i = 0; i < this.keys.length; i++) {
			if (i > 0) output.append(", ");
			output.append(this.keys[i]);
		}
		output.append("]\n");
		output.append(buffer + "Entries:\n");
		for (int i = 0; i < this.highWaterMark; i++)
			output.append(this.children[i].toString(indent + 2));
				
		return output.toString();
	}
	
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.keys != null) {
			used += this.highWaterMark * this.bytesPerKey;
			allocated += this.keys.length;
		}
		
		if (this.children != null) {
			MemoryMeasurement sizes;
			for (int i = 0; i < this.highWaterMark; i++) {
				sizes = this.children[i].getSizeOf();
				used += sizes.getUsed();
				allocated += sizes.getAllocated();
			}
		}
		return new MemoryMeasurement(used, allocated);
	}
}