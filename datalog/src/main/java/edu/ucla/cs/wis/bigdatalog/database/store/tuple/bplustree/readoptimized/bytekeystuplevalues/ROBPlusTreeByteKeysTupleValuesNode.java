package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.bytekeystuplevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

public class ROBPlusTreeByteKeysTupleValuesNode
	extends BPlusTreeNode<ROBPlusTreeByteKeysTupleValuesPage, ROBPlusTreeByteKeysTupleValuesLeaf> 
	implements ROBPlusTreeByteKeysTupleValuesPage, Serializable {
	private static final long serialVersionUID = 1L;

	protected byte[] keys;
	
	public ROBPlusTreeByteKeysTupleValuesNode() { super(); }
	
	public ROBPlusTreeByteKeysTupleValuesNode(int nodeSize, int bytesPerKey) {
		super(nodeSize, bytesPerKey);
		
		this.initialize();
	}
	
	protected void initialize() {
		this.highWaterMark = 0;
		this.keys = new byte[this.numberOfKeys * this.bytesPerKey]; // M-1 keys
		this.children = new ROBPlusTreeByteKeysTupleValuesPage[this.getBranchingFactor()]; // M children
		this.numberOfEntries = 0;
	}
	
	//@Override
	//public int getNumberOfKeys() { return this.nodeSize / this.bytesPerKey; }

	@Override
	public byte[] getLeftMostLeafKey() {
		return this.children[0].getLeftMostLeafKey();
	}

	@Override
	public Pair<ROBPlusTreeByteKeysTupleValuesPage, Boolean> insert(byte[] key, Tuple tuple) {
		int insertAt = 0;
		for (insertAt = 0; insertAt < (this.highWaterMark - 1); insertAt++) {
			if (ByteArrayHelper.compare(key, this.keys, (insertAt * this.bytesPerKey), this.bytesPerKey) < 0)
				break;
		}

		Pair<ROBPlusTreeByteKeysTupleValuesPage, Boolean> retval = this.children[insertAt].insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
		
		if (retval.getFirst() == null)
			return retval;

		// shift right for insertion to the left
		if ((insertAt + 1) < this.highWaterMark)
			System.arraycopy(this.children, insertAt + 1, this.children, insertAt + 2, this.highWaterMark - (insertAt + 1));				
		// shift right for insertion to the left
		if (insertAt < (this.highWaterMark - 1)) 
			System.arraycopy(this.keys, insertAt * this.bytesPerKey, this.keys, (insertAt + 1) * this.bytesPerKey, (this.highWaterMark - 1 - insertAt) * this.bytesPerKey);
		
		// we are inserting the page with the right 1/2 of the values from the previous full page (that was split)
		// therefore, it goes to the right of where we inserted
		insertAt++;
		this.children[insertAt] = retval.getFirst(); 
		
		byte[] newLeftKey = this.children[insertAt].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, this.keys, (insertAt - 1) * this.bytesPerKey, this.bytesPerKey);
				
		this.highWaterMark++;

		if (this.hasOverflow())
			return new Pair<>(this.split(), retval.getSecond());
			
		return new Pair<>(null, retval.getSecond());
	}

	private ROBPlusTreeByteKeysTupleValuesPage split() {
		ROBPlusTreeByteKeysTupleValuesNode rightNode = new ROBPlusTreeByteKeysTupleValuesNode(this.nodeSize, this.bytesPerKey);
		// give the right 1/2 of the children to the new node
		int i;
		int splitPoint = (int) Math.ceil(((double)this.children.length) / 2);
		int numberToMove = this.children.length - splitPoint;
		
		System.arraycopy(this.children, splitPoint, rightNode.children, 0, numberToMove);
		
		for (i = 0; i < numberToMove; i++) {
			this.children[splitPoint + i] = null;
			for (int a = 0; a < this.bytesPerKey; a++)
				this.keys[((splitPoint + i - 1) * this.bytesPerKey) + a] = 0;
			
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
	public void get(byte[] key, ROBPlusTreeGetResult result) {
		for (int i = 0; i < (this.highWaterMark - 1); i++) {
			if (ByteArrayHelper.compare(key, this.keys, (i * this.bytesPerKey), this.bytesPerKey) < 0) {
				this.children[i].get(key, result);
				return;
			}
		}
		
		this.children[this.highWaterMark - 1].get(key, result);
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
			if (this.children[deleteAt].isEmpty()) {
				// remove child and key and shift remaining children 
				for (int j = deleteAt; j < this.highWaterMark; j++)
					this.children[j] = this.children[j + 1];
	
				if (deleteAt < (this.highWaterMark - 1))
					System.arraycopy(this.keys, ((deleteAt + 1) * this.bytesPerKey), this.keys, (deleteAt* this.bytesPerKey), ((this.highWaterMark - (deleteAt+1)) * this.bytesPerKey));
	
				this.highWaterMark--;
			}			
		}
		return status;
	}
	
	@Override
	public void deleteAll() {
		for (int i = 0; i < this.highWaterMark; i++)
			this.children[i].deleteAll();
		
		this.keys = null;
		this.children = null;
		this.numberOfEntries = 0;
		this.highWaterMark = 0;			
	}
		
	@Override
	public String toString(int indent) {
		StringBuilder retval = new StringBuilder();
		String buffer = "";
		for (int i = 0; i < indent; i++) 
			buffer += " ";
		
		//retval.append(buffer + this.pageType.name() + "(PageId: " + this.pageId + ")\n");
		retval.append(buffer + "# of entries | Max # of entries: " + this.highWaterMark + " | " + this.children.length + "\n");
		retval.append(buffer + "IsEmpty: " + this.isEmpty() + ", IsOverflow: " + this.hasOverflow() + "\n");
		retval.append(buffer + "Keys: [");
		for (int i = 0; i < this.keys.length; i++) {
			if (i > 0) retval.append(", ");
			retval.append(this.keys[i]);
		}
		retval.append("]\n");
		retval.append(buffer + "Entries:\n");
		for (int i = 0; i < this.highWaterMark; i++)
			retval.append(buffer + this.children[i].toString(indent + 2) + "\n");
				
		return retval.toString();
	}
	
	@Override
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