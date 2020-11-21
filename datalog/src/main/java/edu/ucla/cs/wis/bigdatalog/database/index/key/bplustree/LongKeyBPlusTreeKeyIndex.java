package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndexResult;
import edu.ucla.cs.wis.bigdatalog.database.index.key.LongKeyKeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class LongKeyBPlusTreeKeyIndex 
	extends LongKeyKeyIndex 
	implements BPlusTreeKeyIndex, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
		
	protected int nodeSize;	
	protected LongKeyBPlusTreeKeyIndexPage rootNode;
	protected LongKeyBPlusTreeKeyIndexResult insertResult;
	
	public LongKeyBPlusTreeKeyIndex(Relation<?> relation, int[] keyColumns, int nodeSize) {
		super(relation, keyColumns);
	
		//this.nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.indexes.bplustree.nodesize"));
		this.nodeSize = nodeSize;
		this.insertResult = new LongKeyBPlusTreeKeyIndexResult();
		this.initialize();
	}
	
	protected void initialize() {
		this.numberOfEntries = 0;
		this.bytesPerKey = this.getKeySize();
		this.rootNode = this.allocatePage();
	}
	
	@Override
	public int getNodeSize() { return this.nodeSize; }
	
	@Override
	public int getHeight() {
		if (this.rootNode.isEmpty())
			return -1;
		
		return this.rootNode.getHeight();
	}
	
	public boolean isEmpty() {
		return this.rootNode.isEmpty();
	}
		
	private LongKeyBPlusTreeKeyIndexPage allocatePage() {
		if (this.rootNode == null)
			return new LongKeyBPlusTreeKeyIndexLeaf(this.nodeSize);
		
		return new LongKeyBPlusTreeKeyIndexNode(this.nodeSize);
	}
		
	public void doPut(long key, KeyIndexResult result) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult);
		result.success = this.insertResult.success;

		if (this.insertResult.success)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage == null) 
			return;

		// case 2 we have grow the tree as the root node was split 
		LongKeyBPlusTreeKeyIndexNode newRoot = (LongKeyBPlusTreeKeyIndexNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
		
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public boolean doGet(long key) {
		if (this.rootNode.isEmpty())
			return false;
		
		return this.rootNode.get(key);
	}
	
	public boolean doRemove(long key) {
		if (this.rootNode.isEmpty())
			return true;
		
		if (this.rootNode.delete(key)) {
			this.numberOfEntries--;
			if (this.numberOfEntries == 0)
				this.initialize();
			return true;
		}
		
		return false;
	}	
	
	public void doClear() {
		if (this.rootNode.isEmpty())
			return;
			
		this.rootNode.deleteAll();
		this.rootNode = null;
		this.initialize();
	}
		
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("\n////START - B+ Tree Key Index - START /////\n");
		if (this.rootNode.isEmpty())
			output.append("empty");			
		else
			output.append(this.rootNode.toString(0));
			
		output.append("\n////END - B+ Tree Key Index - END /////\n");
		return output.toString();
	}
	
	public String toStringStatistics() {
		StringBuilder output = new StringBuilder();
		output.append("\n////START - B+ Tree Key Index Statistics - START /////\n");		

		if (this.rootNode.isEmpty()) {
			output.append("empty");
		} else {
			output.append("height: " + this.getHeight() + "\n");
			output.append("# of entries: " + this.getNumberOfEntries() + "\n");
		}
		output.append("////END - B+ Tree Key Index Statistics - END /////\n");
		return output.toString();
	}
	
	public LongKeyBPlusTreeKeyIndexLeaf getFirstChild() {
		if (this.getHeight() == -1)
			return null;
		
		if (this.getHeight() == 0)
			return (LongKeyBPlusTreeKeyIndexLeaf)this.rootNode; 
		
		return ((LongKeyBPlusTreeKeyIndexNode)this.rootNode).getFirstChild();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		return this.rootNode.getSizeOf();
	}
}
