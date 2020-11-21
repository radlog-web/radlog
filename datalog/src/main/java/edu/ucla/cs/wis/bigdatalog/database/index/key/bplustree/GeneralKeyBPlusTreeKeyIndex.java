package edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.index.key.GeneralKeyKeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndexResult;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public class GeneralKeyBPlusTreeKeyIndex 
	extends GeneralKeyKeyIndex 
	implements BPlusTreeKeyIndex, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;

	protected int nodeSize;	
	protected GeneralKeyBPlusTreeKeyIndexPage rootNode;
	protected GeneralKeyBPlusTreeKeyIndexResult insertResult;
	
	public GeneralKeyBPlusTreeKeyIndex(Relation<?> relation, int[] keyColumns, int nodeSize) {
		super(relation, keyColumns);
	
		this.bytesPerKey = this.getKeySize();
		if (this.bytesPerKey < 1)
			throw new DatabaseException("The schema for this index is unvalid.");
		
		this.nodeSize = nodeSize;
		//this.nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.indexes.bplustree.nodesize"));
		this.insertResult = new GeneralKeyBPlusTreeKeyIndexResult();		
		this.initialize();
	}
	
	protected void initialize() {
		this.numberOfEntries = 0;	
		this.rootNode = null;
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
			
	private GeneralKeyBPlusTreeKeyIndexPage allocatePage() {
		if (this.rootNode == null)
			return new GeneralKeyBPlusTreeKeyIndexLeaf(this.nodeSize, this.bytesPerKey);
		
		return new GeneralKeyBPlusTreeKeyIndexNode(this.nodeSize, this.bytesPerKey);
	}
		
	public void doPut(byte[] key, KeyIndexResult result) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, this.insertResult);
		
		result.success = this.insertResult.success;
		if (this.insertResult.success)
			this.numberOfEntries++;
		
		if (this.insertResult.newPage == null)
			return;

		// case 2 we have grow the tree as the root node was split 
		GeneralKeyBPlusTreeKeyIndexNode newRoot = (GeneralKeyBPlusTreeKeyIndexNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
		
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public boolean doGet(byte[] key) {
		if (this.rootNode.isEmpty())
			return false;
		
		return this.rootNode.get(key);
	}
	
	public boolean doRemove(byte[] key) {
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
	
	public GeneralKeyBPlusTreeKeyIndexLeaf getFirstChild() {
		if (this.getHeight() == -1)
			return null;
		
		if (this.getHeight() == 0)
			return (GeneralKeyBPlusTreeKeyIndexLeaf)this.rootNode; 
		
		return ((GeneralKeyBPlusTreeKeyIndexNode)this.rootNode).getFirstChild();
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		return this.rootNode.getSizeOf();
	}
}
