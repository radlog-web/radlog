package edu.ucla.cs.wis.bigdatalog.database.store.changetracking;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class GeneralChangeTracker 
	extends ChangeTracker implements Serializable {
	private static final long serialVersionUID = 1L;
	protected BPlusTreeByteKeysOnly tree;
	
	public GeneralChangeTracker() { super(); }
	
	public GeneralChangeTracker(int[] keyColumns,  DataType[] keyColumnTypes, int nodeSize) {
		int bytesPerKey = 0;		
		for (int i = 0; i < keyColumns.length; i++)
			bytesPerKey += keyColumnTypes[keyColumns[i]].getNumberOfBytes();
				
		//int nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.store.changetracking.generalkeys.nodesize"));
		this.tree = new BPlusTreeByteKeysOnly(nodeSize, bytesPerKey, keyColumnTypes/*, deALSConfiguration*/);
	}

	@Override
	public void add(long key) { }

	@Override
	public void add(int key) { }
	
	@Override
	public void add(byte[] key) {
		this.tree.insert(key);
	}

	@Override
	public int getNumberOfEntries() {
		return this.tree.getNumberOfEntries();
	}

	@Override
	public void delete() {
		if (this.tree != null)
			this.tree.deleteAll();
		this.tree = null;
	}
	
	@Override
	public void clear() {
		if (this.tree != null)
			this.tree.deleteAll();		
	}
	
	public BPlusTreeByteKeysOnly getTree() { return this.tree; }
}
