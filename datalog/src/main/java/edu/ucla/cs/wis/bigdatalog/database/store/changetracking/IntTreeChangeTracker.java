package edu.ucla.cs.wis.bigdatalog.database.store.changetracking;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnly;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class IntTreeChangeTracker extends ChangeTracker implements Serializable {
	private static final long serialVersionUID = 1L;

	protected BPlusTreeIntKeysOnly tree;
	
	public IntTreeChangeTracker() { super(); }
	
	public IntTreeChangeTracker(int[] keyColumns, DataType[] keyColumnTypes, int nodeSize/*, DeALSConfiguration deALSConfiguration*/) {
		//int nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.store.changetracking.intkeys.nodesize"));
		this.tree = new BPlusTreeIntKeysOnly(nodeSize, keyColumns, keyColumnTypes/*, deALSConfiguration*/);
	}

	@Override
	public void add(long key) {
		this.tree.insert((int)key);		
	}

	@Override
	public void add(int key) {
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
	
	@Override
	public void add(byte[] key) { /*DO NOT IMPLEMENT*/ }
	
	public BPlusTreeIntKeysOnly getTree() { return this.tree; }
	
	public void setTree(BPlusTreeIntKeysOnly tree) { this.tree = tree; }
}
