package edu.ucla.cs.wis.bigdatalog.database.store.changetracking;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly.BPlusTreeLongKeysOnly;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class LongTreeChangeTracker extends ChangeTracker implements Serializable {
	private static final long serialVersionUID = 1L;

	protected BPlusTreeLongKeysOnly tree;
	
	public LongTreeChangeTracker() { super(); }
	
	public LongTreeChangeTracker(int[] keyColumns, DataType[] keyColumnTypes, int nodeSize) {
		this.tree = new BPlusTreeLongKeysOnly(nodeSize, keyColumns, keyColumnTypes);
	}

	@Override
	public void add(long key) {
		this.tree.insert(key);		
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
	
	public BPlusTreeLongKeysOnly getTree() { return this.tree; }
	
	public void setTree(BPlusTreeLongKeysOnly tree) { this.tree = tree; }
}
