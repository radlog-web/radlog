package edu.ucla.cs.wis.bigdatalog.database.store.changetracking;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.intkeysbitmapvalues.BPlusTreeIntKeysBitmapValues;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class IntTreeIntBitmapChangeTracker extends ChangeTracker implements Serializable {
	private static final long serialVersionUID = 1L;

	protected BPlusTreeIntKeysBitmapValues tree;
	
	public IntTreeIntBitmapChangeTracker() { super(); }
	
	public IntTreeIntBitmapChangeTracker(int[] keyColumns, DataType[] keyColumnTypes, int nodeSize, TypeManager typeManager) {
		this.tree = new BPlusTreeIntKeysBitmapValues(nodeSize, keyColumns, keyColumnTypes, typeManager);
	}

	@Override
	public void add(long key) {
		this.tree.insert(key);
	}

	@Override
	public void add(int key) { this.tree.insert(key); }

	@Override
	public void add(byte[] key) { /*DO NOT IMPLEMENT*/ }

	@Override
	public int getNumberOfEntries() { return this.tree.getNumberOfEntries(); }

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
	
	public BPlusTreeIntKeysBitmapValues getTree() { return this.tree; }

}
