package edu.ucla.cs.wis.bigdatalog.database.store.changetracking;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.IntBitMap;

public class IntBitmapChangeTracker extends ChangeTracker implements Serializable {
	private static final long serialVersionUID = 1L;

	protected IntBitMap bitmap;
	public IntBitmapChangeTracker() {
		this.bitmap = new IntBitMap();
	}

	@Override
	public void add(long key) {
		this.bitmap.set((int)key);
	}

	@Override
	public void add(int key) {
		this.bitmap.set(key);
	}

	@Override
	public int getNumberOfEntries() {
		return this.bitmap.getNumberOfEntries();
	}

	@Override
	public void delete() {
		if (this.bitmap != null)
			this.bitmap.deleteAll();
		this.bitmap = null;
	}
	
	@Override
	public void clear() {
		if (this.bitmap != null)
			this.bitmap.clear();		
	}

	@Override
	public void add(byte[] key) { /*DO NOT IMPLEMENT*/}

	public IntBitMap getBitmap() { return this.bitmap; }
	
}
