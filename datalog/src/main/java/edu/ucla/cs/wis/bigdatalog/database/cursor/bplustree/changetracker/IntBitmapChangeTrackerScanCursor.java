package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.IntBitmapChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.IntBitMap;

public class IntBitmapChangeTrackerScanCursor 
	extends ChangeTrackerCursor<ChangeTrackingStore<Integer>> {

	protected int[] keys;
	protected IntBitMap bitmap;
	
	public IntBitmapChangeTrackerScanCursor(DerivedRelation relation) {
		super(relation);
	}

	@Override
	public void initialize() {
		super.initialize();
		if (this.deltaSKeys == null) {
			this.bitmap = null;
			this.keyIndex = -1;
		} else {
			this.bitmap = ((IntBitmapChangeTracker)this.deltaSKeys).getBitmap();
			this.keyIndex = 0;
		}
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		if (this.keyIndex == -1)
			return 0;
		
		int key = this.bitmap.nextSetBit(this.keyIndex);
		this.keyIndex = key + 1;
		if (key > -1)
			return this.store.getTuple(key, tuple);
		this.keyIndex = -1;
		return 0;
	}
}
