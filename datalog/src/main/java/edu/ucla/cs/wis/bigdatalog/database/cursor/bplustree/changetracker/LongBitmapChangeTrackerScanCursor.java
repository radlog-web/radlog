package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.bitmap.IntBitMap;

// APS 7/25/2014
// this design appears too be too memory intensive - fix later
public class LongBitmapChangeTrackerScanCursor 
	extends ChangeTrackerCursor<ChangeTrackingStore<Long>> {

	protected long[] keys;
	protected IntBitMap upperKeys;
	protected IntBitMap lowerKeys;
	protected int upperKeyIndex;
	protected int lowerKeyIndex;
	
	public LongBitmapChangeTrackerScanCursor(DerivedRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize() {
		super.initialize();
/*
		if (this.deltaSKeys == null)
			this.upperKeys = null;	
		else
			this.upperKeys = this.deltaSKeys.longBitmap.upperKeys;
		*/
		this.upperKeyIndex = 0;
		this.lowerKeys = null;
		this.lowerKeyIndex = -1;
	}
	
	@Override
	public void reset() {
		this.upperKeyIndex = 0;
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		/*int lowerKey;
		while (true) {
			if (this.lowerKeyIndex == -1) {
				int upperKey = this.upperKeys.nextSetBit(this.upperKeyIndex);
				if (upperKey == -1)
					return 0;
				
				this.upperKeyIndex = upperKey + 1;
				this.lowerKeys = this.deltaSKeys.longBitmap.lowerKeys.get(upperKey);
				this.lowerKeyIndex = 0;
			}
						
			lowerKey = this.lowerKeys.nextSetBit(this.lowerKeyIndex);
			if (lowerKey > -1)
				break;
						
			// if we checked both positive and negative, we need to get next upper key at top of loop
			this.lowerKeyIndex = 0;
			lowerKey = this.lowerKeys.nextSetBit(this.lowerKeyIndex);
			if (lowerKey > -1)
				break;
		}

		this.lowerKeyIndex = lowerKey + 1;
		
		long key = (((long)(this.upperKeyIndex - 1) << 32) | ((this.lowerKeyIndex - 1) & 0xffffffffL));
		return this.storageStructure.getTuple(key, tuple);*/
		return 0;
	}
}
