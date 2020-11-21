package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValuesLeaf;

public class AggregatorBPlusTreeIntKeysIntValuesScanCursor 
	extends AggregatorBPlusTreeStoreScanCursor<AggregatorBPlusTreeIntKeysIntValues, AggregatorBPlusTreeIntKeysIntValuesLeaf> {

	protected int[] keys;
	protected int[] values;
	
	public AggregatorBPlusTreeIntKeysIntValuesScanCursor(AggregateRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize() {
		super.initialize();
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.values = this.currentLeaf.getValues();
		}
	}
		
	@Override
	public int getTuple(Tuple tuple) {
		// get leaf
		// get key & values
		// get all values 
		// when out, get next leaf
		// when out of leaves, done
		int status = 0;
		while (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				status = this.storageStructure.loadTuple(this.keys[this.keyIndex], this.values[this.keyIndex], tuple);
				this.keyIndex++;
				break;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			if (this.currentLeaf == null)
				break;

			this.keyIndex = 0;
			this.keys = this.currentLeaf.getKeys();
			this.values = this.currentLeaf.getValues();
		}

		return status;
	}	
}
