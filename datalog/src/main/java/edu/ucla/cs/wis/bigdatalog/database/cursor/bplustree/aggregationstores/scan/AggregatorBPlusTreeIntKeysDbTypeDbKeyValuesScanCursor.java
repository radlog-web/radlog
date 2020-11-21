package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues.AggregatorBPlusTreeIntKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues.AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesScanCursor 
	extends AggregatorBPlusTreeStoreScanCursor<AggregatorBPlusTreeIntKeysDbTypeDbKeyValues, AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf> {

	protected int[] keys;
	protected DbTypeBase[] totalValues;
	protected DbKeyValueStore[] keyValues;
	
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesScanCursor(AggregateRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize() {
		super.initialize();
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.totalValues = this.currentLeaf.getTotalValues();
			this.keyValues = this.currentLeaf.getKeyValues();
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
				status = this.storageStructure.loadTuple(this.keys[this.keyIndex], this.totalValues[this.keyIndex], 
						this.keyValues[this.keyIndex], tuple);
				this.keyIndex++;
				break;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			if (this.currentLeaf == null)
				break;
			this.keyIndex = 0;
			this.keys = this.currentLeaf.getKeys();
			this.totalValues = this.currentLeaf.getTotalValues();
			this.keyValues = this.currentLeaf.getKeyValues();
		}

		return status;
	}	
}
