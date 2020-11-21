package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues.AggregatorBPlusTreeByteKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues.AggregatorBPlusTreeByteKeysDbTypeValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeByteKeysDbTypeValuesScanCursor 
	extends AggregatorBPlusTreeStoreScanCursor<AggregatorBPlusTreeByteKeysDbTypeValues, AggregatorBPlusTreeByteKeysDbTypeValuesLeaf> {

	protected int bytesPerKey;
	protected byte[] currentKey;
	protected byte[] keys;
	protected DbTypeBase[] values;
	
	public AggregatorBPlusTreeByteKeysDbTypeValuesScanCursor(AggregateRelation relation) {
		super(relation);
	}

	@Override
	public void initialize() {
		super.initialize();
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.values = this.currentLeaf.getValues();
			this.bytesPerKey = this.currentLeaf.getBytesPerKey();
			this.currentKey = new byte[this.bytesPerKey];
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
				System.arraycopy(this.keys, this.keyIndex * this.bytesPerKey, this.currentKey, 0, this.bytesPerKey);
				status = this.storageStructure.loadTuple(this.currentKey, this.values[this.keyIndex], tuple);
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
