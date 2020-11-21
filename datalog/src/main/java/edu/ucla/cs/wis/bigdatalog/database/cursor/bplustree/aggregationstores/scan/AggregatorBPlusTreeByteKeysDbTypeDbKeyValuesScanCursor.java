package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues.AggregatorBPlusTreeByteKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues.AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesScanCursor 
	extends AggregatorBPlusTreeStoreScanCursor<AggregatorBPlusTreeByteKeysDbTypeDbKeyValues, AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf> {

	protected int bytesPerKey;
	protected byte[] currentKey;
	protected byte[] keys;
	protected DbTypeBase[] totalValues;
	protected DbKeyValueStore[] keyValues;
	
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesScanCursor(AggregateRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize() {
		super.initialize();
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.totalValues = this.currentLeaf.getTotalValues();
			this.keyValues = this.currentLeaf.getKeyValues();
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
				status = this.storageStructure.loadTuple(this.currentKey, this.totalValues[this.keyIndex], 
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
