package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.forest;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeStoreScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.forests.AggregatorBPlusTreeForestIntKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValuesLeaf;

public class AggregatorBPlusTreeForestIntKeysIntValuesScanCursor 
	extends AggregatorBPlusTreeStoreScanCursor<AggregatorBPlusTreeForestIntKeysIntValues, 
		AggregatorBPlusTreeIntKeysIntValuesLeaf> {

	protected int[] keys;
	protected int[] values;
	protected AggregatorBPlusTreeIntKeysIntValues[] forest;
	protected AggregatorBPlusTreeIntKeysIntValues currentTree;
	protected int treeIndex;
	
	public AggregatorBPlusTreeForestIntKeysIntValuesScanCursor(AggregateRelation relation) {
		super(relation);
	}
	
	@Override
	public void initialize() {
		if (this.storageStructure.isEmpty())
			return;
		
		this.forest = this.storageStructure.forest;
		this.currentTree = this.getNextTree();
		if (this.currentTree == null)
			return;
		
		this.currentLeaf = this.currentTree.getFirstChild();
		this.keyIndex = 0;
		if (this.currentLeaf != null) {
			this.keys = this.currentLeaf.getKeys();
			this.values = this.currentLeaf.getValues();
		}
	}
	
	private AggregatorBPlusTreeIntKeysIntValues getNextTree() {
		for (int i = this.treeIndex; i < this.forest.length; i++) {
			if (this.forest[i] != null) {
				this.treeIndex = i+1;
				return this.forest[i];
			}
		}
		return null;
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		// get leaf
		// get key & values
		// get all values 
		// when out, get next leaf
		// when out of leaves, done
		int status = 0;
		while (this.forest != null) {
			while (this.currentLeaf != null) {
				if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
					//System.out.println(this.keys[this.keyIndex] + " " + this.values[this.keyIndex]);
					status = this.currentTree.loadTuple(this.keys[this.keyIndex], this.values[this.keyIndex], tuple);
					this.keyIndex++;
					return status;
				}
			
				// 	out of keys, so move to next leaf
				this.currentLeaf = this.currentLeaf.getNext();
				if (this.currentLeaf == null)
					break;
				this.keyIndex = 0;
				this.keys = this.currentLeaf.getKeys();
				this.values = this.currentLeaf.getValues();
			}
			
			this.currentTree = this.getNextTree();
			if (this.currentTree == null)
				return 0;
			
			this.currentLeaf = this.currentTree.getFirstChild();
			this.keyIndex = 0;
			if (this.currentLeaf != null) {
				this.keys = this.currentLeaf.getKeys();
				this.values = this.currentLeaf.getValues();
			}
		}

		return 0;
	}	
}
