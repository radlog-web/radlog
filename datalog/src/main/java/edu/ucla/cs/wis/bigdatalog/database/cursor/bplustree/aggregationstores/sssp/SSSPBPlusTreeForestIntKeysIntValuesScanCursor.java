package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.sssp;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class SSSPBPlusTreeForestIntKeysIntValuesScanCursor 
	extends Cursor<Tuple> {
	
	protected LinkedHashMap<DbTypeBase, AggregateRelation> relations;
	protected Iterator<Entry<DbTypeBase, AggregateRelation>> iterator;
	
	protected DbTypeBase							currentKey;
	protected AggregatorBPlusTreeIntKeysIntValues currentTree;
	
	protected AggregatorBPlusTreeIntKeysIntValuesLeaf	currentLeaf;
	protected int						keyIndex;
	protected int[] keys;
	protected int[] values;
	protected Tuple twoTuple;
	
	public SSSPBPlusTreeForestIntKeysIntValuesScanCursor(Relation<Tuple> relation) {
		super(relation);
	}
	
	public void setRelations(LinkedHashMap<DbTypeBase, AggregateRelation> relations) {
		this.relations = relations;
	
		if (this.relations == null || this.relations.size() == 0)
			return;
				
		this.iterator = this.relations.entrySet().iterator();

		if (!this.getNextRelation())
			return;		
		
		this.getNextLeaf();
		
		this.twoTuple = new Tuple(2);
		this.twoTuple.columns[0] = DbTypeBase.loadFrom(this.currentTree.getKeyColumnTypes()[0], 0);
		this.twoTuple.columns[1] = DbTypeBase.loadFrom(this.currentTree.getValueColumnTypes()[0], 0);
	}
	
	private boolean getNextLeaf() {
		if (this.currentLeaf == null)
			this.currentLeaf = this.currentTree.getFirstChild();
		else
			this.currentLeaf = this.currentLeaf.getNext();
		
		if (this.currentLeaf == null)
			return false;
		
		this.keys = this.currentLeaf.getKeys();
		this.values = this.currentLeaf.getValues();
		this.keyIndex = 0;
		
		return true;
	}
	
	private boolean getNextRelation() {
		if (this.iterator.hasNext()) {
			Entry<DbTypeBase, AggregateRelation> entry = this.iterator.next();
			
			this.currentKey = entry.getKey();
			this.currentTree = (AggregatorBPlusTreeIntKeysIntValues) ((TupleAggregationStore)entry.getValue().getTupleStore()).storageStructure;
			return true;
		}
		this.currentKey = null;
		this.currentTree = null;
		return false;
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		// get leaf
		// get key & values
		// get all values 
		// when out, get next leaf
		// when out of leaves, done
		int status = 0;
		while (this.currentTree != null) {
			while (this.currentLeaf != null) {
				if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
					//System.out.println(this.keys[this.keyIndex] + " " + this.values[this.keyIndex]);
					status = this.currentTree.loadTuple(this.keys[this.keyIndex], this.values[this.keyIndex], tuple);
					this.keyIndex++;
					/*if (status > 0) {
						//tuple.columns[0] = this.currentKey;
						tuple.columns[0] = this.twoTuple.columns[0];
						tuple.columns[1] = this.twoTuple.columns[1];
					}*/
					return status;
				}
			
				// out of keys, so move to next leaf
				// if out of leaves, move to next tree
				if (!this.getNextLeaf())
					break;
			}
			
			if (!this.getNextRelation())
				return 0;
			
			this.getNextLeaf();
		}

		return 0;
	}
	
	@Override
	public void moveNext() { }

	@Override
	public void reset() { }

}
