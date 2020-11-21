package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

abstract public class AggregatorBPlusTreeStoreScanCursor<T extends TupleAggregationStoreStructure, L extends BPlusTreeLeaf<?>> 
	extends Cursor<Tuple> {

	protected TupleAggregationStore tupleStore;
	protected T						storageStructure;
	protected L				 		currentLeaf;
	protected int						keyIndex;	
		
	@SuppressWarnings("unchecked")
	public AggregatorBPlusTreeStoreScanCursor(AggregateRelation relation) {
		super(relation);
		if (!(relation.getTupleStore() instanceof TupleAggregationStore))
			throw new DatabaseException("TupleAggregationStoreScanCursor can only be used on a TupleAggregationStore.");
		
		this.tupleStore = (TupleAggregationStore)relation.getTupleStore();
		this.storageStructure = (T) ((TupleAggregationStore)relation.getTupleStore()).storageStructure;
						
		this.initialize();
	}
	
	@SuppressWarnings("unchecked")
	public void initialize() {
		this.currentLeaf = (L) this.tupleStore.getFirstChild();
		this.keyIndex = 0;
	}

	public void reset() {
		this.initialize();
	}
	
	@Override
	abstract public int getTuple(Tuple tuple);
	
	@Override
	public void moveNext() { this.keyIndex++; }
}