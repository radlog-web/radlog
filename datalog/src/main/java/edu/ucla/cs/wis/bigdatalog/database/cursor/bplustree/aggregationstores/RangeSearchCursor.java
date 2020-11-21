package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class RangeSearchCursor 
	extends Cursor<Tuple> 
	implements SelectionCursor<Tuple> {
	
	protected RangeSearchableStore tupleStore;
	protected DbTypeBase[] keyValues;
	protected int numberOfKeyColumns;
	protected boolean done;
	protected RangeSearchResultCursor rangeSearchCursor;
		
	public RangeSearchCursor(Relation relation) {
		super(relation);
		this.tupleStore = (RangeSearchableStore) relation.getTupleStore();
		this.numberOfKeyColumns = this.tupleStore.getKeyColumns().length;
		this.rangeSearchCursor = relation.getDatabase().getCursorManager().createRangeSearchResultCursor(this.relation);
	}

	@Override
	public void reset() { this.done = false; }

	@Override
	public void reset(DbTypeBase[] keyValues) {
		this.keyValues = keyValues;
		this.done = false;
		this.rangeSearchCursor.reset(keyValues);
	}

	@Override
	public int[] getFilterColumns() {
		return this.tupleStore.getKeyColumns();
	}

	@Override
	public DbTypeBase[] getFilter() {
		return this.keyValues;
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		// there will only be one exact match
		// if we're not using an exact match filter, we have to get more
		if (this.done)
			return 0;
		
		if (this.keyValues.length == this.numberOfKeyColumns) {
			this.done = true;
			return this.tupleStore.getTuple(this.keyValues, tuple);
		}
		
		if (!this.rangeSearchCursor.hasResults()) {
			RangeSearchKeys<?> keys = RangeSearchKeys.createRangeSearchKeys(this.tupleStore, this.keyValues);
			if (this.tupleStore.getTuple(keys, this.rangeSearchCursor) == 0) {
				this.done = true;
				return 0;
			}			
		}
		int status = this.rangeSearchCursor.getTuple(tuple);
		if (status > 0)
			return 1;
		
		this.done = true;
		return 0;		
	}

	@Override
	public void moveNext() {}

}
