package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class IndexNaiveCursor extends NaiveCursor {
	protected IndexCursor indexedCursor;
	
	protected IndexNaiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) {
		super(relation, filteredColumns);
		this.indexedCursor = (IndexCursor)relation.getDatabase().getCursorManager().createIndexCursor(relation, filteredColumns);
	}
	
	public void reset(DbTypeBase[] values) {
		this.indexedCursor.reset(values);
	}

	@Override
	public int getTuple(AddressedTuple tuple) {		
		while (this.indexedCursor.getTuple(tuple) > 0) {
			if (tuple.address <= this.endTupleAddress)
				return 1;
		}

		return 0;
	}
}
