package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class IndexSemiNaiveCursor extends SemiNaiveCursor {
	protected IndexCursor indexedCursor;
	
	protected IndexSemiNaiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) {
		super(relation, filteredColumns);
		this.indexedCursor = (IndexCursor)relation.getDatabase().getCursorManager().createIndexCursor(relation, filteredColumns);
	}
	
	public void reset(DbTypeBase[] values) {
		this.indexedCursor.reset(values);
	}

	public int getTuple(AddressedTuple tuple) {		
		while (this.indexedCursor.getTuple(tuple) > 0) {
			if ((this.baseTupleAddress <= tuple.address) 
					&& (tuple.address <= this.endTupleAddress))
				return 1;
		}

		return 0;
	}
}
