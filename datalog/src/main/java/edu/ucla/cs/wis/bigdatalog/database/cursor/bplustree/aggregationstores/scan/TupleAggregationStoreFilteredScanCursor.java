package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FilteredScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;

public class TupleAggregationStoreFilteredScanCursor 
	extends FilteredScanCursor<Tuple> {

	public TupleAggregationStoreFilteredScanCursor(Relation<Tuple> relation, int[] filterColumns) {
		super(relation, filterColumns);
	}

}
