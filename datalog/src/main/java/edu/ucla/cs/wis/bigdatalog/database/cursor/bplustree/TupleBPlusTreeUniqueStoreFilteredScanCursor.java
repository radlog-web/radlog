package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FilteredScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;

public class TupleBPlusTreeUniqueStoreFilteredScanCursor 
	extends FilteredScanCursor<Tuple> {
	
	public TupleBPlusTreeUniqueStoreFilteredScanCursor(Relation<Tuple> relation, int[] filterColumns) {
		super(relation, filterColumns);
	}
}