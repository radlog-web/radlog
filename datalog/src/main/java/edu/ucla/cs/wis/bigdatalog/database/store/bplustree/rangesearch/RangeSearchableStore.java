package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public interface RangeSearchableStore {
	
	public int getBytesPerKey();
	
	public int[] getKeyColumns();
	
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple);
	
	public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor);
}
