package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public interface SelectionCursor<T extends Tuple> {
	public void reset(DbTypeBase[] indexColumnValues);
	
	public int getTuple(T tuple);	
	
	public int[] getFilterColumns();
	
	public DbTypeBase[] getFilter();
}
