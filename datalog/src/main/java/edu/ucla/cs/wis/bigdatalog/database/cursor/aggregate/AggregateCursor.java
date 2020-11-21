package edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public interface AggregateCursor<T extends Tuple> {
	public int getTuple(T tuple);
}
