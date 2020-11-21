package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public interface DbTypeNeedsInitialization {	
	public DbTypeBase doAggregation(DbTypeBase newValue);	
}
