package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;

public class LessThan extends DbTypeAggregatorHelper implements Serializable {
	private static final long serialVersionUID = 1L;
	public LessThan(AggregateFunctionType aggregateType) { super(aggregateType); }
	
	public DbTypeBase doAggregation(DbTypeBase oldValue, DbTypeBase newValue) {
		if (newValue.lessThan(oldValue))
			return newValue;
		return oldValue;
	}
}