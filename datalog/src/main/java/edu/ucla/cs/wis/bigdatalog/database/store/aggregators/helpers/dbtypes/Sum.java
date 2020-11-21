package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;

public class Sum extends DbTypeAggregatorHelper implements Serializable {
	private static final long serialVersionUID = 1L; 
	public Sum() { super(AggregateFunctionType.SUM); }
	
	public DbTypeBase doAggregation(DbTypeBase oldValue, DbTypeBase newValue) {
		return ((DbNumericType) oldValue).add((DbNumericType) newValue);
	}
}