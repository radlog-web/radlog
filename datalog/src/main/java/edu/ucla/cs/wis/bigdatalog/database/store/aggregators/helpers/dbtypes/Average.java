package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbAverage;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;

public class Average extends DbTypeAggregatorHelper implements DbTypeNeedsInitialization, Serializable {
	private static final long serialVersionUID = 1L;
	protected TypeManager typeManager;
	
	public Average(TypeManager typeManager) { 
		super(AggregateFunctionType.AVG);
		this.typeManager = typeManager;
	}
	
	@Override
	public DbTypeBase doAggregation(DbTypeBase oldValue, DbTypeBase newValue) {
		return ((DbAverage)oldValue).accrue((DbNumericType) newValue);
	}
	
	@Override
	public DbTypeBase doAggregation(DbTypeBase newValue) {
		return DbAverage.create((DbNumericType) newValue);
	}		
	
}