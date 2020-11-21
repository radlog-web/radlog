package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;

abstract public class DbTypeAggregatorHelper implements Serializable {
	private static final long serialVersionUID = 1L;
	public AggregateFunctionType aggregateType;
	
	public DbTypeAggregatorHelper(AggregateFunctionType aggregateType) {
		this.aggregateType = aggregateType;		
	}
	
	abstract public DbTypeBase doAggregation(DbTypeBase oldValue, DbTypeBase newValue);		
	
	public static DbTypeAggregatorHelper getAggregatorHelper(AggregateFunctionType aggregateType,TypeManager typeManager) {
		switch (aggregateType) {
			case MAX:
			case FSMAX:
				return new GreaterThan(aggregateType);
			case MIN:
			case FSMIN:
				return new LessThan(aggregateType); 
			case FSSUM:
				return new FSSum(typeManager);
			case COUNT:
				return new Count(typeManager);
			case SUM:
				return new Sum();
			case COUNT_DISTINCT:
				return new CountDistinct(typeManager);
			default:
				return new Average(typeManager);
		}
	}
}
