package edu.ucla.cs.wis.bigdatalog.interpreter;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;

public enum AggregateFunctionType {
	MIN,
	MAX,
	COUNT,
	SUM,
	AVG,
	COUNT_DISTINCT,
	FSMIN,
	FSMAX,
	FSCNT,
	FSSUM;
	
	public static AggregateFunctionType getAggregateFunctionType(BuiltInAggregateType aggregateType) {
		switch(aggregateType) {
		case MIN:
			return MIN;
		case MAX:
			return MAX;
		case COUNT:
			return COUNT;
		case SUM:
			return SUM;
		case AVG:
			return AVG;
		case COUNT_DISTINCT:
			return COUNT_DISTINCT;
		default:
			return null;
		}
	}
	
	public static AggregateFunctionType getAggregateFunctionType(FSAggregateType aggregateType) {
		switch(aggregateType) {
		case FSMIN:
			return FSMIN;
		case FSMAX:
			return FSMAX;
		case FSCNT:
			return FSCNT;
		case FSSUM:
			return FSSUM;
		default:
			return null;
		}
	}
}
