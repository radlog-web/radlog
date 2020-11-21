package edu.ucla.cs.wis.bigdatalog.interpreter;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AggregateInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	public AggregateFunctionType 	aggregateType;
	public DataType 				dataType;
	//public int 					index;	// position in input tuple which aggregate aggregates
	public Argument 				sourceArgument;

	public AggregateInfo() {}
	
	public AggregateInfo(BuiltInAggregateType builtInAggregateType) {
		this(builtInAggregateType, DataType.UNKNOWN);
	}
	
	public AggregateInfo(BuiltInAggregateType builtInAggregateType, DataType dataType) {
		this.aggregateType = AggregateFunctionType.getAggregateFunctionType(builtInAggregateType);
		this.dataType = dataType;
	}

	public AggregateInfo(FSAggregateType fsAggregateType, DataType dataType) {
		this.aggregateType = AggregateFunctionType.getAggregateFunctionType(fsAggregateType);
		this.dataType = dataType;
	}
	
	/*public String toString() {
		return this.aggregateType + " " + this.dataType + " " + this.sourceArgument.toString();
	}*/
}
