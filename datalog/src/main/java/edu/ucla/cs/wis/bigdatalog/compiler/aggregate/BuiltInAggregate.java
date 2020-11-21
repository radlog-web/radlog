package edu.ucla.cs.wis.bigdatalog.compiler.aggregate;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class BuiltInAggregate extends Aggregate {
	protected BuiltInAggregateType builtInAggregateType;	
	
	public BuiltInAggregate(String aggregateName, CompilerTypeBase aggregateTerm, CompilerType compilerType, boolean isStratified) {
		super(aggregateName, aggregateTerm, compilerType, isStratified);		
		this.builtInAggregateType = BuiltInAggregateType.getBuiltInAggregateType(aggregateName);
	}
	
	public BuiltInAggregate(String aggregateName, CompilerTypeBase aggregateTerm) {
		this(aggregateName, aggregateTerm, CompilerType.BUILT_IN_AGGREGATE, true);
	}
	
	public BuiltInAggregate(BuiltInAggregateType builtInAggregateType, CompilerTypeBase aggregateTerm) {
		this(builtInAggregateType.getName(), aggregateTerm, CompilerType.BUILT_IN_AGGREGATE, true);
		this.builtInAggregateType = builtInAggregateType;
	}
	
	public BuiltInAggregateType getBuiltInAggregateType() {return this.builtInAggregateType;}
	
	public BuiltInAggregate copy() {
		BuiltInAggregate bia = new BuiltInAggregate(this.aggregateName, 
				this.aggregateTerm != null ? this.aggregateTerm.copy() : null,
						this.type, this.isStratified);
		bia.builtInAggregateType = this.getBuiltInAggregateType();
		bia.returnDataType = this.returnDataType;
		return bia;
	}

	public BuiltInAggregate copy(CompilerVariableList variableList) {
		BuiltInAggregate bia = new BuiltInAggregate(this.aggregateName, 
				this.aggregateTerm != null ? this.aggregateTerm.copy(variableList) : null,
						this.type, this.isStratified);
		bia.builtInAggregateType = this.getBuiltInAggregateType();
		bia.returnDataType = this.returnDataType;
		return bia;
	}

	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof BuiltInAggregate))
			return false;
		
		BuiltInAggregate otherBuiltInAggregate = (BuiltInAggregate)other;
		
		return ((this.builtInAggregateType == otherBuiltInAggregate.getBuiltInAggregateType())
				&& (this.aggregateName.equals(otherBuiltInAggregate.getAggregateName()))
				&& ((this.aggregateTerm == null) && (otherBuiltInAggregate.getAggregateTerm() == null) ||
						((this.aggregateTerm != null)
								&& (this.aggregateTerm.equals(otherBuiltInAggregate.getAggregateTerm()))))
				&& (this.isStratified == otherBuiltInAggregate.isStratified()));
	}	
}
