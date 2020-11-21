package edu.ucla.cs.wis.bigdatalog.compiler.aggregate;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class UserDefinedAggregate extends Aggregate {

	public UserDefinedAggregate(String aggregateName, CompilerTypeBase aggregateTerm) {
		super(aggregateName, aggregateTerm, CompilerType.USER_DEFINED_AGGREGATE, true);
	}

	@Override
	public CompilerTypeBase copy() {
		UserDefinedAggregate uda = new UserDefinedAggregate(this.aggregateName, this.aggregateTerm);
		uda.returnDataType = this.returnDataType;
		return uda;		
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		UserDefinedAggregate uda = new UserDefinedAggregate(this.aggregateName, this.aggregateTerm.copy(variableList));
		uda.returnDataType = this.returnDataType;
		return uda;		
	}
}
