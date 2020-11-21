package edu.ucla.cs.wis.bigdatalog.compiler.aggregate;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class FSAggregate extends BuiltInAggregate {

	public static final String FSMAX_NAME = "fsmax";
	public static final String FSMAX_NAME2 = "mmax";
	public static final String FSMIN_NAME = "fsmin";
	public static final String FSMIN_NAME2 = "mmin";
	public static final String FSCNT_NAME = "fscnt";
	public static final String FSCNT_NAME2 = "mcount";
	public static final String FSSUM_NAME = "fssum";
	public static final String FSSUM_NAME2 = "msum";
	
	protected FSAggregateType fsAggregateType;

	public FSAggregate(String aggregateName, CompilerTypeBase aggregateTerm) {
		super(aggregateName, aggregateTerm, CompilerType.FS_AGGREGATE, false);
		
		if (aggregateName.equals(FSAggregate.FSMAX_NAME) || aggregateName.equals(FSAggregate.FSMAX_NAME2)) {
			this.fsAggregateType = FSAggregateType.FSMAX;
		} else if (aggregateName.equals(FSAggregate.FSCNT_NAME) || aggregateName.equals(FSAggregate.FSCNT_NAME2)) {
			this.fsAggregateType = FSAggregateType.FSCNT; 
		} else if (aggregateName.equals(FSAggregate.FSMIN_NAME) || aggregateName.equals(FSAggregate.FSMIN_NAME2)) {
			this.fsAggregateType = FSAggregateType.FSMIN;
		} else if (aggregateName.equals(FSAggregate.FSSUM_NAME) || aggregateName.equals(FSAggregate.FSSUM_NAME2) ) {
			this.fsAggregateType = FSAggregateType.FSSUM;
		} 		
	}

	public FSAggregateType getFSAggregateType() {return this.fsAggregateType;}
	
	public void useAlternativeName() { 
		if (this.aggregateName.equals(FSMAX_NAME))
			this.aggregateName = FSMAX_NAME2;
		else if (this.aggregateName.equals(FSMIN_NAME))
			this.aggregateName = FSMIN_NAME2;
		else if (this.aggregateName.equals(FSCNT_NAME))
			this.aggregateName = FSCNT_NAME2;
		else if (this.aggregateName.equals(FSSUM_NAME))
			this.aggregateName = FSSUM_NAME2;
	}

	public FSAggregate copy() {
		FSAggregate aggr =  new FSAggregate(this.aggregateName, 
				(this.aggregateTerm != null ? this.aggregateTerm.copy() : null));
		aggr.returnDataType = this.returnDataType;
		return aggr;
	}

	public FSAggregate copy(CompilerVariableList variable_list) {
		FSAggregate aggr = new FSAggregate(this.aggregateName, 
				(this.aggregateTerm != null ? this.aggregateTerm.copy(variable_list) : null));
		aggr.returnDataType = this.returnDataType;
		return aggr;
	}

	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;

		if (!(other instanceof FSAggregate))
			return false;

		FSAggregate otherFSAggregate = (FSAggregate)other;

		return ((this.fsAggregateType == otherFSAggregate.getFSAggregateType())
				&& (this.aggregateName.equals(otherFSAggregate.getAggregateName()))
				&& ((this.aggregateTerm == null) && (otherFSAggregate.getAggregateTerm() == null) ||
						((this.aggregateTerm != null)
								&& (this.aggregateTerm.equals(otherFSAggregate.getAggregateTerm())))));
	}

}
