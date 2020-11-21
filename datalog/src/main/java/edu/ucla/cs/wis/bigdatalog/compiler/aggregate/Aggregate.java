package edu.ucla.cs.wis.bigdatalog.compiler.aggregate;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class Aggregate extends CompilerTypeBase {

	protected CompilerTypeBase aggregateTerm;
	protected String aggregateName;
	protected boolean isStratified;
	protected DataType returnDataType;

	public Aggregate(String aggregateName, CompilerTypeBase aggregateTerm, CompilerType type, boolean isStratified)	{
		super(type);
		this.aggregateName = aggregateName;
		this.aggregateTerm = aggregateTerm;
		this.isStratified = isStratified;
		this.returnDataType = DataType.UNKNOWN;
	}

	public void setAggregateTerm(CompilerTypeBase aggregateTerm) {
		this.aggregateTerm = aggregateTerm;
	}

	public CompilerTypeBase getAggregateTerm() { return this.aggregateTerm; }

	public String getAggregateName() { return this.aggregateName; }
	
	public boolean isStratified() { return this.isStratified; }

	public DataType getReturnDataType() { return this.returnDataType; }
	
	public void setReturnDataType(DataType returnDataType) { 
		this.returnDataType = returnDataType; 
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append(this.aggregateName.toString());
		retval.append("<");
				
		if (this.aggregateTerm != null) {
			if (this.aggregateTerm instanceof CompilerTypeList) {
				CompilerTypeList args = (CompilerTypeList)this.aggregateTerm;
				for (int i = 0; i < args.size(); i++) {
					if (i > 0)
						retval.append(", ");
					retval.append(args.get(i).toString());
				}
			} else {
				retval.append(this.aggregateTerm.toString());
			}
		} else {
			retval.append("*");
		}

		retval.append(">");
		return retval.toString();
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
	
		if (!(other instanceof Aggregate))
			return false;
		
		Aggregate otherAggregate = (Aggregate)other;
		return (this.aggregateName.equals(otherAggregate.aggregateName) 
				&& ((this.aggregateTerm == null) || ((this.aggregateTerm != null) 
						&& this.aggregateTerm.equals(otherAggregate.getAggregateTerm()))) 
						&& this.isStratified == otherAggregate.isStratified());
	}
}