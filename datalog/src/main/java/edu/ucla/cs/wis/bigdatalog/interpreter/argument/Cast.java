package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class Cast implements Argument, Serializable {
	private static final long serialVersionUID = 1L;

	private Argument value;
	private DataType dataType;
	
	public Cast(Argument value, DataType dataType) {
		this.value = value;
		this.dataType = dataType;
	}

	@Override
	public DataType getDataType() { return this.dataType; }

	public Argument getValue() { 
		return this.value; 
	}
	
	@Override
	public boolean isGround() {
		return this.value.isGround();
	}

	@Override
	public boolean isBound() {
		return this.value.isBound();
	}

	@Override
	public boolean isConstant() {
		return this.value.isConstant();
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return this.value.toDbType(typeManager);
	}

	@Override
	public Argument reduce() {
		Argument reducedArgument = this.value.reduce();
		if (reducedArgument instanceof DbNumericType)
			return DataType.cast((DbNumericType) reducedArgument, this.dataType);
		else if (reducedArgument instanceof Variable)
			return DataType.cast((DbNumericType)((Variable) reducedArgument).getValue(), this.dataType);
		return null;
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		return this.value.match(dbTypeObject);
	}

	@Override
	public boolean matchByFree(Argument argument) {
		return this.value.matchByFree(argument);
	}

	@Override
	public boolean matchByBound(Argument argument) {
		return this.value.matchByBound(argument);
	}

	@Override
	public String toFact() {
		StringBuilder fact = new StringBuilder();
		fact.append("cast(");
		fact.append(this.hashCode());
		// remove period from value's fact representation
		String valueOutput = this.value.toFact();
		fact.append(valueOutput.substring(0,  valueOutput.length()-2));
		fact.append(" as ");
		fact.append(this.dataType);
		fact.append(").");
		return fact.toString();
	}

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("cast(");
		output.append(this.value.toString());
		output.append(") as ");
		output.append(this.dataType);
		return output.toString();
	}
	
	@Override
	public Argument copy() {
		return new Cast(this.value.copy(), this.dataType);
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		return new Cast(this.value.copy(argumentList), this.dataType);
	}

}
