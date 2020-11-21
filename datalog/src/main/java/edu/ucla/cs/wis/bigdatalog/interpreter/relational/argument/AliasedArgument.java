package edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AliasedArgument
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;

	protected Argument argument;
	protected Argument alias;
	
	public AliasedArgument(Argument argument, Argument alias) {
		this.argument = argument;
		this.alias = alias;
	}

	public Argument getArgument() { return this.argument; }
	
	public void setArgument(Argument argument) { this.argument = argument; }
	
	public Argument getAlias() { return this.alias; }
	
	public void setAlias(Argument alias) { this.alias = alias; }

	@Override
	public DataType getDataType() {
		return this.argument.getDataType();
	}

	@Override
	public boolean isGround() {
		return this.argument.isGround();
	}

	@Override
	public boolean isBound() {
		return this.argument.isBound();
	}

	@Override
	public boolean isConstant() {
		return this.argument.isConstant();
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return this.argument.toDbType(typeManager);
	}

	@Override
	public Argument reduce() {
		return this.argument.reduce();
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		return this.argument.match(dbTypeObject);
	}

	@Override
	public boolean matchByFree(Argument argument) {
		return this.argument.matchByFree(argument);
	}

	@Override
	public boolean matchByBound(Argument argument) {
		return this.argument.matchByBound(argument);
	}

	@Override
	public String toFact() {
		return this.argument.toFact();
	}

	@Override
	public Argument copy() {
		return this.argument.copy();
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		return this.argument.copy(argumentList);
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.argument.toString());
		output.append(" as ");
		if (this.alias instanceof Variable)
			output.append(((Variable)this.alias).toString());
		else if (this.alias.isConstant())
			output.append(this.alias.toString());
		return output.toString();
	}
	
	public boolean contains(Argument argument) {
		if (this.equals(argument))
			return true;
		
		if ((this.argument instanceof Variable) && ((Variable)this.argument).equals(argument))
			return true;
		
		if ((this.alias instanceof Variable) && ((Variable)this.alias).equals(argument))
			return true;
				
		return false;
	}
}
