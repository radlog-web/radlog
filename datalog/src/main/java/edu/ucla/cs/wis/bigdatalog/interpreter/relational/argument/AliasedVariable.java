package edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AliasedVariable
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;

	protected Variable variable;
	protected String alias;
	
	public AliasedVariable(Variable variable, String alias) {
		this.variable = variable;
		this.alias = alias;
	}
	
	public Variable getVariable() { return this.variable; }
	
	public void setVariable(Variable variable) { this.variable = variable; }
	
	public String getAlias() { return this.alias; }
		
	public String toString() {
		return this.variable.toString() + " as " + this.alias;		
	}

	@Override
	public DataType getDataType() {
		return this.variable.getDataType();
	}

	@Override
	public boolean isGround() {
		return this.variable.isGround();
	}

	@Override
	public boolean isBound() {
		return this.variable.isBound();
	}

	@Override
	public boolean isConstant() {
		return this.variable.isConstant();
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return this.variable.toDbType(typeManager);
	}

	@Override
	public Argument reduce() {
		return this.variable.reduce();
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		return this.variable.match(dbTypeObject);
	}

	@Override
	public boolean matchByFree(Argument argument) {
		return this.variable.matchByFree(argument);
	}

	@Override
	public boolean matchByBound(Argument argument) {
		return this.variable.matchByBound(argument);
	}

	@Override
	public String toFact() {
		return this.variable.toFact();
	}

	@Override
	public Argument copy() {
		return new AliasedVariable(this.variable, this.alias);
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		return new AliasedVariable((Variable) this.variable.copy(argumentList), this.alias);		
	}
	
	public boolean contains(Argument argument) {
		if (this.equals(argument))
			return true;
		
		if (this.variable.equals(argument))
			return true;
			
		return false;
	}
}