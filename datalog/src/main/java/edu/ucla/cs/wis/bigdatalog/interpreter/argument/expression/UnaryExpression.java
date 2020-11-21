package edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class UnaryExpression extends Expression implements Serializable {
	private static final long serialVersionUID = 1L;

	protected Argument argument;
	
	public UnaryExpression(ArithmeticOperation operation, Argument argument, DataType dataType) {
		super(operation, dataType);
		this.argument = argument;
	}
	
	public Argument getArgument() { return this.argument; }
	
	public void setArgument(Argument argument) { this.argument = argument; }

	@Override
	public boolean isGround() {
		return this.argument.isGround();
	}

	@Override
	public boolean isBound() {
		return this.argument.isBound();
	}

	@Override
	public boolean isConstant() { return false; }

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return this.argument.toDbType(typeManager);
	}

	@Override
	protected void initializeReduce() {
		if (this.argument instanceof Variable) {
			this.firstArgVariable = (Variable)this.argument;
			this.reduceOperation = 1;
		}

		this.reduceInitialized = true;
	}
	
	@Override
	public Argument reduce() {
		if (!this.reduceInitialized)
			this.initializeReduce();
		
		if (this.reduceOperation == 1)
			return this.arithmeticExpression.evaluate((DbNumericType)this.firstArgVariable.getValue());
	
		return this.arithmeticExpression.evaluate((DbNumericType)this.argument.reduce());
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		return false;
	}

	@Override
	public boolean matchByFree(Argument argument) {
		return false;
	}

	@Override
	public boolean matchByBound(Argument argument) {
		return false;
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.operation.toString());
		
		if (this.argument instanceof Expression) {
			output.append("(");
			output.append(this.argument.toString());		
			output.append(")");
		} else {
			output.append(" " + this.argument.toString());
		}		
				
		return output.toString();		
	}

	@Override
	public String toFact() {
		StringBuilder fact = new StringBuilder();
		fact.append("argument(");
		fact.append(this.hashCode());
		fact.append(",'expression','");
		fact.append(this.toString());
		fact.append("','");
		fact.append(this.getDataType());
		fact.append("').");
		return fact.toString();
	}
	@Override
	public Argument copy() {
		return new UnaryExpression(this.operation, this.argument.copy(), this.dataType);
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		return new UnaryExpression(this.operation, this.argument.copy(argumentList), this.dataType);
	}

}
