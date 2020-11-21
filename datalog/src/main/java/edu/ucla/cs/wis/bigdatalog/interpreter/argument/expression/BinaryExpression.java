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

public class BinaryExpression extends Expression implements Serializable {
	private static final long serialVersionUID = 1L;

	private Argument left;
	private Argument right;	
		
	public BinaryExpression(ArithmeticOperation operation, Argument left, Argument right, DataType dataType) {
		super(operation, dataType);
		this.left = left;
		this.right = right;
	}

	public Argument getLeft() { return this.left; }
	
	public void setLeft(Argument left) { this.left = left; }
	
	public Argument getRight() { return this.right; }
	
	public void setRight(Argument right) { this.right = right; }

	@Override
	public boolean isGround() {
		return (this.left.isGround() && this.right.isGround());
	}

	@Override
	public boolean isBound() {
		return (this.left.isBound() && this.right.isBound());
	}

	@Override
	public boolean isConstant() { return false; }

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) { return this.reduce().toDbType(typeManager); }

	@Override
	public void initializeReduce() {
		boolean firstArgIsVariable = (this.left instanceof Variable);
		if (firstArgIsVariable)
			this.firstArgVariable = (Variable)this.left;
		
		boolean secondArgIsVariable = (this.right instanceof Variable);		
		if (secondArgIsVariable)
			this.secondArgVariable = (Variable)this.right;
		
		if (firstArgIsVariable)
			if (secondArgIsVariable)
				this.reduceOperation = 1;
			else
				this.reduceOperation = 2;
		else
			if (secondArgIsVariable)
				this.reduceOperation = 3;
			else
				this.reduceOperation = 4;
		
		this.reduceInitialized = true;
	}
	
	@Override
	public Argument reduce() {
		DbNumericType value = this.arithmeticExpression.evaluate((DbNumericType)this.left.reduce(), (DbNumericType)this.right.reduce());
		
		if (value.getDataType() != this.dataType)
			value = DataType.cast(value, this.dataType);
		return value;
		
		/*if (!this.reduceInitialized)
			this.initializeReduce();
		
		DbTypeBase value = null; 
		switch (this.reduceOperation) {
			case 1:
				value = this.arithmeticExpression.evaluate((DbTypeBase)this.firstArgVariable.getValue(), (DbTypeBase)this.secondArgVariable.getValue());
				break;
			case 2:
				value = this.arithmeticExpression.evaluate((DbTypeBase)this.firstArgVariable.getValue(), (DbTypeBase)this.right.reduce());
				break;
			case 3:
				value = this.arithmeticExpression.evaluate((DbTypeBase)this.left.reduce(), (DbTypeBase)this.secondArgVariable.getValue());
				break;
			case 4:
				value = this.arithmeticExpression.evaluate((DbTypeBase)this.left.reduce(), (DbTypeBase)this.right.reduce());
				break;
		}
		
		if (value.getDataType() != this.dataType)
			value = DataType.cast(value, this.dataType);
		
		return value;*/
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
		if (this.left instanceof Expression) {
			output.append("(");
			output.append(this.left.toString());
			output.append(")");
		} else {
			output.append(this.left.toString());
		}
		output.append(" ");
		output.append(this.operation.toString());
		output.append(" ");
		
		if (this.right instanceof Expression) {
			output.append("(");
			output.append(this.right.toString());
			output.append(")");
		} else {
			output.append(this.right.toString());
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
		return new BinaryExpression(this.operation, this.left.copy(), this.right.copy(), this.dataType);
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		return new BinaryExpression(this.operation, this.left.copy(argumentList), this.right.copy(argumentList), this.dataType);
	}

}
