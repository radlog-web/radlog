package edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class ComparisonExpression
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;

	protected ComparisonOperation operation;
	protected Argument left;
	protected Argument right;
	
	public ComparisonExpression(ComparisonOperation operation, Argument left, Argument right) {
		this.operation = operation;
		this.left = left;
		this.right = right;
	}
	
	public ComparisonOperation getOperation() { return this.operation; }
	
	public Argument getLeft() { return this.left; }
	
	public void setLeft(Argument left) { this.left = left; }
	
	public Argument getRight() { return this.right; }
	
	public void setRight(Argument right) { this.right = right; }

	@Override
	public DataType getDataType() {
		return null;
	}

	@Override
	public boolean isGround() {
		return (this.left.isGround() && this.right.isGround());
	}

	@Override
	public boolean isBound() {
		return (this.left.isBound() && this.right.isBound());
	}

	@Override
	public boolean isConstant() {
		return (this.left.isConstant() && this.right.isConstant());
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		return null;
	}

	@Override
	public Argument reduce() {
		return null;
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
	public String toFact() {
		return null;
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		if ((this.left instanceof DbString) || (this.left instanceof DbDateTime)) output.append("'");
		output.append(this.left);
		if ((this.left instanceof DbString) || (this.left instanceof DbDateTime)) output.append("'");
		output.append(" ");
		output.append(this.operation.getSymbol());
		output.append(" ");
		if ((this.right instanceof DbString) || (this.right instanceof DbDateTime)) output.append("'");
		output.append(this.right);
		if ((this.right instanceof DbString) || (this.right instanceof DbDateTime)) output.append("'");
		return output.toString();
	}

	@Override
	public Argument copy() {
		return new ComparisonExpression(this.operation, this.left.copy(), this.right.copy());
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		return new ComparisonExpression(this.operation, this.left.copy(argumentList), this.right.copy(argumentList));
	}

}
