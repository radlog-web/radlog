package edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument;

import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.interpreter.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;

public class JoinConditionExpression 
	extends ComparisonExpression {
	
	private static final long serialVersionUID = 1L;
	public int leftRelationIndex;
	public int rightRelationIndex;
	
	public JoinConditionExpression(ComparisonOperation operation, int leftRelationIndex, Argument left,
			int rightRelationIndex, Argument right) {
		super(operation, left, right);
		this.leftRelationIndex = leftRelationIndex;
		this.rightRelationIndex = rightRelationIndex;
	}

	public int getLeftRelationIndex() { return this.leftRelationIndex; }
	
	public void setLeftRelationIndex(int leftRelationIndex) { this.leftRelationIndex = leftRelationIndex; }
	
	public int getRightRelationIndex() { return this.rightRelationIndex; }
	
	public void setRightRelationIndex(int rightRelationIndex) { this.rightRelationIndex = rightRelationIndex; }
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.leftRelationIndex);
		output.append(".");
		if ((this.left instanceof DbString) || (this.left instanceof DbDateTime)) output.append("'");
		output.append(this.left);
		
		if ((this.left instanceof DbString) || (this.left instanceof DbDateTime)) output.append("'");
		output.append(" ");
		output.append(this.operation.getSymbol());
		output.append(" ");
		
		output.append(this.rightRelationIndex);
		output.append(".");
		if ((this.right instanceof DbString) || (this.right instanceof DbDateTime)) output.append("'");
		output.append(this.right);
		if ((this.right instanceof DbString) || (this.right instanceof DbDateTime)) output.append("'");
		return output.toString();
	}
	
	@Override
	public Argument copy() {
		return new JoinConditionExpression(this.operation, this.leftRelationIndex, this.left.copy(), 
				this.rightRelationIndex, this.right.copy());
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		return new JoinConditionExpression(this.operation, this.leftRelationIndex, this.left.copy(argumentList), 
				this.rightRelationIndex, this.right.copy(argumentList));
	}
	
}
