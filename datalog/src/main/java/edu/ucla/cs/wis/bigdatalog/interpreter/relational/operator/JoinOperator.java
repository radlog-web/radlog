package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.interpreter.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.JoinConditionExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;

public class JoinOperator extends Operator implements Serializable {
	private static final long serialVersionUID = 1L;

	private List<JoinConditionExpression> conditions;
	
	protected JoinOperator(String name, OperatorType operatorType) {
		super(name, operatorType, new OperatorArguments());
		this.conditions = new ArrayList<>();
	}
	
	public JoinOperator(String name) {
		super(name, OperatorType.JOIN, new OperatorArguments());
		this.conditions = new ArrayList<>();
	}

	public List<JoinConditionExpression> getConditions() { return this.conditions; }
	
	public int getNumberOfConditions() { return this.conditions.size(); }
		
	public void addCondition(ComparisonOperation comparisonOperation, int leftPredicateIndex, 
			Argument leftPredicateColumn, int rightPredicateIndex, Argument rightPredicateColumn) {
		this.conditions.add(new JoinConditionExpression(comparisonOperation, leftPredicateIndex, 
				leftPredicateColumn, rightPredicateIndex, rightPredicateColumn));
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		//output.append(this.name);
    	output.append("(");	    	

		for (int i = 0; i < this.conditions.size(); i++) {
			if (i > 0)
				output.append(", ");
			
			output.append(this.conditions.get(i).toString());
    	}
    	output.append(")");
		
	    output.append(" <");
		output.append(this.operatorType.name());
	    //output.append(this.getClass().getSimpleName());
	    output.append(">");
	    //output.append("[");
	    //output.append(this.hashCode());
	    //output.append("]");
		
		return output.toString();
	}
}
