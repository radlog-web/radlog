package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.interpreter.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.ComparisonExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;

public class FilterOperator extends Operator implements Serializable {
	private static final long serialVersionUID = 1L;

	private List<ComparisonExpression> expressions;
	
	public FilterOperator(String name) {
		super(name, OperatorType.FILTER, new OperatorArguments());
		this.expressions = new ArrayList<>();
	}
	
	public FilterOperator(String name, List<ComparisonExpression> expressions) {
		this(name);
		for (ComparisonExpression expression : expressions)
			this.expressions.add(expression);
	}

	public int getNumberOfExpressions() { return this.expressions.size(); }
	
	public List<ComparisonExpression> getExpressions() { return this.expressions; }
		
	public void addExpression(String condition, Argument left, Argument right) {
		this.expressions.add(new ComparisonExpression(ComparisonOperation.getOperation(condition), left, right));
	}
	
	public void addExpression(ComparisonExpression expression) {
		this.expressions.add(expression);
	}	
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
	
		for (int i = 0; i < this.expressions.size(); i++) {
			if (i > 0)
				output.append(" && ");
			if (this.expressions.size() > 1) output.append("(");
			output.append(this.expressions.get(i).toString());
			if (this.expressions.size() > 1) output.append(")");
    	}
				
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
