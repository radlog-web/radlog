package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AliasedArgument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;

public class ProjectionOperator extends Operator implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected boolean isDistinct;

	public ProjectionOperator(String name, OperatorArguments arguments) {
		super(name, OperatorType.PROJECT);
		this.arguments = arguments;
		this.isDistinct = false;
	}

	public void setDistinct(boolean isDistinct) { this.isDistinct = isDistinct; }
	
	public boolean isDistinct() { return this.isDistinct; }
	
	public void addArgument(Argument argument, Argument alias) {
		this.arguments.add(new AliasedArgument(argument, alias));
	}
	
	public void removeArgument(Argument argument) {
		for (int i = this.getArity() - 1; i >= 0; i--)
			if (this.getArguments().get(i) == argument)
				this.getArguments().remove(i);
	}
	
	public void setArguments(OperatorArguments arguments) {
		this.arguments = arguments;
	}
	
	public Operator getChild() {
		return this.children[0];
	}
	
	public void setChild(Operator operator) {
		this.children[0] = operator;
	}
			
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
	   	//output.append(this.name);
	   	output.append("(");
	   	output.append(this.arguments.toString());
	   	output.append(")");
	
	    output.append(" <");
	    if (this.isDistinct)
	    	output.append("DISTINCT ");
	    
	    output.append(this.operatorType.name());	    
	    output.append(">");
	    //output.append("[");
	    //output.append(this.hashCode());
	    //output.append("]");
		
		return output.toString();
	}

	public static ProjectionOperator createWithTrueArgument(String predicateName, DbString tru) {
		OperatorArguments args = new OperatorArguments();
		args.add(tru);
		return new ProjectionOperator(predicateName, args);
	}
}
