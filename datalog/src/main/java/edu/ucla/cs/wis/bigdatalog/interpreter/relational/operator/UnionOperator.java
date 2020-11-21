package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import java.io.Serializable;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;

public class UnionOperator extends Operator implements Serializable {
	private static final long serialVersionUID = 1L;

	public UnionOperator(String name, OperatorArguments arguments) {
		super(name, OperatorType.UNION, arguments);
	}
	
	public int getArity() { return this.arguments.size(); }
	
	public OperatorArguments getArguments() { return this.arguments; }
}
