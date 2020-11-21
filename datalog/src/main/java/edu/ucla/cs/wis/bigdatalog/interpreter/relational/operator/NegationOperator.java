package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import java.io.Serializable;

public class NegationOperator extends JoinOperator implements Serializable {
	private static final long serialVersionUID = 1L;

	public NegationOperator(String name) {
		super(name, OperatorType.NEGATION);
	}

}
