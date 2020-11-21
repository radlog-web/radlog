package edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class Expression 
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected ArithmeticOperation operation;
	protected DataType dataType;
	protected ArithmeticExpression arithmeticExpression;
	protected boolean reduceInitialized;
	protected int reduceOperation;
	
	protected Variable firstArgVariable;
	protected Variable secondArgVariable;
		
	public Expression(ArithmeticOperation operation, DataType dataType) {
		this.operation = operation;
		this.dataType = dataType;	

		this.arithmeticExpression = new ArithmeticExpression(operation);
	}
	
	public ArithmeticOperation getOperation() { return this.operation; }
	
	public DataType getDataType() { return this.dataType; }

	public boolean isUnary() { return this instanceof UnaryExpression; }
	
	public boolean isBinary() { return this instanceof BinaryExpression; }
	
	abstract void initializeReduce();
}
