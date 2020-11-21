package edu.ucla.cs.wis.bigdatalog.interpreter;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class ArithmeticExpression implements Serializable { 
	private static final long serialVersionUID = 1L;
	
	protected ArithmeticOperation operation;

	public ArithmeticExpression(ArithmeticOperation operation) {
		this.operation = operation;
	}
	
	public DbNumericType evaluate(DbNumericType left, DbNumericType right) {
		/*DataType leftDataType = left.getDataType();
		DataType rightDataType = right.getDataType();
		int order1 = leftDataType.getOrder();
		int order2 = rightDataType.getOrder();
		
		if (order2 > order1)
			left = left.convertTo(rightDataType);
		else if (order1 > order2)
			right = right.convertTo(leftDataType);*/

		switch (this.operation) {
			case ADDITION:
				return left.add(right);
			case SUBTRACTION:
				return left.subtract(right);
			case MULTIPLICATION:
				return left.multiply(right);
			case DIVISION:
				return left.divide(right);
			case INTEGER_DIVISION:
				int dividend = ((DbInteger)DataType.cast(left, DataType.INT)).getValue();
				int divisor = ((DbInteger)DataType.cast(right, DataType.INT)).getValue();
				if (divisor == 0)
					throw new InterpreterException("Divide by zero error in integer division (DIV).");
				return DbInteger.create(dividend / divisor); 
			case MOD:
				return ((DbInteger) left).modulo(right);
			case OPC:
				return ((DbInteger) left).opcom(right);
		}
		return null;
	}
	
	public DbTypeBase evaluate(DbNumericType arg) {
		if (this.operation == ArithmeticOperation.LOG)
			return arg.logarithm();
		else if (this.operation == ArithmeticOperation.EXP)
		    return arg.exponential();
        else if (this.operation == ArithmeticOperation.STEP)
            return arg.step();
		
		return null;
	}
	
	public static ArithmeticExpression getArithmeticExpression(String operationName) {
		ArithmeticOperation operation = ArithmeticOperation.getOperation(operationName);
		if (operation == ArithmeticOperation.NONE)
			return null;
		
		return new ArithmeticExpression(operation);
	}
}
