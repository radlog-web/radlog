package edu.ucla.cs.wis.bigdatalog.compiler.type;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.Aggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGNode;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class CompilerTypeBase implements Serializable {
	private static final long serialVersionUID = 1L;

	public static final String NIL = "nil";

	protected transient CompilerType type;

	public CompilerTypeBase(CompilerType type) {
		this.type = type;
	}

	public CompilerType getType() {return this.type;}

	public boolean isVariable() {return (this.type == CompilerType.VARIABLE);}

	public boolean isInputVariable() {return (this.type == CompilerType.INPUT_VARIABLE);}

	public boolean isFunctor() {return (this.type == CompilerType.COMPILER_FUNCTOR);}

	public boolean isList() {return (this.type == CompilerType.COMPILER_LIST);}

	public boolean isNil() {return (this.type == CompilerType.COMPILER_NIL);}

	public boolean isCast() {return (this.type == CompilerType.CAST); }
	
	public boolean isAnyAggregate() {
		switch (this.type) {
			case AGGREGATE:
			case USER_DEFINED_AGGREGATE:
			case BUILT_IN_AGGREGATE:
			case FS_AGGREGATE:
				return true;
		}
		return false;/*
		return (isType(CompilerType.AGGREGATE) || isType(CompilerType.USER_DEFINED_AGGREGATE) 
				|| isType(CompilerType.BUILT_IN_AGGREGATE) || isType(CompilerType.FS_AGGREGATE));*/
	}

	public boolean isBuiltInAggregate() {return (this.type == CompilerType.BUILT_IN_AGGREGATE);}
	
	public boolean isUserDefinedAggregate() {return (this.type == CompilerType.USER_DEFINED_AGGREGATE);}

	public boolean isFSAggregate() {return (this.type == CompilerType.FS_AGGREGATE);}

	@SuppressWarnings("static-method")
	public boolean isConstant() { return false;} 

	public boolean isNumeric() {
		switch (this.type) {
			case COMPILER_INT:
			case COMPILER_FLOAT:
			case COMPILER_LONG:
			case COMPILER_LONGLONG:
			case COMPILER_LONGLONGLONGLONG:
			case COMPILER_BYTE:
			case COMPILER_SHORT:
			case COMPILER_DOUBLE:
				return true;
		}
		return false;
		//return (isInteger() || isFloat() || isLong() || isLongLong() || isLongLongLongLong()); 
	}
	
	public boolean isExpression() { return this.type == CompilerType.ARITHMETIC_EXPRESSION; } 

	@SuppressWarnings("static-method")
	public String toStringIndent(int level) {
		StringBuilder retval = new StringBuilder();
		retval.append("\n");
		while (level > 0) {
			retval.append(" ");
			level--;
		}
		return retval.toString();
	}

	public boolean isBound() {
		boolean status;

		switch (type)
		{
		case COMPILER_STRING:
			status = !((CompilerString)this).getText().equals(NIL);
			break;
		case COMPILER_INT:
		case COMPILER_FLOAT:
		case COMPILER_LONG:
		case COMPILER_LONGLONG:
		case COMPILER_LONGLONGLONGLONG:
		case COMPILER_DATETIME:
		case COMPILER_BYTE:
		case COMPILER_SHORT:
		case COMPILER_DOUBLE:
		case INPUT_VARIABLE:
			status = true;
			break;
		case VARIABLE:
		{
			status = false;
			CompilerVariable variable = (CompilerVariable)this;
			if (variable.isBound())
				status = variable.deepDereference().isBound();
		}
		break;
		case COMPILER_FUNCTOR:
		{
			status = ((CompilerFunctor)this).getArguments().isBound();
		}
		break;
		case COMPILER_LIST:
		{
			CompilerList list = (CompilerList)this;
			if (!list.isEmpty()) {
				if (list.getHead().isBound()) {
					if (list.getTail() != null)
						status = list.getTail().isBound();
					else
						status = true;
				} else {
					status = false;
				}
			} else {
				status = true;
			}
		}
		break;
		case COMPILER_TYPE_LIST:
		{
			status = true;
			for (CompilerTypeBase compilerTypeObject : ((CompilerTypeList) this)) {
				status = compilerTypeObject.isBound();
				if (!status)
					break;
			}
		}
		break;
		case CAST: {
			status = ((CompilerTypeCast)this).getValue().isBound();
		}
		break;
		case ARITHMETIC_EXPRESSION: {
			status = ((CompilerArithmeticExpression)this).getArgument1().isBound();
			if (((CompilerArithmeticExpression)this).isBinary()) 
				status = status && ((CompilerArithmeticExpression)this).getArgument2().isBound();
		}
		break;
		default:
			status = false;
			break;
		}

		return status;
	}

	public boolean isGround() {
		boolean status = true;

		switch (type)
		{
		case COMPILER_STRING:
		case COMPILER_INT:
		case COMPILER_LONG:
		case COMPILER_LONGLONG:
		case COMPILER_LONGLONGLONGLONG:
		case COMPILER_FLOAT:
		case COMPILER_DATETIME:
		case COMPILER_BYTE:
		case COMPILER_SHORT:
		case COMPILER_DOUBLE:
		case INPUT_VARIABLE:
			break;

		case VARIABLE:
			status = false;
			break;

		case COMPILER_FUNCTOR:
		{
			status = ((CompilerFunctor)this).getArguments().isGround();
		}
		break;

		case COMPILER_LIST:
		{
			CompilerList list = (CompilerList) this;
			if (!list.isEmpty()) {
				if (!list.getHead().isGround()) {
					status = false;
				} else {
					if (list.getTail() != null)
						if(!list.getTail().isGround())
							status = false;
				}
			}
		}
		break;

		case AGGREGATE:
		case BUILT_IN_AGGREGATE:
		case FS_AGGREGATE:
		case USER_DEFINED_AGGREGATE:
			status = false;
			break;

		case PREDICATE:
		{
			for (CompilerTypeBase argument : ((Predicate)this).getArguments()) {
				if (!argument.isGround()) {
					status = false;
					break;
				}
			}
		}
		break;

		case COMPILER_TYPE_LIST:
		{
			for (CompilerTypeBase compilerTypeObject : ((CompilerTypeList)this)) {
				if (!compilerTypeObject.isGround()) {
					status = false;
					break;
				}	     				
			}
		}
		break;
		case CAST: {
			status = ((CompilerTypeCast)this).getValue().isGround();
		}
			break;
		
		case ARITHMETIC_EXPRESSION: {
			status = ((CompilerArithmeticExpression)this).getArgument1().isGround();
			if (((CompilerArithmeticExpression)this).isBinary())
				status = status && ((CompilerArithmeticExpression)this).getArgument2().isGround();			
		}
			break;
		default:
			status = false;
			break;
		}

		return status;
	}

	public boolean containsBuiltInAggregate() {
		return this.containsTermType(CompilerType.BUILT_IN_AGGREGATE);
	}

	public boolean containsFSAggregate() {
		return this.containsTermType(CompilerType.FS_AGGREGATE);
	}

	public boolean containsUserDefinedAggregate() {
		return this.containsTermType(CompilerType.USER_DEFINED_AGGREGATE);
	}

	public boolean containsAnyAggregate() {
		if (this.containsTermType(CompilerType.AGGREGATE)) return true;
		if (this.containsTermType(CompilerType.BUILT_IN_AGGREGATE)) return true;
		if (this.containsTermType(CompilerType.FS_AGGREGATE)) return true;
		if (this.containsTermType(CompilerType.USER_DEFINED_AGGREGATE)) return true;		
		return false;
	}

	public boolean containsTermType(CompilerType compilerType) {
		boolean status = false;

		if (compilerType == this.type) {
			status = true;
		} else {
			switch (this.type) {
			case VARIABLE:
			{
				CompilerVariable variable = (CompilerVariable)this;
				if (variable.isBound()) {
					CompilerTypeBase arg = variable.deepDereference();
					if (arg instanceof CompilerVariable) {
						status = ((CompilerVariable)arg).containsTermType(compilerType);
					} else if (arg instanceof CompilerInputVariable) {
						status = ((CompilerInputVariable)arg).containsTermType(compilerType);
					} else {
						status = (arg.getType() == compilerType);
					}
				}
			}
			break;
			case AGGREGATE:
			case BUILT_IN_AGGREGATE:
			case FS_AGGREGATE:
			case USER_DEFINED_AGGREGATE:
			{
				CompilerTypeBase aggregateTerm;	
				if ((aggregateTerm = ((Aggregate) this).getAggregateTerm()) != null)
					status = aggregateTerm.containsTermType(compilerType);
			}
			break;

			case COMPILER_FUNCTOR:
			{
				status = ((CompilerFunctor)this).getArguments().containsTermType(compilerType);
			}
			break;

			case COMPILER_LIST:
			{
				CompilerList list = (CompilerList) this;			    
				if (!list.isEmpty()) {
					if (list.getHead().containsTermType(compilerType)) {
						status = true;
					} else {
						if ((list.getTail() != null) 
								&& (list.getTail().containsTermType(compilerType)))
							status = true;
					}
				}
			}
			break;
			
			case BUILT_IN_PREDICATE:
			case PREDICATE:
			{
				status = ((Predicate)this).getArguments().containsTermType(compilerType);
			}
			break;
			
			case PCG_NODE:
			case PCG_AND_NODE:
			{
				status = ((PCGNode<?>)this).getArguments().containsTermType(compilerType);
			}
				break;

			case RULE:
				status = ((Rule)this).getHead().containsTermType(compilerType);
				if (!status) {
					for (Predicate goal : ((Rule)this).getBody()) {
						status = goal.containsTermType(compilerType);
						if (status)
							break;						
					}
				}
				break;
			case COMPILER_TYPE_LIST:
			{			  		
				for (CompilerTypeBase compilerTypeObject : ((CompilerTypeList)this)) {
					if (compilerTypeObject.containsTermType(compilerType)) {
						status = true;
						break;
					}
				}
			}
			break;
			
			case CAST: {
				status = ((CompilerTypeCast)this).getValue().containsTermType(compilerType);
			}
				break;
				
			case ARITHMETIC_EXPRESSION: {
				status = ((CompilerArithmeticExpression)this).getArgument1().containsTermType(compilerType);
				if (((CompilerArithmeticExpression)this).isBinary())
					status = status && ((CompilerArithmeticExpression)this).getArgument2().containsTermType(compilerType);
			}
				break;

			case COMPILER_STRING:
			case COMPILER_INT:
			case COMPILER_LONG:
			case COMPILER_LONGLONG:
			case COMPILER_LONGLONGLONGLONG:
			case COMPILER_FLOAT:
			case COMPILER_DATETIME:
			case COMPILER_BYTE:
			case COMPILER_SHORT:
			case COMPILER_DOUBLE:
			case INPUT_VARIABLE:
			default:
				break;
			}
		}

		return status;
	}

	abstract public CompilerTypeBase copy();

	abstract public CompilerTypeBase copy(CompilerVariableList variableList);

	abstract public boolean equals(CompilerTypeBase other);

	abstract public String toString();
	
	public DataType getDataType() {
		return DataType.UNKNOWN;
	}
}
