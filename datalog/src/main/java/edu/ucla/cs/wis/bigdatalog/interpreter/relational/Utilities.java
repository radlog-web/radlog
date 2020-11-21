package edu.ucla.cs.wis.bigdatalog.interpreter.relational;

import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeCast;
import edu.ucla.cs.wis.bigdatalog.compiler.type.DbConvertible;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNil;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Cast;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.Expression;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.UnaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AggregateArgument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AliasedArgument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AliasedVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.ComparisonExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.JoinConditionExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.VariableMappings;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.CliqueOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.FilterOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.JoinOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.Operator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.OperatorType;

public class Utilities {

	public static void getVariables(OperatorArguments arguments, VariableList variableList) {
		for (int i = 0; i < arguments.size(); i++)
			getVariables(arguments.get(i), variableList);
	}
	
	public static void getVariables(Argument argument, VariableList variableList) {
		if (argument instanceof InputVariable)
			return;
		
		if (argument instanceof Variable) {
			if (!variableList.contains((Variable)argument))
				variableList.add((Variable) ((Variable)argument).deepDereference());		
		} else if (argument instanceof InterpreterList) {
			getVariables(((InterpreterList)argument).getHead(), variableList);
			if (((InterpreterList)argument).getTail() != null)
				getVariables(((InterpreterList)argument).getTail(), variableList);
		} else if (argument instanceof InterpreterFunctor) {
			InterpreterFunctor functor = (InterpreterFunctor)argument; 
			for (int i = 0; i < functor.getArity(); i++)
				getVariables(functor.getArgument(i), variableList);
		} else if (argument instanceof NodeArguments) {
			for (int i = 0; i < ((NodeArguments)argument).size(); i++)
				getVariables(((NodeArguments)argument).get(i), variableList);
		} else if (argument instanceof AliasedArgument) {
			getVariables(((AliasedArgument)argument).getAlias(), variableList);
			getVariables(((AliasedArgument)argument).getArgument(), variableList);
		} else if (argument instanceof AggregateArgument) {
			getVariables(((AggregateArgument)argument).getTerm(), variableList);
		} else if (argument instanceof ComparisonExpression) {
			getVariables(((ComparisonExpression)argument).getLeft(), variableList);
			getVariables(((ComparisonExpression)argument).getRight(), variableList);
		} else if (argument instanceof BinaryExpression) {
			getVariables(((BinaryExpression)argument).getLeft(), variableList);
			getVariables(((BinaryExpression)argument).getRight(), variableList);
		} else if (argument instanceof UnaryExpression) {
			getVariables(((UnaryExpression)argument).getArgument(), variableList);
		}
	}
	
	/*public static Argument convertToArgument(CompilerTypeBase compilerObject, VariableMappings variableMappings) {
		Argument arg = edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.convertToArgument(compilerObject, variableMappings);
		/*if ((arg instanceof Variable) && compilerObject.isVariable() 
				&& !((CompilerVariable)compilerObject).isAnonymous() 
				&& ((CompilerVariable)compilerObject).hasValueAssigned())
			((Variable)arg).setValue(edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.convertToArgument(((CompilerVariable)compilerObject).deepDereference(), variableMappings));
		else *if (arg instanceof BinaryExpression) { 
			BinaryExpression expr = (BinaryExpression)arg;
			arg = new BinaryExpression(expr.getOperation(), expr.getLeft(), expr.getRight(), expr.getDataType()); 
		} else if (arg instanceof UnaryExpression) {
			UnaryExpression expr = (UnaryExpression)arg;
			arg = new UnaryExpression(expr.getOperation(), expr.getArgument(), expr.getDataType()); 
		}
		
		return arg;
	}*/
	
	/*public static Pair<Boolean, char[]> getBoundArgumentAnalysis(CompilerTypeList arguments) {
		char[] pattern = new char[arguments.size()];
		int boundCounter = 0;
		for (int i = 0; i < arguments.size(); i++) {
			if (arguments.get(i).isConstant()) { 
				pattern[i] = 'c';
				boundCounter++;
			} else if (arguments.get(i).isVariable()) {
				CompilerVariable var = (CompilerVariable)arguments.get(i);
				if (var.deepDereference().isConstant()) {
					pattern[i] = 'v';
					boundCounter++;
				}
			}
		}
		return new Pair<>(boundCounter > 0, pattern);
	}*/
	
	public static void getArguments(Operator operator, List<Argument> argumentList, boolean getConstants) {
		switch (operator.getOperatorType()) {
			case JOIN:
			case NEGATION:
				for (JoinConditionExpression condition : ((JoinOperator)operator).getConditions()) {
					getArguments(condition.getLeft(), argumentList, getConstants);
					getArguments(condition.getRight(), argumentList, getConstants);
				}
				break;
			case FILTER:
				for (ComparisonExpression expression : ((FilterOperator)operator).getExpressions())
					getArguments(expression, argumentList, getConstants);
				
				break;
			case RECURSIVE_CLIQUE:
			case MUTUAL_RECURSIVE_CLIQUE:
				if (((CliqueOperator)operator).getExitRulesOperator() != null)
					getArguments(((CliqueOperator)operator).getExitRulesOperator(), argumentList, getConstants);
				getArguments(((CliqueOperator)operator).getRecursiveRulesOperator(), argumentList, getConstants);
				break;

			default:
				for (Argument arg : operator.getArguments())
					getArguments(arg, argumentList, getConstants);
		}
	}
	
	public static void getArguments(Argument argument, List<Argument> argumentList, boolean getConstants) {
		if (argumentList.contains(argument))
			return;
		
		if (argument instanceof ComparisonExpression) {
			getArguments(((ComparisonExpression)argument).getLeft(), argumentList, getConstants);
			if (((ComparisonExpression)argument).getRight() != null)
				getArguments(((ComparisonExpression)argument).getRight(), argumentList, getConstants);
		} else if (argument instanceof AliasedVariable) {
			getArguments(((AliasedVariable)argument).getVariable(), argumentList, getConstants);
		} else if (argument instanceof AliasedArgument) {
			getArguments(((AliasedArgument)argument).getArgument(), argumentList, getConstants);
			getArguments(((AliasedArgument)argument).getAlias(), argumentList, getConstants);
		} else if (argument instanceof AggregateArgument) {
			getArguments(((AggregateArgument)argument).getTerm(), argumentList, getConstants);
		} else if (argument instanceof BinaryExpression) {
			getArguments(((BinaryExpression)argument).getLeft(), argumentList, getConstants);
			getArguments(((BinaryExpression)argument).getRight(), argumentList, getConstants);
		} else if (argument instanceof UnaryExpression) {
			getArguments(((UnaryExpression)argument).getArgument(), argumentList, getConstants);
		}
		
		if (getConstants) {
			if ((argument instanceof Variable) && (((Variable)argument).deepDereference() instanceof DbTypeBase)) {
				argumentList.add(((Variable)argument).deepDereference());
				return;
			}
		}
		
		argumentList.add(argument);
	}
	
	public static Argument convertToConstant(CompilerTypeBase compilerObject, TypeManager typeManager) {
		if (compilerObject.isVariable() && ((CompilerVariable)compilerObject).hasValueAssigned())
			return ((DbConvertible)((CompilerVariable)compilerObject).deepDereference()).toDbType(typeManager);
		
		if (compilerObject.isInputVariable() && (((CompilerInputVariable)compilerObject).getValue() != null))
			return ((DbConvertible)((CompilerInputVariable)compilerObject).getValue()).toDbType(typeManager);
		
		if (compilerObject.isConstant())
			return ((DbConvertible)compilerObject).toDbType(typeManager);
		
		if (compilerObject.isNil())
			return DbList.create();
		
		if (compilerObject.isCast()) {
			CompilerTypeCast cast = (CompilerTypeCast)compilerObject;
			if (cast.getValue().isVariable())
				return convertToConstant(cast.getValue(), typeManager);
		}
		
		return null;
	}	
	
	public static boolean isExactMatch(OperatorArguments arguments1, Operator operator2) {
		return isExactMatch(arguments1, operator2, false);
	}
	
	public static boolean isExactMatch(OperatorArguments arguments1, Operator operator2, boolean matchOrder) {
		if (operator2.getOperatorType() == OperatorType.FILTER)
			operator2 = operator2.getChild(0);
		
		OperatorArguments arguments2 = operator2.getArguments();
		if (arguments1.size() != arguments2.size())
			return false;
		
		for (Argument arg : arguments1)
			if (!arguments2.contains(arg))
				return false;
		
		for (Argument arg : arguments2)
			if (!arguments1.contains(arg))
				return false;
		
		if (matchOrder) {
			for (int i = 0; i < arguments1.size(); i++) {
				if (arguments1.get(i) != operator2.getArgument(i))
					return false;
			}
		}
		
		return true;
	}
	
	public static InterpreterFunctor convertToInterpreterFunctor(CompilerFunctor compilerFunctor, VariableMappings variableMappings, TypeManager typeManager) {
		NodeArguments arguments = new NodeArguments(compilerFunctor.getArity());
		for (int i = 0; i < compilerFunctor.getArity(); i++)
			arguments.set(i, Utilities.convertToArgument(compilerFunctor.getArgument(i), variableMappings, typeManager));

		return new InterpreterFunctor(compilerFunctor.getFunctorName(), arguments);
	}
	
	public static InterpreterList convertToInterpreterList(CompilerList compilerList, VariableMappings variableMappings, TypeManager typeManager) {
		if (compilerList.isEmpty())
			return new InterpreterList(null);

		Argument newHead = Utilities.convertToArgument(compilerList.getHead(), variableMappings, typeManager);
		InterpreterList newTail = null;
		if (compilerList.getTail() != null)
			newTail = Utilities.convertToInterpreterList(compilerList.getTail(), variableMappings, typeManager);
		
		return new InterpreterList(newHead, newTail);
	}
	
	public static Variable toVariable(CompilerVariable compilerVariable, VariableMappings variableMappings) {
		Variable variable = variableMappings.get(compilerVariable);
		if (variable == null) {
			variable = new Variable(compilerVariable.getVariableName(), compilerVariable.getDataType());
			variableMappings.put(compilerVariable, variable);
		}
		return variable;
	}
	
	public static Expression convertToExpression(CompilerArithmeticExpression compilerArithmeticExpression, VariableMappings variableMappings, 
			TypeManager typeManager) {
		if (compilerArithmeticExpression.isBinary()) {
			return new BinaryExpression(ArithmeticOperation.getOperation(compilerArithmeticExpression.getOperation().toString()), 
					Utilities.convertToArgument(compilerArithmeticExpression.getArgument1(), variableMappings, typeManager),
					Utilities.convertToArgument(compilerArithmeticExpression.getArgument2(), variableMappings, typeManager), 
					compilerArithmeticExpression.getDataType());
		}

		return new UnaryExpression(ArithmeticOperation.getOperation(compilerArithmeticExpression.getOperation().toString()), 
				Utilities.convertToArgument(compilerArithmeticExpression.getArgument1(), variableMappings, typeManager), 
				compilerArithmeticExpression.getDataType());
	}
	
	public static Argument convertToArgument(CompilerTypeBase compilerObject, VariableMappings variableMappings, TypeManager typeManager) {
		Argument arg = null;
		if (compilerObject.isVariable())
			arg = toVariable((CompilerVariable)compilerObject, variableMappings);
		else if (compilerObject.isInputVariable()) {
			CompilerInputVariable civ = (CompilerInputVariable)compilerObject;
			if (civ.getValue() == null)
				arg = new InputVariable(civ.getName(), civ.getDataType());
			else
				arg = new InputVariable(civ.getName(), ((DbConvertible)civ.getValue()).toDbType(typeManager));
		} else if (compilerObject.isConstant())
			arg = ((DbConvertible)compilerObject).toDbType(typeManager);
		else if (compilerObject.isFunctor())
			arg = convertToInterpreterFunctor((CompilerFunctor)compilerObject, variableMappings, typeManager);
		else if (compilerObject.isList())
			arg = convertToInterpreterList((CompilerList)compilerObject, variableMappings, typeManager);
		else if (compilerObject.isNil())
			arg = new DbNil();
		else if (compilerObject.isCast()) {
			CompilerTypeCast cast = (CompilerTypeCast)compilerObject;
			if (cast.getValue().isVariable()) {
				arg = toVariable((CompilerVariable)cast.getValue(), variableMappings);
				arg = new Cast(arg, cast.getDataType());
			} else { 
				arg = convertToExpression((CompilerArithmeticExpression)cast.getValue(), variableMappings, typeManager);
			}
		} else if (compilerObject.getType() == CompilerType.ARITHMETIC_EXPRESSION) {
			arg = convertToExpression((CompilerArithmeticExpression) compilerObject, variableMappings, typeManager);
		}
		
		return arg;
	}
}
