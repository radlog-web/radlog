package edu.ucla.cs.wis.bigdatalog.interpreter;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.interpreter.argument.*;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.Expression;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.UnaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Quad;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeCast;
import edu.ucla.cs.wis.bigdatalog.compiler.type.DbConvertible;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNil;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

public class Utilities {
	private static Logger logger = LoggerFactory.getLogger(Utilities.class.getName());
	
	public static boolean isMatch(Argument object1, Argument object2) {
		return object1.equals(object2);
	}
	
	public static String getPostfix(String str, String prefix) {
		if (str.startsWith(prefix))
			return str.substring(prefix.length());

		return null;
	}

	public static void getVariables(OperatorArguments arguments, VariableList variableList) {
		for (int i = 0; i < arguments.size(); i++)
			getVariables(arguments.get(i), variableList);
	}
	
	public static void getVariables(Argument argument, VariableList variableList) {
		if (argument instanceof InputVariable)
			return;
		
		if (argument instanceof Variable) {
			if (!variableList.contains((Variable)argument))
				variableList.add((Variable)argument);		
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
		} else if (argument instanceof Cast) {
			getVariables(((Cast)argument).getValue(), variableList);
		} else if (argument instanceof BinaryExpression) {
			getVariables(((BinaryExpression)argument).getLeft(), variableList);
			getVariables(((BinaryExpression)argument).getRight(), variableList);
		} else if (argument instanceof UnaryExpression) {
			getVariables(((UnaryExpression)argument).getArgument(), variableList);
		}
	}
	
	public static void getVariables(Arguments arguments, Arguments argumentList) {
		for (int i = 0; i < arguments.size(); i++)
			getInputVariables(arguments.get(i), argumentList);
	}
	
	public static void getInputVariables(Argument argument, Arguments argumentList) {
		if (argument instanceof CompilerVariable)
			return;
		
		if (argument instanceof InputVariable) {
			if (!argumentList.contains(argument))
				argumentList.add(argument);
		
		} else if (argument instanceof InterpreterList) {
			getInputVariables(((InterpreterList)argument).getHead(), argumentList);
			if (((InterpreterList)argument).getTail() != null)
				getInputVariables(((InterpreterList)argument).getTail(), argumentList);
			
		} else if (argument instanceof InterpreterFunctor) {
			InterpreterFunctor functor = (InterpreterFunctor)argument; 
			for (int i = 0; i < functor.getArity(); i++) {
				getInputVariables(functor.getArgument(i), argumentList);
			}
		}
	}
	
	public static void getBoundVariables(Arguments arguments, Binding binding, VariableList boundVariableList) {
		for (int i = 0; i < arguments.size(); i++)
			if (binding.getBinding(i) == BindingType.BOUND)
				getVariables(arguments.get(i), boundVariableList);
	}

	public static void getFreeVariables(Arguments arguments, Binding binding, VariableList boundVariableList) {
		for (int i = 0; i < arguments.size(); i++)
			if (binding.getBinding(i) == BindingType.FREE) {
				Argument arg = arguments.get(i);
				getVariables(arg, boundVariableList);
			}
	}
	
	public static void removeBoundVariables(DeALSContext deALSContext, VariableList boundVariableList, VariableList freeVariableList) {
		deALSContext.logTrace(logger, "Entering removeBoundVariables");
		
		if (deALSContext.isDebugEnabled()) {
			deALSContext.logDebug(logger, "  Before free variables are:");
			for (int i = 0; i < freeVariableList.size(); i++)
				deALSContext.logDebug(logger, "    {}", freeVariableList.get(i));
			deALSContext.logDebug(logger, "  Bound variables are:");
			if (boundVariableList != null && boundVariableList.size() > 0) {
				for (int i = 0; i < boundVariableList.size(); i++)
					deALSContext.logDebug(logger, "    {}", boundVariableList.get(i));
			} else {
				deALSContext.logDebug(logger, "    No bound variables");
			}
	    }
	  
		// work backwards through the list - under the covers, it is an arraylist
		int count = freeVariableList.size() - 1;
		for (int i = count; i >= 0; i--) {
			Variable variable = freeVariableList.get(i);
	      
			if (boundVariableList.contains(variable))
				freeVariableList.remove(i);
	    }
	  	
		if (deALSContext.isDebugEnabled()) {	
			deALSContext.logDebug(logger, "  After free variables are:");
			if (freeVariableList.size() > 0) {
				for (int i = 0; i < freeVariableList.size(); i++)
					deALSContext.logDebug(logger, "    {}", freeVariableList.get(i));
			} else {
				deALSContext.logDebug(logger, "    No free variables");
			}
		}
		
		deALSContext.logTrace(logger, "Exiting removeBoundVariables");
	}
	
	public static Pair<VariableList, VariableList> assignIfThenBoundVariableList(PCGOrNode orNode, 
			VariableList boundVariableList, VariableList boundVariableList1, VariableList boundVariableList2) {
		
		boolean isLastIf = false;
		boolean isLastThen = false;
		
		PCGAndNode ifAndNode = (PCGAndNode)orNode.getChild(0);
		PCGOrNode lastIfOrNode = ifAndNode.getLastChild();		
		if (lastIfOrNode.getPredicate() instanceof BuiltInPredicate)
			isLastIf = ((BuiltInPredicate)lastIfOrNode.getPredicate()).isFalse();
				
		PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1);		
		PCGOrNode lastThenOrNode = thenAndNode.getLastChild();
		
		if (lastThenOrNode.getPredicate() instanceof BuiltInPredicate)
			isLastThen = ((BuiltInPredicate)lastThenOrNode.getPredicate()).isFalse();
				
		VariableList ifBoundVariableList;
		VariableList thenBoundVariableList;
	  		
		if (isLastIf) {
			if (isLastThen) {
				ifBoundVariableList = boundVariableList1;
				thenBoundVariableList = boundVariableList2;
			} else {
				ifBoundVariableList = boundVariableList1;
				thenBoundVariableList = boundVariableList;
			}
	    } else {
	    	if (isLastThen) {
	    		ifBoundVariableList = boundVariableList1;
	    		thenBoundVariableList = boundVariableList1;
	    	} else {
	    		ifBoundVariableList = boundVariableList;
	    		thenBoundVariableList = boundVariableList;
	    	}
	    }
		
		return new Pair<>(ifBoundVariableList, thenBoundVariableList);
	}
	
	public static Quad<Boolean, VariableList, VariableList, VariableList> assignIfThenElseBoundVariableList(PCGOrNode orNode, 
			VariableList boundVariableList, VariableList boundVariableList1, VariableList boundVariableList2, 
			VariableList boundVariableList3) {
	
		boolean intersectsList = false;
		PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0);
		PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1);
		PCGAndNode elseAndNode = (PCGAndNode) orNode.getChild(2);
		PCGOrNode lastIfOrNode = ifAndNode.getLastChild();
		PCGOrNode lastThenOrNode = thenAndNode.getLastChild();
		PCGOrNode lastElseOrNode = elseAndNode.getLastChild();
		VariableList ifBoundVariableList;
		VariableList thenBoundVariableList;
		VariableList elseBoundVariableList;
		
		boolean isLastIf = false;
		boolean isLastThen = false;
		boolean isLastElse = false;
		
		if (lastIfOrNode.getPredicate() instanceof BuiltInPredicate)
			isLastIf = ((BuiltInPredicate)lastIfOrNode.getPredicate()).isFalse();

		if (lastThenOrNode.getPredicate() instanceof BuiltInPredicate)
			isLastThen = ((BuiltInPredicate)lastThenOrNode.getPredicate()).isFalse();

		if (lastElseOrNode.getPredicate() instanceof BuiltInPredicate)
			isLastThen = ((BuiltInPredicate)lastElseOrNode.getPredicate()).isFalse();

		if (isLastIf) {
			if (isLastThen) {
				if (isLastElse){
					// no binding out of if-then-else
					ifBoundVariableList = boundVariableList1;
					thenBoundVariableList = boundVariableList2;
					elseBoundVariableList = boundVariableList3;
				} else {
					// binding passed out of if-then-else is ELSE
					ifBoundVariableList = boundVariableList1;
					thenBoundVariableList = boundVariableList2;
					elseBoundVariableList = boundVariableList;
				}
			} else {
				if (isLastElse) {
					// no binding out of if-then-else
					ifBoundVariableList = boundVariableList1;
					thenBoundVariableList = boundVariableList;
					elseBoundVariableList = boundVariableList3;
				} else {
					// binding passed out of if-then-else is THEN union ELSE
					ifBoundVariableList = boundVariableList1;
					thenBoundVariableList = boundVariableList2;
					elseBoundVariableList = boundVariableList3;
					intersectsList = true;
				}
			}
	    } else {
	    	if (isLastThen) {
	    		if (isLastElse) {
	    			// no binding out of if-then-else
	    			ifBoundVariableList = boundVariableList1;
	    			thenBoundVariableList = boundVariableList2;
	    			elseBoundVariableList = boundVariableList3;
	    		} else {
	    			// binding passed out of if-then-else is ELSE
	    			ifBoundVariableList = boundVariableList1;
	    			thenBoundVariableList = boundVariableList2;
	    			elseBoundVariableList = boundVariableList;
	    		}
	    	} else {
	    		if (isLastElse) {
	    			// binding passed out of if-then-else is IF union THEN
	    			ifBoundVariableList = boundVariableList;
	    			thenBoundVariableList = boundVariableList;
	    			elseBoundVariableList = boundVariableList3;
	    		} else {
	    			// binding passed out of if-then-else is IF union THEN intersect ELSE
	    			ifBoundVariableList = boundVariableList2;
	    			thenBoundVariableList = boundVariableList2;
	    			elseBoundVariableList = boundVariableList3;
	    			intersectsList = true;
	    		}
	    	}
	    }
	  
		return new Quad<>(intersectsList, ifBoundVariableList, thenBoundVariableList, elseBoundVariableList);
	}
	
	public static void getIntersectingVariables(VariableList list1, VariableList list2, VariableList list3) {
		for (int i = 0; i < list1.size(); i++) {
			if (list2.contains(list1.get(i)))
				list3.add(list1.get(i));
	    }
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
	
	/*public static InputVariable convertToInputVariable(CompilerInputVariable compilerInputVariable) {
		if (compilerInputVariable.getValue() == null)
			return new InputVariable(compilerInputVariable.getName(), compilerInputVariable.getDataType());
		
		return new InputVariable(compilerInputVariable.getName(), ((DbConvertible)compilerInputVariable.getValue()).toDbType()); 
	}*/
	
	public static Variable toVariable(CompilerVariable compilerVariable, VariableMappings variableMappings) {
		Variable variable = variableMappings.getVariable(compilerVariable);
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
			//arg = convertToInputVariable((CompilerInputVariable)compilerObject);
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
			//arg = DbList.create(); APS 6/17/2015 so the aggregates rules look better when printed ;)
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
		//else
		//	throw new CompilerException("Unknown compiler type. Can not covnert to interpreter type");
		
		return arg;
	}
}
