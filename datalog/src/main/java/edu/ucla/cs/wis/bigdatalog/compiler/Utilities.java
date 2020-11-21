package edu.ucla.cs.wis.bigdatalog.compiler;

import java.util.List;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Quad;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.Aggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.UserDefinedAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenElsePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeCast;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class Utilities {

	public static CompilerTypeList getBoundArguments(CompilerTypeList arguments, Binding binding) {
		CompilerTypeList boundArguments = new CompilerTypeList();

		for (int i = 0; i < arguments.size(); i++)
			if (binding.getBinding(i) == BindingType.BOUND)
				boundArguments.add(arguments.get(i));
	  
		return boundArguments;
	}

	public static CompilerTypeList getFreeArguments(CompilerTypeList arguments, Binding binding) {
		CompilerTypeList freeArguments = new CompilerTypeList();

		for (int i = 0; i < arguments.size(); i++)
			if (binding.getBinding(i) == BindingType.FREE)
				freeArguments.add(arguments.get(i));
	  
		return freeArguments;
	}

	public static boolean isMatch(CompilerTypeBase object1, CompilerTypeBase object2) {
		boolean status = false;
		
		if (object1.getType() == object2.getType()) {
			switch (object1.getType())
			{
				case VARIABLE:
				case INPUT_VARIABLE:
				{
					if (object1.equals(object2))
						status = true;
				}
					break;

				case COMPILER_STRING:
				case COMPILER_INT:
				case COMPILER_LONG:
				case COMPILER_LONGLONG:
				case COMPILER_LONGLONGLONGLONG:
				case COMPILER_DATETIME:
				case COMPILER_FLOAT:
				case COMPILER_BYTE:
				case COMPILER_SHORT:
				case COMPILER_DOUBLE:
				{
					if (object1.equals(object2))
						status = true;
				}
					break;
		  
				case COMPILER_FUNCTOR:
				{
					CompilerFunctor functor1 = (CompilerFunctor) object1;
					CompilerFunctor functor2 = (CompilerFunctor) object2;
		    
					if (functor1.getFunctorName().equals(functor2.getFunctorName()) 
							&& (functor1.getArity() == functor2.getArity()))
						status = isMatch(functor1.getArguments(), functor2.getArguments());
				}
					break;
		  
				case COMPILER_LIST:
				{
					CompilerList list1 = (CompilerList) object1;
					CompilerList list2 = (CompilerList) object2;
		    
					if (list1.isEmpty() && list2.isEmpty()) {
						status = true;
					} else {
						if (!list1.isEmpty() && !list2.isEmpty()) {
							if (isMatch(list1.getHead(), list2.getHead())) {
								if ((list1.getTail() != null) && list2.getTail() != null)
									status = isMatch(list1.getTail(), list2.getTail());
								else
									status = true;
							}
						}
					}
				}
					break;
		  
				case COMPILER_TYPE_LIST:
				{
					CompilerTypeList objectList1 = (CompilerTypeList) object1;
					CompilerTypeList objectList2 = (CompilerTypeList) object2;
		    
					if (objectList1.size() == objectList2.size()) {
						int position = 0;
						status = true;
						for (CompilerTypeBase objectListItem : objectList1) {
							if (!isMatch(objectListItem, objectList2.get(position))) {
								status = false;
								break;
							}
							position++;
						}
					}
				}
					break;
		  
				case CAST:
				{
					CompilerTypeCast cast1 = (CompilerTypeCast)object1;
					CompilerTypeCast cast2 = (CompilerTypeCast)object2;
					status = isMatch(cast1.getValue(), cast2.getValue());
				}
					break;
				
				case ARITHMETIC_EXPRESSION:
				{
					CompilerArithmeticExpression cae1 = (CompilerArithmeticExpression)object1;
					CompilerArithmeticExpression cae2 = (CompilerArithmeticExpression)object2;
					status = ((cae1.getOperation() == cae2.getOperation())
							&& isMatch(cae1.getArgument1(), cae2.getArgument1()));

					if (cae1.isBinary())
						status = status && isMatch(cae1.getArgument2(), cae2.getArgument2());					
				}
				
					break;
				default:
					break;
			}
	    }
	  
		return status;
	}

	public static boolean hasSameBoundArguments(PCGAndNode andNode, PCGOrNode orNode) {
		boolean status = false;
	  
		if (andNode.getPredicateName().equals(orNode.getPredicateName()) 
				&& (andNode.getArity() == orNode.getArity()) 
				&& andNode.getBindingPattern().equals(orNode.getBindingPattern())) {
			status = true;
	      
			for (int i = 0; i < andNode.getArity(); i++) {
				if (andNode.getBinding(i) == BindingType.BOUND 
						&& !isMatch(andNode.getArgument(i), orNode.getArgument(i))) {
					status = false;
					break;
				}
			}
		}
	  
		return status;
	}

	public static boolean hasSameFreeArguments(PCGAndNode andNode, PCGOrNode orNode) {
		boolean status = false;
	  
		if (andNode.getPredicateName().equals(orNode.getPredicateName()) 
				&& (andNode.getArity() == orNode.getArity()) 
				&& andNode.getBindingPattern().equals(orNode.getBindingPattern())) {
			status = true;
	      
			for (int i = 0; i < andNode.getArity(); i++) {
				if (andNode.getBinding(i) == BindingType.FREE 
					&& !isMatch(andNode.getArgument(i), orNode.getArgument(i))) {
					status = false;
					break;
				}
			}
		}
	
		return status;
	}

	/*************************************************************
	* check if a list of variables are all bound given a list of bound variables
	*
	* - A variable is bound iff it is in the bound variable list or
	*   it is currently bound to some constant or
	*   the value that it is pointing to is the same as that pointed to by any
	*   of the bound variables
	* - The third case essentially deals with the case of
	*      h <- p(X,X).
	*      p(A,B) <- A = B.
	*   such that the equality ought to be bound-bound
	**************************************************************/
	public static boolean allVariablesBound(CompilerVariableList allVariableList, CompilerVariableList boundVariableList) {
		boolean status = true;
	  
		CompilerVariable variable;
		CompilerVariable boundVariable;
		for (int i = 0; i < allVariableList.size(); i++) {
			variable = allVariableList.get(i);	
			status = false;
			for (int j = 0; j < boundVariableList.size(); j++) {
				boundVariable = boundVariableList.get(j);
				if (variable.equals(boundVariable) 
						|| variable.hasValueAssigned() 
						|| (variable.isBound() && boundVariable.isBound() 
								&& variable.getValue().equals(boundVariable.getValue()))) {
					status = true;
					break;
				}
			}
			if (!status)
				break;
	    }
	  
		return status;
	}
		
	public static boolean isSubset(CompilerVariableList subset, CompilerVariableList set) {
		for (int i = 0; i < subset.size(); i++) {
			if (!set.contains(subset.get(i)))
				return false;
		}		
	  
		return true;
	}

	public static boolean intersects(CompilerTypeList set1, CompilerTypeList set2) {
		boolean status = true;
	  
		for (int i = 0; i < set1.size(); i++) {
			if (!set2.contains(set1.get(i))) {
				status = false;
				break;
			}
		}
	  
		return status;
	}
	
	public static boolean intersects(CompilerVariableList set1, CompilerVariableList set2) {
		for (int i = 0; i < set1.size(); i++) {
			if (!set2.contains(set1.get(i)))
				return false;
		}
		return true;
	}

	public static void mergeVariableLists(CompilerVariableList list1, CompilerVariableList list2) {
		for (int i = 0; i < list2.size(); i++) {
			if (!list1.contains(list2.get(i)))
				list1.add(list2.get(i));
	    }
	}
	
	public static void getVariables(List<?> compilerTypeObjects, CompilerVariableList variableList) {
		for (Object compilerTypeObject : compilerTypeObjects)
			getVariables((CompilerTypeBase)compilerTypeObject, variableList);
	}
		
	public static void getVariables(CompilerTypeBase object, CompilerVariableList variableList) {	
		switch (object.getType()) {
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
	  		{
	  			if (!variableList.contains((CompilerVariable)object))
	  				variableList.add((CompilerVariable)object);
	  		}
	  			break;
	      
	  		case COMPILER_FUNCTOR:
	  		{
	  			CompilerFunctor functor = (CompilerFunctor)object;  			
	  			getVariables(functor.getArguments(), variableList);
	  		}
	  			break;
	      
	  		case AGGREGATE:
	  		case BUILT_IN_AGGREGATE:
	  		case FS_AGGREGATE:
	  		case USER_DEFINED_AGGREGATE:
	  		{
	  			CompilerTypeBase aggregateTerm = ((Aggregate)object).getAggregateTerm(); 
	  			if (aggregateTerm != null)
	  				getVariables(aggregateTerm, variableList);
	  			
	  		}
	  			break;
	      
	  		case COMPILER_LIST:
	  		{
	  			CompilerList list = (CompilerList)object;
		
	  			if (!list.isEmpty()) {
	  				getVariables(list.getHead(), variableList);
	  				if (list.getTail() != null)
	  					getVariables(list.getTail(), variableList);
	  			}
	  		}
	  			break;
	      
	  		case PREDICATE:
	  		{
	  			Predicate predicate = (Predicate)object;
	  			getVariables(predicate.getArguments(), variableList);
	  		}
	  			break;
	      
	  		case IFTHENELSE_PREDICATE:
	  		{
	  			IfThenElsePredicate predicate = (IfThenElsePredicate)object;
		
	  			getVariables(predicate.getIfLiterals(), variableList);	  			
	  			getVariables(predicate.getThenLiterals(), variableList);  			
	  			getVariables(predicate.getElseLiterals(), variableList);
	  		}
	  			break;
	      
	  		case IFTHEN_PREDICATE:
	  		{
	  			IfThenPredicate predicate = (IfThenPredicate)object;
		
	  			getVariables(predicate.getIfLiterals(), variableList); 			
	  			getVariables(predicate.getThenLiterals(), variableList);
	  		}
	  			break;
	      
	  		case PCG_OR_NODE:
	  		{
	  			PCGOrNode orNode = (PCGOrNode)object;
	  			if ((orNode.getPredicate() instanceof IfThenElsePredicate) 
	  					|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHENELSE)) {
	  				getVariables(((PCGAndNode)orNode.getChild(0)).getChildren(), variableList);  				
	  				getVariables(((PCGAndNode)orNode.getChild(1)).getChildren(), variableList);	  				
	  				getVariables(((PCGAndNode)orNode.getChild(2)).getChildren(), variableList);
	  			} else if ((orNode.getPredicate() instanceof IfThenPredicate) 
	  					|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHEN)) {
	  				getVariables(((PCGAndNode)orNode.getChild(0)).getChildren(), variableList);	  				
	  				getVariables(((PCGAndNode)orNode.getChild(1)).getChildren(), variableList);
	  			} else {
	  				getVariables(orNode.getArguments(), variableList);
	  			}
	  		}
	  			break;
	      
	  		case PCG_AND_NODE:
	  		{
	  			PCGAndNode andNode = (PCGAndNode)object;
	  			getVariables(andNode.getArguments(), variableList);
	  		}
	  			break;
	      
	  		case COMPILER_TYPE_LIST:
	  		{
	  			CompilerTypeList list = (CompilerTypeList)object;
	  			for (CompilerTypeBase compilerTypeObject : list)
	  				getVariables(compilerTypeObject, variableList);
	  		}
	  			break;
	  			
	  		case CAST:
	  		{
	  			CompilerTypeCast cast = (CompilerTypeCast)object;
	  			getVariables(cast.getValue(), variableList);
	  		}
	  			break;
	  	
	  		case ARITHMETIC_EXPRESSION:
	  		{
	  			CompilerArithmeticExpression cbae = (CompilerArithmeticExpression)object;
	  			getVariables(cbae.getArgument1(), variableList);
	  			if (cbae.isBinary())
	  				getVariables(cbae.getArgument2(), variableList);
	  		}
	  			break;
	      
	  		default:
	  			break;
	  	}
	}
	
	public static void getBoundVariables(CompilerTypeList arguments, Binding binding, CompilerVariableList boundVariableList) {
		for (int i = 0; i < arguments.size(); i++)
			if (binding.getBinding(i) == BindingType.BOUND)
				getVariables(arguments.get(i), boundVariableList);
	}

	public static void getFreeVariables(CompilerTypeList arguments, Binding binding, CompilerVariableList boundVariableList) {
		for (int i = 0; i < arguments.size(); i++)
			if (binding.getBinding(i) == BindingType.FREE)
				getVariables(arguments.get(i), boundVariableList);
	}
	
	public static void getIntersectingVariables(CompilerVariableList list1, CompilerVariableList list2, CompilerVariableList list3) {
		for (int i = 0; i < list1.size(); i++) {
			if (list2.contains(list1.get(i)))
				list3.add(list1.get(i));
	    }
	}
	
	public static CompilerVariableList getIntersectingVariables(CompilerVariableList list1, CompilerVariableList list2) {
		CompilerVariableList resultList = new CompilerVariableList();
		for (int i = 0; i < list1.size(); i++) {
			if (list2.contains(list1.get(i)))
				resultList.add(list1.get(i));
	    }
		return resultList;
	}

	public static void getDifferenceVariables(CompilerVariableList list1, CompilerVariableList list2, CompilerVariableList list3) {
		for (int i = 0; i < list1.size(); i++) {
			if (!list2.contains(list1.get(i)))
				list3.add(list1.get(i));
	    }
	}	

	public static Quad<Boolean, CompilerVariableList, CompilerVariableList, CompilerVariableList> assignIfThenElseBoundVariableList(PCGOrNode orNode, 
				CompilerVariableList boundVariableList, 
				CompilerVariableList boundVariableList1, 
				CompilerVariableList boundVariableList2, 
				CompilerVariableList boundVariableList3) {
		
		boolean intersectsList = false;
		PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0);
		PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1);
		PCGAndNode elseAndNode = (PCGAndNode) orNode.getChild(2);
		PCGOrNode lastIfOrNode = ifAndNode.getLastChild();
		PCGOrNode lastThenOrNode = thenAndNode.getLastChild();
		PCGOrNode lastElseOrNode = elseAndNode.getLastChild();
		CompilerVariableList ifBoundVariableList;
		CompilerVariableList thenBoundVariableList;
		CompilerVariableList elseBoundVariableList;
		
		boolean isLastIf = false;
		boolean isLastThen = false;
		boolean isLastElse = false;
		
		if (lastIfOrNode.getPredicate() instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)lastIfOrNode.getPredicate();
			isLastIf = bip.isFalse();
		}
		
		if (lastThenOrNode.getPredicate() instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)lastThenOrNode.getPredicate();
			isLastThen = bip.isFalse();
		}
		
		if (lastElseOrNode.getPredicate() instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)lastElseOrNode.getPredicate();
			isLastElse = bip.isFalse();
		}
		
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

	public static Pair<CompilerVariableList, CompilerVariableList> assignIfThenBoundVariableList(PCGOrNode orNode, 
			CompilerVariableList boundVariableList, CompilerVariableList boundVariableList1, 
			CompilerVariableList boundVariableList2) {
		
		boolean isLastIf = false;
		boolean isLastThen = false;
		
		PCGAndNode ifAndNode = (PCGAndNode)orNode.getChild(0);
		PCGOrNode lastIfOrNode = ifAndNode.getLastChild();		
		if (lastIfOrNode.getPredicate() instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)lastIfOrNode.getPredicate();
			isLastIf = bip.isFalse();
		}
		
		PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1);		
		PCGOrNode lastThenOrNode = thenAndNode.getLastChild();
		
		if (lastThenOrNode.getPredicate() instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)lastThenOrNode.getPredicate();
			isLastThen = bip.isFalse();
		}
		
		CompilerVariableList ifBoundVariableList;
		CompilerVariableList thenBoundVariableList;
	  		
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

	public static CompilerTypeBase copyExceptVariables(CompilerTypeBase object) {
		CompilerTypeBase retval;
	  
		switch (object.getType()) {
	    	case VARIABLE:
	    		retval = object;
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
		    	retval = object.copy();
		    	break;

		    case COMPILER_FUNCTOR: {
		    	CompilerFunctor functor = (CompilerFunctor)object;
		    	retval = new CompilerFunctor(functor.getFunctorName(), functor.getArguments().copyExceptVariables());
		    }
		    	break;

		    case BUILT_IN_AGGREGATE: {
		    	BuiltInAggregate bia = (BuiltInAggregate)object;
		    	CompilerTypeBase term = bia.getAggregateTerm();
	    	  
		    	retval = new BuiltInAggregate(bia.getAggregateName(), (term != null ? copyExceptVariables(term) : null));
		    }
		    	break;
		    	
		    case FS_AGGREGATE: {
		    	FSAggregate fsAggregate = (FSAggregate)object;
		    	CompilerTypeBase term = fsAggregate.getAggregateTerm();
	    	  
		    	retval = new FSAggregate(fsAggregate.getAggregateName(), (term != null ? copyExceptVariables(term) :  null));
		    }
		    	break;
		    	
		    case USER_DEFINED_AGGREGATE: {
		    	UserDefinedAggregate uda = (UserDefinedAggregate)object;
		    	CompilerTypeBase term = uda.getAggregateTerm();
	    	  
		    	retval = new UserDefinedAggregate(uda.getAggregateName(), (term != null ? copyExceptVariables(term) :  null));
		    }
		    	break;
		    
		    case COMPILER_LIST:  {
		    	CompilerList list = (CompilerList)object;
		    	if (!(list.isEmpty())) {
		    		CompilerTypeBase head = copyExceptVariables(list.getHead());
		    		CompilerList tail = null;
		    		if (list.getTail() != null)
		    			tail = (CompilerList)copyExceptVariables(list.getTail());
		    		retval = new CompilerList(head, tail);
		    	} else {
		    		retval = object.copy();
		    	}
		    }
		    	break;

		    case COMPILER_TYPE_LIST: {
		    	CompilerTypeList list = (CompilerTypeList) object;
		    	retval = list.copyExceptVariables();
		    }
		    	break;
		    	
		    case CAST:
	  		{
	  			CompilerTypeCast cast = (CompilerTypeCast)object;
	  			retval = CompilerTypeCast.create(copyExceptVariables(cast.getValue()), cast.getDataType());
	  		}
	  			break;
	  		
		    case ARITHMETIC_EXPRESSION:
		    {
		    	CompilerArithmeticExpression cbae = (CompilerArithmeticExpression)object;
		    	if (cbae.isUnary())
		    		retval = new CompilerArithmeticExpression(cbae.getOperation(), copyExceptVariables(cbae.getArgument1()), cbae.getDataType());
		    	else
		    		retval = new CompilerArithmeticExpression(cbae.getOperation(), copyExceptVariables(cbae.getArgument1()), 
		    				copyExceptVariables(cbae.getArgument2()), cbae.getDataType());
		    }
		    	break;

		    case PREDICATE:
		    case IFTHENELSE_PREDICATE:
		    case IFTHEN_PREDICATE:
		    case PCG_OR_NODE:
		    case PCG_AND_NODE:
		    default: {
		    	retval = null;
		    	throw new CompilerException("illegal types in copyExceptVariables()");
		    }
	    }
	  
		return retval;
	}

	public static void substituteVariable(CompilerTypeBase container, CompilerVariable oldVariable, CompilerTypeBase newObject) {
		boolean copy = true;

		if (newObject.isVariable())
			copy = false;

		if (substituteObject(container, oldVariable, newObject, copy) != null)
			throw new CompilerException("bad substitution for immediate variable in substituteVariable");
	}
	
	public static CompilerTypeBase substituteObject(List<?> container, CompilerTypeBase oldObject, 
			CompilerTypeBase newObject, boolean copy) {
		
		CompilerTypeBase retval;
		for (Object object : container) {
			retval = substituteObject((CompilerTypeBase)object, oldObject, newObject, copy);
			if (retval != null)
				return retval;
			
		}
		return null;
	}
	
	public static CompilerTypeBase substituteObject(CompilerTypeBase container, CompilerTypeBase oldObject, 
			CompilerTypeBase newObject, boolean copy) {
		CompilerTypeBase retval;

		if (container.equals(oldObject)) {
			if (copy)
				retval = copyExceptVariables(newObject);
			else
				retval = newObject;
		} else {
			retval = null;

			switch (container.getType())
			{
				case VARIABLE:
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
				case COMPILER_NIL:
					break;
		  
				case COMPILER_FUNCTOR: {
					substituteObject(((CompilerFunctor) container).getArguments(), oldObject, newObject, copy);
				}
					break;
		  
				case AGGREGATE:
				case BUILT_IN_AGGREGATE:
				case FS_AGGREGATE:
				case USER_DEFINED_AGGREGATE: {
					CompilerTypeBase term = ((Aggregate)container).getAggregateTerm();
					CompilerTypeBase newTerm = substituteObject(term, oldObject, newObject, copy);							
					if ((term != null) && (newTerm != null))
						((Aggregate)container).setAggregateTerm(newTerm);
				}
					break;
		  
				case COMPILER_LIST: {
					CompilerList list = (CompilerList)container;
					CompilerTypeBase newHead;
					CompilerList newTail;
					if (!list.isEmpty()) {
						newHead = substituteObject(list.getHead(), oldObject, newObject, copy);
						if (newHead != null)
							list.setHead(newHead);
						
						if (list.getTail() != null) {
							newTail = (CompilerList)substituteObject(list.getTail(), oldObject, newObject, copy);
							if (newTail != null)
								list.setTail(newTail);
						}
					}
				}
					break;

				case PREDICATE: {
					substituteObject(((Predicate)container).getArguments(), oldObject, newObject, copy);
				}
					break;
		  
				case IFTHENELSE_PREDICATE: {
					IfThenElsePredicate predicate = (IfThenElsePredicate)container;
		    
					substituteObject(predicate.getIfLiterals(), oldObject, newObject, copy);
					substituteObject(predicate.getThenLiterals(), oldObject, newObject, copy);
					substituteObject(predicate.getElseLiterals(), oldObject, newObject, copy);
				}
					break;
		  
				case IFTHEN_PREDICATE: {
					IfThenPredicate predicate = (IfThenPredicate)container;
		    
					substituteObject(predicate.getIfLiterals(),	oldObject, newObject, copy);
					substituteObject(predicate.getThenLiterals(), oldObject, newObject, copy);
				}
					break;

				case PCG_OR_NODE: {
					PCGOrNode orNode = (PCGOrNode)container;		    
					substituteObject(orNode.getArguments(), oldObject, newObject, copy);
		    
					if (orNode.getPredicate().getPredicateType() == PredicateType.BUILT_IN) {
						if ((orNode.getPredicate() instanceof IfThenElsePredicate)
								|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHENELSE)) {
							substituteObject((CompilerTypeBase)orNode.getChild(0), oldObject, newObject, copy);
							substituteObject((CompilerTypeBase)orNode.getChild(1), oldObject, newObject, copy);
							substituteObject((CompilerTypeBase)orNode.getChild(2), oldObject, newObject, copy);
				
							for (PCGOrNode node : ((PCGAndNode) orNode.getChild(0)).getChildren())
								substituteObject(node, oldObject, newObject, copy);
							for (PCGOrNode node : ((PCGAndNode) orNode.getChild(1)).getChildren())
								substituteObject(node, oldObject, newObject, copy);							
							for (PCGOrNode node : ((PCGAndNode) orNode.getChild(2)).getChildren())
								substituteObject(node, oldObject, newObject, copy);
						} else if ((orNode.getPredicate() instanceof IfThenPredicate) 
								|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHEN)) {
							substituteObject((CompilerTypeBase)orNode.getChild(0), oldObject, newObject, copy);
							substituteObject((CompilerTypeBase)orNode.getChild(1), oldObject, newObject, copy);
				
							for (PCGOrNode node : ((PCGAndNode)orNode.getChild(0)).getChildren())
								substituteObject(node, oldObject, newObject, copy);
							for (PCGOrNode node : ((PCGAndNode)orNode.getChild(1)).getChildren())
								substituteObject(node, oldObject, newObject, copy);
						}
					}
				}
					break;
		  
				case PCG_AND_NODE: {
					PCGAndNode andNode = (PCGAndNode) container;
					substituteObject(andNode.getArguments(), oldObject, newObject, copy);
				}
					break;
		  
				case COMPILER_TYPE_LIST: {
					CompilerTypeList objectList = (CompilerTypeList) container;
					CompilerTypeBase newTerm;
					for (int i = 0; i < objectList.size(); i++) {
						newTerm = substituteObject(objectList.get(i), oldObject, newObject, copy);
						if (newTerm != null)
							objectList.set(i, newTerm);
					}
				}
	      	  		break;
	      	  		
				case CAST: {
					CompilerTypeCast cast = (CompilerTypeCast)container;
					CompilerTypeBase newValue = substituteObject(cast.getValue(), oldObject, newObject, copy);
					if (newValue != null)
						cast.setValue(newValue);			  			
				}
					break;
							 
				case ARITHMETIC_EXPRESSION: {
					CompilerArithmeticExpression cae = (CompilerArithmeticExpression)container;
					CompilerTypeBase newValue = substituteObject(cae.getArgument1(), oldObject, newObject, copy);
					if (newValue != null)
						cae.setArgument1(newValue);
					
					if (cae.isBinary()) {
						newValue = substituteObject(cae.getArgument2(), oldObject, newObject, copy);
						if (newValue != null)
							cae.setArgument2(newValue);
					}
				}
					break;
					
				default: {
					throw new CompilerException("Unknown type in substituteObject");
				}
			}
	    }

		return retval;
	}

	public static void getVariableCounts(CompilerTypeBase object, Map<CompilerVariable, Integer> variableCounts) {	
		switch (object.getType()) {
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

	  		case VARIABLE: {
	  			int count = 0;
	  			if (variableCounts.containsKey(object))
	  				count = variableCounts.get(object);
	  			variableCounts.put((CompilerVariable)object, count + 1);
	  		}
	  			break;
	      
	  		case COMPILER_FUNCTOR: {
	  			CompilerFunctor functor = (CompilerFunctor)object;  			
	  			getVariableCounts(functor.getArguments(), variableCounts);
	  		}
	  			break;
	      
	  		case AGGREGATE:
	  		case BUILT_IN_AGGREGATE:
	  		case FS_AGGREGATE:
	  		case USER_DEFINED_AGGREGATE: {
	  			CompilerTypeBase aggregateTerm = ((Aggregate)object).getAggregateTerm(); 
	  			if (aggregateTerm != null)
	  				getVariableCounts(aggregateTerm, variableCounts);
	  			
	  		}
	  			break;
	      
	  		case COMPILER_LIST: {
	  			CompilerList list = (CompilerList)object;
		
	  			if (!list.isEmpty()) {
	  				getVariableCounts(list.getHead(), variableCounts);
	  				if (list.getTail() != null)
	  					getVariableCounts(list.getTail(), variableCounts);
	  			}
	  		}
	  			break;
	      
	  		case PREDICATE: {
	  			Predicate predicate = (Predicate)object;
	  			getVariableCounts(predicate.getArguments(), variableCounts);
	  		}
	  			break;
	      
	  		case IFTHENELSE_PREDICATE: {
	  			IfThenElsePredicate predicate = (IfThenElsePredicate)object;
		
	  			getVariableCounts(predicate.getIfLiterals(), variableCounts);	  			
	  			getVariableCounts(predicate.getThenLiterals(), variableCounts);  			
	  			getVariableCounts(predicate.getElseLiterals(), variableCounts);
	  		}
	  			break;
	      
	  		case IFTHEN_PREDICATE: {
	  			IfThenPredicate predicate = (IfThenPredicate)object;
		
	  			getVariableCounts(predicate.getIfLiterals(), variableCounts); 			
	  			getVariableCounts(predicate.getThenLiterals(), variableCounts);
	  		}
	  			break;
	      
	  		case PCG_OR_NODE: {
	  			PCGOrNode orNode = (PCGOrNode)object;
	  			if ((orNode.getPredicate() instanceof IfThenElsePredicate) 
	  					|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHENELSE)) {
	  				getVariableCounts(((PCGAndNode)orNode.getChild(0)).getChildren(), variableCounts);  				
	  				getVariableCounts(((PCGAndNode)orNode.getChild(1)).getChildren(), variableCounts);	  				
	  				getVariableCounts(((PCGAndNode)orNode.getChild(2)).getChildren(), variableCounts);
	  			} else if ((orNode.getPredicate() instanceof IfThenPredicate) 
	  					|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHEN)) {
	  				getVariableCounts(((PCGAndNode)orNode.getChild(0)).getChildren(), variableCounts);	  				
	  				getVariableCounts(((PCGAndNode)orNode.getChild(1)).getChildren(), variableCounts);
	  			} else {
	  				getVariableCounts(orNode.getArguments(), variableCounts);
	  			}
	  		}
	  			break;
	      
	  		case PCG_AND_NODE: {
	  			PCGAndNode andNode = (PCGAndNode)object;
	  			getVariableCounts(andNode.getArguments(), variableCounts);
	  		}
	  			break;
	      
	  		case COMPILER_TYPE_LIST: {
	  			CompilerTypeList list = (CompilerTypeList)object;
	  			for (CompilerTypeBase compilerTypeObject : list)
	  				getVariableCounts(compilerTypeObject, variableCounts);
	  		}
	  			break;
	  			
	  		case CAST: {
	  			CompilerTypeCast cast = (CompilerTypeCast)object;
	  			getVariableCounts(cast.getValue(), variableCounts);
	  		}
	  			break;
	  		
	  		case ARITHMETIC_EXPRESSION: {
	  			CompilerArithmeticExpression cae = (CompilerArithmeticExpression)object;
	  			getVariableCounts(cae.getArgument1(), variableCounts);
	  			if (cae.isBinary())
	  				getVariableCounts(cae.getArgument2(), variableCounts);
	  		}
	  			break;
	  		default:
	  			break;
	  	}
	}
	
	public static void getVariableCounts(List<?> compilerTypeObjects, Map<CompilerVariable, Integer> variableCounts) {	
		for (Object compilerTypeObject : compilerTypeObjects)
			getVariableCounts((CompilerTypeBase)compilerTypeObject, variableCounts);
	}	
	
	public static void getConstants(List<?> compilerTypeObjects, CompilerTypeList constants) {
		for (Object compilerTypeObject : compilerTypeObjects)
			getConstants((CompilerTypeBase)compilerTypeObject, constants);
	}
		
	public static void getConstants(CompilerTypeBase object, CompilerTypeList constants) {	
		switch (object.getType()) {
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
				if (!constants.contains(object))
					constants.add(object);
				break;
	  		case INPUT_VARIABLE:
	  			getConstants(((CompilerInputVariable)object).getValue(), constants);
	  			break;

	  		case VARIABLE:	  		
	  			if (((CompilerVariable)object).getValue() != null)
	  				getConstants(((CompilerVariable)object).getValue(), constants);
	  			
	  			break;
	      
	  		case COMPILER_FUNCTOR:
	  			CompilerFunctor functor = (CompilerFunctor)object;  			
	  			getConstants(functor.getArguments(), constants);
	  			break;
	      
	  		case AGGREGATE:
	  		case BUILT_IN_AGGREGATE:
	  		case FS_AGGREGATE:
	  		case USER_DEFINED_AGGREGATE:
	  		{
	  			CompilerTypeBase aggregateTerm = ((Aggregate)object).getAggregateTerm(); 
	  			if (aggregateTerm != null)
	  				getConstants(aggregateTerm, constants);
	  			
	  		}
	  			break;
	      
	  		case COMPILER_LIST:	  		
	  			CompilerList list = (CompilerList)object;
		
	  			if (!list.isEmpty()) {
	  				getConstants(list.getHead(), constants);
	  				if (list.getTail() != null)
	  					getConstants(list.getTail(), constants);
	  			}	  		
	  			break;
	      
	  		case PREDICATE:
	  		{
	  			Predicate predicate = (Predicate)object;
	  			getConstants(predicate.getArguments(), constants);
	  		}
	  			break;
	      
	  		case IFTHENELSE_PREDICATE:
	  		{
	  			IfThenElsePredicate predicate = (IfThenElsePredicate)object;
		
	  			getConstants(predicate.getIfLiterals(), constants);	  			
	  			getConstants(predicate.getThenLiterals(), constants);  			
	  			getConstants(predicate.getElseLiterals(), constants);
	  		}
	  			break;
	      
	  		case IFTHEN_PREDICATE:
	  		{
	  			IfThenPredicate predicate = (IfThenPredicate)object;
		
	  			getConstants(predicate.getIfLiterals(), constants); 			
	  			getConstants(predicate.getThenLiterals(), constants);
	  		}
	  			break;
	      
	  		case PCG_OR_NODE:
	  		{
	  			PCGOrNode orNode = (PCGOrNode)object;
	  			if ((orNode.getPredicate() instanceof IfThenElsePredicate) 
	  					|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHENELSE)) {
	  				getConstants(((PCGAndNode)orNode.getChild(0)).getChildren(), constants);  				
	  				getConstants(((PCGAndNode)orNode.getChild(1)).getChildren(), constants);	  				
	  				getConstants(((PCGAndNode)orNode.getChild(2)).getChildren(), constants);
	  			} else if ((orNode.getPredicate() instanceof IfThenPredicate) 
	  					|| (orNode.getBuiltInPredicateType() == BuiltInPredicateType.IFTHEN)) {
	  				getConstants(((PCGAndNode)orNode.getChild(0)).getChildren(), constants);	  				
	  				getConstants(((PCGAndNode)orNode.getChild(1)).getChildren(), constants);
	  			} else {
	  				getConstants(orNode.getArguments(), constants);
	  			}
	  		}
	  			break;
	      
	  		case PCG_AND_NODE:	  		
	  			PCGAndNode andNode = (PCGAndNode)object;
	  			getConstants(andNode.getArguments(), constants);
	  			break;
	      
	  		case COMPILER_TYPE_LIST:	  		
	  			for (CompilerTypeBase compilerTypeObject : (CompilerTypeList)object)
	  				getConstants(compilerTypeObject, constants);	  		
	  			break;
	  			
	  		case CAST:
	  			getConstants(((CompilerTypeCast)object).getValue(), constants);
	  			break;
	  	
	  		case ARITHMETIC_EXPRESSION:	  		
	  			CompilerArithmeticExpression cbae = (CompilerArithmeticExpression)object;
	  			getConstants(cbae.getArgument1(), constants);
	  			if (cbae.isBinary())
	  				getConstants(cbae.getArgument2(), constants);	  		
	  			break;
	      
	  		default:
	  			break;
	  	}
	}
}
