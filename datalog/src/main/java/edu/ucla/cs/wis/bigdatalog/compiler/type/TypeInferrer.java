package edu.ucla.cs.wis.bigdatalog.compiler.type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.Aggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.UserDefinedAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.BasicClique;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateStructuralAttribute;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenElsePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TypeInferrer {
	private static Logger logger = LoggerFactory.getLogger(TypeInferrer.class.getName());
	
	private final static int MULTI_RULE_ARITY = 4;	// multi predicates always have 4 arguments
	private final static int MULTI_RULE_RETURN_ARGUMENT_POSITION = 3;	
	private final static int MAX_ATTEMPTS = 100;
	
	private DeALSContext deALSContext;
	
	public TypeInferrer(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	
	public boolean inferTypes(Module module) {		
		this.deALSContext.logTrace(logger, "Entering assignTypes for {}", module.getModuleName());		
		this.deALSContext.logInfo(logger, "[BEGIN Assign Argument Data Types for Module '{}' BEGIN]", module.getModuleName());
		// Process:
		// 1) Set the DataType on all variables of base predicates
		// 2) Use the results of step 1 to update the remaining goals of the rule
		// 2b) This will require inferring results of operations such as add/minus/etc.
		// 3) Use the variable DataTypes in the body of the rule to update the variables in the head
		
		// iterate through all derived predicates using the base predicates for variable types
		// we only have to use ordinal position to do the assignments
		List<Predicate> assignedPredicates = new ArrayList<>();
		List<Predicate>  partiallyAssignedPredicates = new ArrayList<>();
		ArrayList<Rule> unassignedRules = new ArrayList<>();
		int iteration = 0;
		
		// STEP 1 - collect assigned rules
		for (DerivedPredicate derivedPredicate : module.getDerivedPredicates()) {	
			for (Rule rule : derivedPredicate.getRules()) {
				if (TypeInferrer.isPredicateAssigned(rule.getHead()))
					assignedPredicates.add(rule.getHead());
				else
					partiallyAssignedPredicates.add(rule.getHead());

				for (Predicate goal : rule.getBody())
					if (TypeInferrer.isPredicateAssigned(goal))
						assignedPredicates.add(goal);
					else
						partiallyAssignedPredicates.add(goal);
				
				if (!TypeInferrer.isRuleAssigned(rule))
					unassignedRules.add(rule);
			}
		}

		// STEP 2 - attempt to assign rules, partitioning failed and successful rules into separate lists
		ArrayList<Rule> failedRules = new ArrayList<>();

		boolean retry = true;
		
		while (retry && unassignedRules.size() > 0) {
			retry = false;
			this.deALSContext.logDebug(logger, "\nTrying to assign types in the remaining {} rules.", unassignedRules.size());
			
			failedRules.clear();
			boolean isAssigned = false;
			for (Rule rule : unassignedRules) {
				isAssigned = assignRule(module, rule, assignedPredicates, assignedPredicates, partiallyAssignedPredicates);
				if (isAssigned) {
					retry = true;
				} else {
					isAssigned = assignRule(module, rule, partiallyAssignedPredicates, assignedPredicates, partiallyAssignedPredicates);
					if (isAssigned)
						retry = true;
				}
				
				// double check that the rule is assigned
				if (isAssigned && TypeInferrer.isRuleAssigned(rule)) {
					partiallyAssignedPredicates.remove(rule.getHead());
						
					for (Predicate goal : rule.getBody())
						partiallyAssignedPredicates.remove(goal);

					if (!assignedPredicates.contains(rule.getHead()))
						assignedPredicates.add(rule.getHead());
					
					for (Predicate goal : rule.getBody())
						if (!assignedPredicates.contains(goal))
							assignedPredicates.add(goal);
						
					this.deALSContext.logInfo(logger, "Success!");
					this.showRule(rule);
				} else {
					isAssigned = false;
				}
				
				if (!isAssigned) {
					if (!partiallyAssignedPredicates.contains(rule.getHead()))
						partiallyAssignedPredicates.add(rule.getHead());
						
					for (Predicate goal : rule.getBody())
						if (isPredicateAssigned(goal))
							partiallyAssignedPredicates.remove(goal);
						else if (!partiallyAssignedPredicates.contains(goal))
							partiallyAssignedPredicates.add(goal);
					
					this.deALSContext.logInfo(logger, "Partial assignment:");
					this.showRule(rule);
					failedRules.add(rule);	
				}
			}

			unassignedRules.clear();
			for (Rule rule : failedRules)
				unassignedRules.add(rule);

			if (this.deALSContext.isDebugEnabled()) {
				if (failedRules.isEmpty()) {
					this.deALSContext.logDebug(logger, "\nAll variables in all rules have been successfully assigned types!");
					break;
				}
				
				this.deALSContext.logDebug(logger, "\nThe following rules failed to be fully assigned:");
				for (Rule failedRule : failedRules) {					
					for (Predicate goal : failedRule.getBody())
						this.deALSContext.logDebug(logger, "    {}", showPredicate(goal, false));
					
					this.deALSContext.logInfo(logger, "  {}\n", showPredicate(failedRule.getHead(), true));
				}
			}
			
			// last ditch effort to assign all binary (+-*/) predicates to use floats datatypes
			// maybe this will cause other assignments
			if ((iteration > 0) && !retry)
				retry = TypeInferrer.assignUnassignedBinaryPredicateVariablesToDouble(partiallyAssignedPredicates);
			
			iteration++;
			
			if (iteration > MAX_ATTEMPTS)
				break;			
		}
		
		// APS 11/29/2013
		// Verify recursive predicates have the 'largest' datatypes
		// based on the order in which types are assigned to unknown arguments, a recursive goal and head could end up with different datatypes
		// this is confusing for later in the program generation, so promote here.  
		// This is caused by the introduction of long as a system type
		this.promoteNumericVariableTypeAssignments(module, assignedPredicates);
		
		// APS 5/20/2014
		// synchronize types in all heads of the same predicate
		this.sychronizePredicateSignatures(module, assignedPredicates);
		
		boolean status = (failedRules.size() == 0); 
		
		this.deALSContext.logTrace(logger, "Exiting assignTypes for {} with status = {}", module.getModuleName(), status);
		
		this.deALSContext.logInfo(logger, "[END Assign Argument Data Types for Module '{}' END]", module.getModuleName());

		return status;
	}
	
	private boolean assignRule(Module module, Rule rule, List<Predicate> predicates, 
			List<Predicate> assignedPredicates, List<Predicate> partiallyAssignedPredicates) {				
		this.deALSContext.logInfo(logger, "\nAssigning rule({}):", rule.getRuleId());
		this.deALSContext.logInfo(logger, "  {}", this.showPredicate(rule.getHead(), true));
		for (Predicate goal : rule.getBody())
			this.deALSContext.logInfo(logger, "    {}", this.showPredicate(goal, false));
		
		Predicate head = rule.getHead();
		
		//for each rule, collect variables - variables will not span across rules
		CompilerTypeList headVariables = new CompilerTypeList();
		collectTypeableArguments(head, headVariables);
		
		CompilerTypeList bodyVariables = new CompilerTypeList();
		for (Predicate literal : rule.getBody())
			collectTypeableArguments(literal, bodyVariables);		
		
		// to store the already typed variables
		HashMap<String, DataType> variableDataTypeAssignmentsMap = new HashMap<>();
		
		TypeInferrer.removeAssignedArguments(headVariables, variableDataTypeAssignmentsMap);		
		int beforeUnassignedHeadVariables = headVariables.size();
		
		TypeInferrer.removeAssignedArguments(bodyVariables, variableDataTypeAssignmentsMap);
		int beforeUnassignedBodyVariables = bodyVariables.size();

		this.deALSContext.logInfo(logger, "Body variables before assignment: {}", bodyVariables.size());
		
		// assign body first, so we can use it to assign head
		boolean bodyRetval = this.assignBody(module, rule.getBody(), variableDataTypeAssignmentsMap, predicates);
		
		TypeInferrer.removeAssignedArguments(headVariables, variableDataTypeAssignmentsMap);
		
		TypeInferrer.removeAssignedArguments(bodyVariables, variableDataTypeAssignmentsMap);
						
		this.deALSContext.logInfo(logger, "Body variables after assignment: {}", bodyVariables.size());

		// see if by assigning the body, we've taken care of assigning the head
		if (headVariables.isEmpty() && this.setAggregatesReturnType(head, assignedPredicates))
			if (bodyRetval)
				return true; // done
		
		this.deALSContext.logInfo(logger, "Head variables before assignment: {}", headVariables.size());
		
		// check if UDA needs to be assigned before assuming failure
		// UDAs are special in that they area combination of rules that work together to define the type assignments
		boolean headSuccessful = false;
		if (head.isBuiltIn())
			headSuccessful = this.assignUserDefinedAggregateRule(module, rule, assignedPredicates);				
		else
			headSuccessful = TypeInferrer.assignHead(head, assignedPredicates, partiallyAssignedPredicates);
		
		if (headSuccessful && headVariables.isEmpty())
			this.setAggregatesReturnType(head, assignedPredicates);
				
		// APS 1/1/2014
		// check if there is a list in head - our other checks miss this, but it is easy to identify
		if (!headVariables.isEmpty() || !headSuccessful) {
			TypeInferrer.handleListsInHead(head);
			
			TypeInferrer.removeAssignedArguments(headVariables, variableDataTypeAssignmentsMap);

			if (bodyVariables.isEmpty() && !headVariables.isEmpty()) {
				this.assignAggregatesWithAnonymousTerms(head);
				TypeInferrer.removeAssignedArguments(headVariables, variableDataTypeAssignmentsMap);
			}
			
			// check again in case we did some assignments
			if (headVariables.isEmpty())
				this.setAggregatesReturnType(head, assignedPredicates);
		} 
		
		this.deALSContext.logInfo(logger, "Head variables after assignment: {}", headVariables.size());
	
		return !(((beforeUnassignedBodyVariables - bodyVariables.size()) == 0) 
				&& ((beforeUnassignedHeadVariables - headVariables.size()) == 0));
	}
	
	private void assignAggregatesWithAnonymousTerms(Predicate head) {
		for (int i = 0; i < head.getArity(); i++) {
			if (head.getArgument(i).isBuiltInAggregate()) {
				Aggregate aggregate = (Aggregate)head.getArgument(i);
				if (aggregate.getAggregateTerm().isVariable() && ((CompilerVariable)aggregate.getAggregateTerm()).isAnonymous()) {
					CompilerVariable aggregateTerm = (CompilerVariable)aggregate.getAggregateTerm();
					if (aggregateTerm.getDataType() == DataType.UNKNOWN)
						aggregateTerm.setDataType(inferAggregateDataType(aggregate, new ArrayList<Predicate>()));
				}
			}
		}
	}
	
	private static void handleListsInHead(Predicate head) {
		CompilerTypeBase argument;
		for (int i = 0; i < head.getArity(); i++) {
			argument = head.getArgument(i);
			if (argument.isList()) {			
				CompilerList list = (CompilerList)argument;
				if (!list.isEmpty()) {
					if (list.getHead().isList()) {
						if (((CompilerList)list.getHead()).getHead() instanceof CompilerVariable) {
							CompilerVariable headList = (CompilerVariable)((CompilerList)list.getHead()).getHead(); 
							if (headList.getDataType() == DataType.UNKNOWN)
								headList.setDataType(DataType.LIST);
						}
					} else {
						if (list.getHead() instanceof CompilerVariable) {
							if (list.getHead().getDataType() == DataType.UNKNOWN)
								((CompilerVariable)list.getHead()).setDataType(DataType.ANY);
						}
					}
					
					if (list.getTail().isList()) {
						if (list.getTail().getHead() instanceof CompilerVariable) {
							CompilerVariable tail = (CompilerVariable)list.getTail().getHead(); 
							if (tail.getDataType() == DataType.UNKNOWN)
								tail.setDataType(DataType.LIST);
						}
					}
				}
			}
		}
	}
	
	private static void removeAssignedArguments(CompilerTypeList arguments, HashMap<String, DataType> variableDataTypeAssignmentsMap) {
		CompilerTypeBase arg;
		for (int i = arguments.size() - 1; i >= 0; i--) {
			arg = arguments.get(i);
			if (arg.getDataType() != DataType.UNKNOWN) {
				arguments.remove(arg);
				if (arg.isVariable()) {
					if (variableDataTypeAssignmentsMap != null)
						variableDataTypeAssignmentsMap.put(((CompilerVariable)arg).getVariableName(), arg.getDataType());
				}
			}
		}
	}
	
	private static boolean assignHead(Predicate head, List<Predicate> assignedPredicates, List<Predicate> partiallyAssignedPredicates) {
		Predicate assignedPredicate = TypeInferrer.getMatchingDerivedPredicate(assignedPredicates, head.getPredicateName(), head.getArity());
		if (assignedPredicate != null) {
			int argumentIndex = 0;
			for (CompilerTypeBase assignedArgument : assignedPredicate.getArguments()) {
				if (assignedArgument.isAnyAggregate() && ((Aggregate)assignedArgument).getAggregateTerm().isVariable()) {
					// if left is variable and right is aggregate, we can transfer the type
					CompilerTypeBase aggVariable = head.getArgument(argumentIndex);
					if (aggVariable.isVariable())
						TypeInferrer.transferType(aggVariable, ((Aggregate)assignedArgument).getAggregateTerm());
				} else {
					TypeInferrer.transferType(head.getArgument(argumentIndex), assignedArgument);
				}

				argumentIndex++;
			}
			return true;
		}
		
		// maybe we just need to complete the assignment of the arithmetic expression
		for (int i = 0; i < head.getArity(); i++)
			if (head.getArgument(i).isExpression())
				inferTermDataType(head.getArgument(i));
					
		// we don't have a fully assigned head, but maybe we have some assignments in a head
		List<Predicate> matchingPartiallyAssignedPredicates = TypeInferrer.getMatchingPredicate(partiallyAssignedPredicates, head.getPredicateName(), head.getArity(), head);
		for (Predicate partiallyAssignedPredicate : matchingPartiallyAssignedPredicates) {
			if (isPredicateAssigned(head))
				return true;
			
			if (partiallyAssignedPredicate != null) {
				CompilerTypeBase argument;
				for (int i = 0; i < head.getArity(); i++) {
					if (head.getArgument(i).isVariable() && (head.getArgument(i).getDataType() != DataType.UNKNOWN))
						continue;
					
					if (head.getArgument(i).isAnyAggregate() && (((Aggregate)head.getArgument(i)).getReturnDataType() != DataType.UNKNOWN))
						continue;
					
					argument = partiallyAssignedPredicate.getArgument(i);

					if (argument.isAnyAggregate() && ((Aggregate)argument).getReturnDataType() != DataType.UNKNOWN) {
						// if left is variable and right is aggregate with a known data type, we can transfer the type
						CompilerTypeBase aggVariable = head.getArgument(i);
						if (aggVariable.isVariable() && (aggVariable.getDataType() == DataType.UNKNOWN))
							((CompilerVariable)aggVariable).setDataType(((Aggregate)argument).getReturnDataType());
					} else if (argument.isVariable() && (argument.getDataType() != DataType.UNKNOWN)) {					
						TypeInferrer.transferType(head.getArgument(i), argument);
					}
				}	
			}
		}
				
		return false;
	}
	
	private boolean assignUserDefinedAggregateRule(Module module, Rule udaRule, List<Predicate> assignedPredicates) {
		// Assignment pattern works as such:
		// 1) assign body to assign types to aggregate term variables
		// 2) use aggregate term variables to assign types to single and multi predicate variables
		//   - the multi rule's last argument identifies the aggregate return type
		// 3) use the multi predicate variables to assign the aggregate return type
		boolean status = false;
		String udaName;
		UserDefinedAggregate uda = null;
		BuiltInPredicate head = (BuiltInPredicate)udaRule.getHead();
		
		switch(head.getBuiltInPredicateType()) {
			case SINGLE:
				// should be of the format: single(UDA Name, VAR INPUT, VAR OUTPUT) where INPUT and OUTPUT are the same VAR.
				
				// aggregate name is in 1st argument as a string
				udaName = ((CompilerString)head.getArgument(0)).getText();
				uda = TypeInferrer.getMatchingUDA(udaName, module.getDerivedPredicates());

				// with the UDA and the predicate, we can use the variables in the UDA to 
				// assign the data types to the predicate's arguments
				if (uda != null) {
					if (uda.getAggregateTerm().isVariable()) {
						for (int i = 1; i < head.getArity(); i++) {
							if (head.getArgument(i).isVariable())
								TypeInferrer.transferType(head.getArgument(i), uda.getAggregateTerm());
						}
						status = true;
					} else if (uda.getAggregateTerm().isFunctor()) {
						TypeInferrer.transferType(head.getArgument(1), uda.getAggregateTerm());
						TypeInferrer.transferType(head.getArgument(2), uda.getAggregateTerm());
						status = true;
					} else {
						status = false;
					}
				}
				
				break;
			case MULTI:
				// should be of the format: multi(UDA Name, VAR INPUT1, VAR INPUT2, VAR Retval).
				udaName = ((CompilerString)head.getArgument(0)).getText();

				uda = TypeInferrer.getMatchingUDA(udaName, module.getDerivedPredicates());
				
				Predicate single = null;
				
				for (Predicate predicate : assignedPredicates) {
					if (predicate.getPredicateName().equals(BuiltInPredicate.SINGLE_PREDICATE_NAME))
						if (((CompilerString)predicate.getArgument(0)).getText().equals(udaName))
							single = predicate;
				}

				CompilerTypeBase fromTerm = null;
				if (uda != null)
					fromTerm = uda.getAggregateTerm();
				
				// use the single's terms if we have it
				if (single != null)
					fromTerm = single.getArgument(2);
				
				TypeInferrer.transferType(head.getArgument(1), fromTerm);
				TypeInferrer.transferType(head.getArgument(2), fromTerm);
				
				//for each rule, collect variables - variables will not span across rules
				CompilerTypeList bodyArguments = new CompilerTypeList();
				for (Predicate literal : udaRule.getBody())
					TypeInferrer.collectTypeableArguments(literal, bodyArguments);
				
				CompilerTypeList headArguments = new CompilerTypeList();
				TypeInferrer.collectTypeableArguments(udaRule.getHead(), headArguments);

				HashMap<String, DataType> variableDataTypeAssignmentsMap = new HashMap<>();
								
				TypeInferrer.removeAssignedArguments(headArguments, variableDataTypeAssignmentsMap);
				TypeInferrer.removeAssignedArguments(bodyArguments, variableDataTypeAssignmentsMap);
											
				// assign body first, so we can use it to assign head
				status = this.assignBody(module, udaRule.getBody(), variableDataTypeAssignmentsMap, assignedPredicates);

				// assign the return type to the uda if we have successfully assigned the body of the multi
				// the return type of the aggregate is the same type of the last argument of the head of the multi rule
				if (status) {					
					List<UserDefinedAggregate> matchingUDAs = TypeInferrer.getMatchingUDAs(udaName, module.getDerivedPredicates());
					
					if (head.getArgument(TypeInferrer.MULTI_RULE_RETURN_ARGUMENT_POSITION).isVariable()) {
						CompilerVariable multiRuleReturnVariable = (CompilerVariable)head.getArgument(TypeInferrer.MULTI_RULE_RETURN_ARGUMENT_POSITION);

						// transfer to each uda instance
						for (UserDefinedAggregate matchingUDA : matchingUDAs)
							matchingUDA.setReturnDataType(multiRuleReturnVariable.getDataType());
						
					} else if (head.getArgument(TypeInferrer.MULTI_RULE_RETURN_ARGUMENT_POSITION).isFunctor()) {						
						// transfer to each uda instance
						for (UserDefinedAggregate matchingUDA : matchingUDAs)
							matchingUDA.setReturnDataType(DataType.COMPLEX);
					}
				}
				break;
			case RETURN:
				break;
			default:
				status = false;	// was not a UDA rule
		}
		return status;
	}
	
	private static UserDefinedAggregate getMatchingUDA(String udaName, List<DerivedPredicate> derivedPredicates) {
		UserDefinedAggregate uda = null;
		for (DerivedPredicate derivedPredicate : derivedPredicates) {
			if (uda != null) break;
			for (Rule rule : derivedPredicate.getRules()) {
				if (uda != null) break;
				CompilerTypeBase arg;
				for (int i = 0; i < rule.getHead().getArity(); i++) {
					arg = rule.getHead().getArgument(i);
					if (arg.isUserDefinedAggregate() 
							&& ((UserDefinedAggregate)arg).getAggregateName().equals(udaName)) {
						uda = (UserDefinedAggregate)arg;
						break;
					}
				}
			}
		}
		return uda;
	}
	
	private static List<UserDefinedAggregate> getMatchingUDAs(String udaName, List<DerivedPredicate> derivedPredicates) {
		List<UserDefinedAggregate> udas = new ArrayList<>();

		for (DerivedPredicate derivedPredicate : derivedPredicates) {
			for (Rule rule : derivedPredicate.getRules()) {
				CompilerTypeBase arg;
				for (int i = 0; i < rule.getHead().getArity(); i++) {
					arg = rule.getHead().getArgument(i);
					if (arg.isUserDefinedAggregate()
							&& ((UserDefinedAggregate)arg).getAggregateName().equals(udaName)) {
						udas.add((UserDefinedAggregate)arg);
					}
				}
			}
		}
		return udas;
	}
	
	private boolean assignBody(Module module, List<Predicate> body, HashMap<String, DataType> variableDataTypeAssignments, List<Predicate> predicates) {
		int[] goalsCompleted = new int[body.size()];
		int goalIndex = 0;
		boolean status = false;
		
		for (Predicate goal : body) {
			status = false;
			if (goal.isBase())	
				status = assignBase(module, goal, variableDataTypeAssignments);
			else if (goal.isDerived())
				status = this.assignDerived(module, goal, variableDataTypeAssignments, predicates);
			else if (goal.isBuiltIn())
				status = assignBuiltIn(module, goal, variableDataTypeAssignments, predicates);
			else	
				goalsCompleted[goalIndex] = 0;
						
			if (status)
				goalsCompleted[goalIndex] = 1;
			goalIndex++;
		}
		
		for (int i = 0; i < goalsCompleted.length; i++)
			if (goalsCompleted[i] == 0)
				return false;
		
		return true;
	}
	
	private static boolean assignBase(Module module, Predicate goal, HashMap<String, DataType> variableDataTypeAssignments) {
		BasePredicate basePredicate = module.getBasePredicate(goal.getPredicateName());
		if (basePredicate != null) {
			int argumentIndex = 0;
			DataType columnType;
			for (BasePredicateStructuralAttribute bpsa : basePredicate.getBasePredicateStructuralAttributes()) {
				columnType = bpsa.getDataType();
				if (columnType == DataType.UNKNOWN) {
					CompilerTypeBase arg = goal.getArgument(argumentIndex);
					if (arg.isVariable() && (arg.getDataType() != DataType.UNKNOWN))
						basePredicate.getBasePredicateStructuralAttribute(argumentIndex).setDataType(arg.getDataType());
					
					continue;
				}
				CompilerTypeBase arg = goal.getArgument(argumentIndex); 
				if (arg.isVariable()) {
					CompilerVariable varg = (CompilerVariable)goal.getArgument(argumentIndex);
					varg.setDataType(columnType);
					variableDataTypeAssignments.put(varg.getVariableName(), varg.getDataType());
				} else if (arg.isFunctor() && (columnType == DataType.COMPLEX)) {
					CompilerFunctor argFunc = (CompilerFunctor)goal.getArgument(argumentIndex);
										
					for (int i = 0; i < argFunc.getArity(); i++) {
						CompilerTypeBase argFuncArg = argFunc.getArgument(i);
						if (argFuncArg.isVariable()) {
							if((bpsa.getSchemaStructuralAttributes() != null)
								&& (bpsa.getSchemaStructuralAttributes()[i].getDataType() != DataType.UNKNOWN)) {
								((CompilerVariable)argFuncArg).setDataType(bpsa.getSchemaStructuralAttributes()[i].getDataType());
								variableDataTypeAssignments.put(((CompilerVariable)argFuncArg).getVariableName(), argFuncArg.getDataType());
							}
						}
					}
				} else if (arg.getType() == CompilerType.ARITHMETIC_EXPRESSION) {
					CompilerArithmeticExpression cae = (CompilerArithmeticExpression)goal.getArgument(argumentIndex);
					cae.setDataType(columnType);
				}
				argumentIndex++;
			}
			return true;
		}
		return false;
	}
	
	private boolean assignDerived(Module module, Predicate goal, HashMap<String, DataType> variableDataTypeAssignments, 
			List<Predicate> predicates) {
		Predicate assignedPredicate = TypeInferrer.getMatchingDerivedPredicate(goal, predicates, goal.getPredicateName(), goal.getArity());	
		// if we already have the predicate with all its types, use it to set this instance
		// otherwise, attempt to find the typed variables from the previously typed variables in the same rule
		if (assignedPredicate != null) {
			for (int i = 0; i < assignedPredicate.getArity(); i++) {
				if (assignedPredicate.getArgument(i).isAnyAggregate()) {
					Aggregate aggregate = (Aggregate)assignedPredicate.getArgument(i);
					if (goal.getArgument(i).isVariable()) {
						((CompilerVariable)goal.getArgument(i)).setDataType(aggregate.getReturnDataType());
					} else if (goal.getArgument(i).isFunctor()) /* only happens if aggregate is UDA */{
						
						// UDA - get multi rule head and use last variable
						for (Predicate predicate : predicates) {
							if (predicate.getPredicateName().equals(BuiltInPredicate.MULTI_PREDICATE_NAME) 
									&& (predicate.getArity() == MULTI_RULE_ARITY)) {
								if (((CompilerString)predicate.getArgument(0)).getText().equals(aggregate.getAggregateName())) {
									TypeInferrer.transferType(goal.getArgument(i), 
											predicate.getArgument(MULTI_RULE_RETURN_ARGUMENT_POSITION));
									break;
								}
							}
						}
					}
				} else {
					TypeInferrer.transferType(goal.getArgument(i), assignedPredicate.getArgument(i));
				}
			}
			
			if (isPredicateAssigned(goal))
				return true;
		}
		
		CompilerTypeBase argument;
		for (int argumentIndex = 0; argumentIndex < goal.getArity(); argumentIndex++) {
			argument = goal.getArgument(argumentIndex);
			if (argument.isVariable()) {
				CompilerVariable varg = (CompilerVariable)argument;
				if (varg.getDataType() != DataType.UNKNOWN)
					continue;
				// if we can find it from previously assigned variables, do so
				// otherwise, check if the variable is bound from the result of an aggregate
				// - if it is, then it should be assigned as REAL
				if (variableDataTypeAssignments.containsKey(varg.getVariableName())) {
					varg.setDataType(variableDataTypeAssignments.get(varg.getVariableName()));
				} else {
					DerivedPredicate dr = module.getDerivedPredicate(goal.getPredicateName(), goal.getArity());
					if (dr == null)
						continue;
					// get any rule - should all be the same argument typing in the head
					Rule goalsRule = dr.getRule(0);											
					if (goalsRule.getHead().getArgument(argumentIndex).isAnyAggregate())
						varg.setDataType(this.inferAggregateDataType((Aggregate)goalsRule.getHead().getArgument(argumentIndex), predicates));
					else if (goalsRule.getHead().getArgument(argumentIndex).isVariable())
						varg.setDataType(((CompilerVariable)goalsRule.getHead().getArgument(argumentIndex)).getDataType());
				}						
			} else if (argument.isFunctor()) {
				CompilerFunctor func = (CompilerFunctor)argument;
				for (CompilerTypeBase funcArg : func.getArguments()) {
					if (funcArg.isVariable()) {
						CompilerVariable varg = (CompilerVariable)funcArg;
						if (variableDataTypeAssignments.containsKey(varg.getVariableName()))										
							varg.setDataType(variableDataTypeAssignments.get(varg.getVariableName()));
						
					// could be a double nested functor - but no more than 2 levels
					} else if (funcArg.isFunctor()) {
						CompilerFunctor func2 = (CompilerFunctor)funcArg;
						for (CompilerTypeBase func2Arg : func2.getArguments()) {
							if (func2Arg.isVariable()) {
								CompilerVariable varg = (CompilerVariable)funcArg;
								if (variableDataTypeAssignments.containsKey(varg.getVariableName()))								
									varg.setDataType(variableDataTypeAssignments.get(varg.getVariableName()));
							}
						}
					}
				}
			} else if (argument.isList()) {
				CompilerList list = (CompilerList)argument;
				if (!list.isEmpty()) {
					if (list.getHead().isVariable()) {
						CompilerVariable headArg = (CompilerVariable)list.getHead();
						if (variableDataTypeAssignments.containsKey(headArg.getVariableName()))										
							headArg.setDataType(variableDataTypeAssignments.get(headArg.getVariableName()));
						else
							headArg.setDataType(DataType.ANY);
					}
					
					if ((list.getTail() != null) && list.getTail().isList()) {
						if (list.getTail().getHead() instanceof CompilerVariable) {
							CompilerVariable tail = (CompilerVariable)list.getTail().getHead(); 
							if (tail.getDataType() == DataType.UNKNOWN)
								tail.setDataType(DataType.LIST);							
						}
					}
				}
			} else if (argument.isExpression()) {
				inferTermDataType(argument);
			}
		}
		return true;
	}
	
	private boolean assignBuiltIn(Module module, Predicate goal, HashMap<String, DataType> variableDataTypeAssignments, List<Predicate> predicates) {
		BuiltInPredicate bip = (BuiltInPredicate)goal;
		boolean status = false;
		CompilerVariable varg = null;
		
		if (bip.isBinary()) {
			status = assignBinary(bip, variableDataTypeAssignments);
		} else if (bip.isIfThenElse()) {
			IfThenElsePredicate ite = (IfThenElsePredicate)bip;
			if (this.assignBody(module, ite.getIfLiterals(), variableDataTypeAssignments, predicates)
					&& this.assignBody(module, ite.getThenLiterals(), variableDataTypeAssignments, predicates)
					&& this.assignBody(module, ite.getElseLiterals(), variableDataTypeAssignments, predicates))
				status = true;
		} else if (bip.isIfThen()) {
			IfThenPredicate it = (IfThenPredicate)bip;
			if (this.assignBody(module, it.getIfLiterals(), variableDataTypeAssignments, predicates)
					&& this.assignBody(module, it.getThenLiterals(), variableDataTypeAssignments, predicates))
				status = true;			
		} else if (bip.isReadAggregate() || bip.isReadAggregateFS() || bip.isWriteAggregate() || bip.isWriteAggregateFS()) {
			this.assignDerived(module, goal, variableDataTypeAssignments, predicates);
		} else if (bip.isGeneric()){
			if (bip.getPredicateName().equals(BuiltInPredicate.APPEND_PREDICATE_NAME)) {				
				// bip has 3 args, and all are lists
				if (bip.getArgument(0) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(0));
					varg.setDataType(DataType.LIST);				
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.LIST);
				}
				
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.LIST);				
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.LIST);
				}
				
				if (bip.getArgument(2) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(2));
					varg.setDataType(DataType.LIST);				
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.LIST);
				}
				
				status = true;		
			} else if (bip.getPredicateName().equals(BuiltInPredicate.MEMBER_PREDICATE_NAME)) {
				if (bip.getArgument(0) instanceof CompilerVariable) {
					// the 1st argument is Any, if type is unknown
					varg = ((CompilerVariable)bip.getArgument(0));				
					if (variableDataTypeAssignments.containsKey(varg.getVariableName()))
						varg.setDataType(variableDataTypeAssignments.get(varg.getVariableName()));
					else
						varg.setDataType(DataType.ANY);
				
				}
				// the 2nd argument is List
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.LIST);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.LIST);
				}			
				
				status = true;
			} else if (bip.getPredicateName().equals(BuiltInPredicate.CARDINALITY_PREDICATE_NAME)) {
				if (bip.getArgument(0) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(0));
					varg.setDataType(DataType.LIST);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.LIST);
				}
				
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.INT);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.INT);
				}
					
				status = true;				
			} else if (bip.getPredicateName().equals(BuiltInPredicate.GET_NTH_MEMBER_PREDICATE_NAME)) {
				if (bip.getArgument(0) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(0));
					varg.setDataType(DataType.LIST);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.LIST);
				}
				
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.INT);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.INT);
				}
				
				if (bip.getArgument(2) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(2));
					if (variableDataTypeAssignments.containsKey(varg.getVariableName())) {
						varg.setDataType(variableDataTypeAssignments.get(varg.getVariableName()));
						status = true;
					}
				}
			} else if (bip.getPredicateName().equals(BuiltInPredicate.FUNCTOR_PREDICATE_NAME)) {
				if (bip.getArgument(0) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(0));
					varg.setDataType(DataType.COMPLEX);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.COMPLEX);
				}
				
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.COMPLEX);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.COMPLEX);
				}
				
				if (bip.getArgument(2) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(2));
					varg.setDataType(DataType.LIST);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.LIST);
				}
			} else if (bip.getPredicateName().equals(BuiltInPredicate.GET_DATE_PREDICATE_NAME)) {
				if (bip.getArgument(0) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(0));
					varg.setDataType(DataType.DATETIME);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.DATETIME);
				}
			} else if (bip.getPredicateName().equals(BuiltInPredicate.DATE_PART_PREDICATE_NAME)) {
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.DATETIME);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.DATETIME);
				}
				
				if (bip.getArgument(2) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(2));
					varg.setDataType(DataType.INT);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.INT);
				}
			} else if (bip.getPredicateName().equals(BuiltInPredicate.DATE_ADD_PREDICATE_NAME)) {
				if (bip.getArgument(2) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(2));
					varg.setDataType(DataType.DATETIME);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.DATETIME);
				}
				
				if (bip.getArgument(3) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(3));
					varg.setDataType(DataType.DATETIME);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.DATETIME);
				}
			} else if (bip.getPredicateName().equals(BuiltInPredicate.DATE_DIFF_PREDICATE_NAME)) {
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.DATETIME);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.DATETIME);
				}
				
				if (bip.getArgument(2) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(2));
					varg.setDataType(DataType.DATETIME);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.DATETIME);
				}
				
				if (bip.getArgument(3) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(3));
					varg.setDataType(DataType.INT);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.INT);
				}
			} else if (bip.getPredicateName().equals(BuiltInPredicate.SUB_STRING_PREDICATE_NAME)) {
				if (bip.getArgument(0) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(0));
					varg.setDataType(DataType.STRING);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.STRING);
				}
				
				if (bip.getArgument(1) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(1));
					varg.setDataType(DataType.INT);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.INT);
				}
				
				if (bip.getArgument(2) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(2));
					varg.setDataType(DataType.INT);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.INT);
				}
				
				if (bip.getArgument(3) instanceof CompilerVariable) {
					varg = ((CompilerVariable)bip.getArgument(3));
					varg.setDataType(DataType.STRING);
					variableDataTypeAssignments.put(varg.getVariableName(), DataType.STRING);
				}
			}
		} else if (bip.isFalse() || bip.isTrue()) {
			status = true;
		} else if (bip.isSingle() || bip.isMulti() || bip.isReturn()) {
			status = assignDerived(module, bip, variableDataTypeAssignments, predicates);
		} else if (bip.getPredicateName().equals(BuiltInPredicate.LIMIT_PREDICATE_NAME)) {
			CompilerTypeBase arg = bip.getArgument(0);
			if (arg.isConstant()) {
				status = true;
			} else if (arg.isVariable()) {
				((CompilerVariable)arg).setDataType(DataType.INT);				
				status = true;
			}		
		} else {
			status = false;
		}
		
		return status;
	}

	private static boolean assignBinary(BuiltInPredicate binary, HashMap<String, DataType> variableDataTypeAssignments) {
		CompilerTypeBase arg0 = binary.getArgument(0);
		CompilerTypeBase arg1 = binary.getArgument(1);
		DataType arg0DataType = arg0.getDataType();
		DataType arg1DataType = arg1.getDataType();
		
		boolean arg0Assigned = (arg0.isConstant() || (arg0.getDataType() != DataType.UNKNOWN));
		boolean arg1Assigned = (arg1.isConstant() || (arg1.getDataType() != DataType.UNKNOWN));

		if (!arg0Assigned) {
			arg0DataType = inferTermDataType(arg0);
			arg0Assigned = (arg0DataType != DataType.UNKNOWN);
		}
		
		if (!arg1Assigned) {
			arg1DataType = inferTermDataType(arg1);
			arg1Assigned = (arg1DataType != DataType.UNKNOWN);
		}
		
		// if variables, check if we already have them typed
		if (arg0.isVariable() && !arg0Assigned) {
			CompilerVariable varg = ((CompilerVariable)arg0);
			if (variableDataTypeAssignments.containsKey(varg.getVariableName())) {
				varg.setDataType(variableDataTypeAssignments.get(varg.getVariableName()));
				arg0Assigned = true;
				arg0DataType = varg.getDataType();
			}
		}
		
		if (arg1.isVariable() && !arg1Assigned) {
			CompilerVariable varg = ((CompilerVariable)arg1);
			if (variableDataTypeAssignments.containsKey(varg.getVariableName())) {
				varg.setDataType(variableDataTypeAssignments.get(varg.getVariableName()));
				arg1Assigned = true;
				arg1DataType = varg.getDataType();
			}
		}
		
		if (arg0.isVariable() && !arg0Assigned && arg1Assigned) {
			((CompilerVariable)arg0).setDataType(arg1.getDataType());
			arg0Assigned = true;
			arg0DataType = arg1DataType;
		}
		
		if (arg1.isVariable() && arg0Assigned && !arg1Assigned) {
			((CompilerVariable)arg1).setDataType(arg0.getDataType());
			arg1Assigned = true;
			arg1DataType = arg0DataType;
		}		

		if (arg0Assigned && arg1Assigned) {		
			// if unequal, but comparable data types, cast one to the other
			if ((arg0DataType != arg1DataType) && (arg0DataType.getOrder() > 0) && (arg1DataType.getOrder() > 0)) {
				if (arg0DataType.getOrder() > arg1DataType.getOrder()) {
					if (arg1.isConstant())
						binary.setArgument(1, DataType.cast(arg1, arg0DataType));
					else if (arg1.getType() == CompilerType.ARITHMETIC_EXPRESSION)
						binary.setArgument(1, CompilerTypeCast.create(arg1, arg0DataType));
				} else {
					if (arg0.isConstant())
						binary.setArgument(0, DataType.cast(arg0, arg1DataType));
					else if (arg0.getType() == CompilerType.ARITHMETIC_EXPRESSION)
						binary.setArgument(0, CompilerTypeCast.create(arg0, arg1DataType));
				}
			}
		}
		
		return true;
	}
	
	private void promoteNumericVariableTypeAssignments(Module module, List<Predicate> assignedPredicates) {
		// to promote the numeric data types in between goals with two or more rules, we do the following:
		// 1) for each goal, see if it have more than one rule
		// 2) if so, find the best matching predicate based on the one with the larger numeric data typed arguments
		// 3) reassign the arguments in the goal to this predicate (if the goal's data types aren't larger)

		// loop until no new heads or goals promoted
		while (true) {
			List<Predicate> headsWithPromotedTypes = new ArrayList<>();
			List<Predicate> goalsWithPromotedTypes = new ArrayList<>();
			
			for (DerivedPredicate derivedPredicate : module.getDerivedPredicates()) {	
				for (Rule rule : derivedPredicate.getRules()) {
					for (Predicate goal : rule.getBody()) {
						if (TypeInferrer.isPredicateAssigned(goal)) {
							Predicate bestPredicate = TypeInferrer.getMatchingDerivedPredicate(assignedPredicates, goal.getPredicateName(), goal.getArity());
							if (bestPredicate == null)
								continue;
							if (bestPredicate == goal)
								continue;
							if (TypeInferrer.promoteNumericDataTypesFromBetterPredicate(goal, bestPredicate)) {				
								if (!goalsWithPromotedTypes.contains(goal))
									goalsWithPromotedTypes.add(goal);
							}
						}
					}
					
					if (TypeInferrer.isPredicateAssigned(rule.getHead())) {
						Predicate bestPredicate = TypeInferrer.getMatchingDerivedPredicate(assignedPredicates, 
								rule.getHead().getPredicateName(), rule.getHead().getArity());
						if (bestPredicate == null)
							continue;
						if (bestPredicate == rule.getHead())
							continue;
						if (TypeInferrer.promoteNumericDataTypesFromBetterPredicate(rule.getHead(), bestPredicate)) {				
							if (!headsWithPromotedTypes.contains(rule.getHead()))
								headsWithPromotedTypes.add(rule.getHead());
						}
						// switcherrooo!
						if (TypeInferrer.promoteNumericDataTypesFromBetterPredicate(bestPredicate, rule.getHead())) {				
							if (!headsWithPromotedTypes.contains(bestPredicate))
								headsWithPromotedTypes.add(bestPredicate);
						}
					}
				}
			}
		
			if (this.deALSContext.isInfoEnabled()) {
				if (headsWithPromotedTypes.size() > 0 || goalsWithPromotedTypes.size() > 0) {
					this.deALSContext.logInfo(logger, "\nThe following predicates had variable data type assignment(s) promoted to larger types:");
					
					for (Predicate predicate : headsWithPromotedTypes)
						this.deALSContext.logInfo(logger, "    {}", this.showPredicate(predicate, true));
					
					for (Predicate predicate : goalsWithPromotedTypes)
						this.deALSContext.logInfo(logger, "    {}", this.showPredicate(predicate, false));
				}
			}
			
			if (headsWithPromotedTypes.size() == 0 && goalsWithPromotedTypes.size() == 0)
				break;
		}
	}
	
	private static boolean assignUnassignedBinaryPredicateVariablesToDouble(List<Predicate> partiallyAssignedPredicates) {
		boolean retval1 = false;
		boolean retval2 = false;

		for (Predicate partiallyAssignedPredicate : partiallyAssignedPredicates) {
			if (partiallyAssignedPredicate.isBuiltIn()) {
				BuiltInPredicate bip = (BuiltInPredicate)partiallyAssignedPredicate;
				//if (bip.isBinary() && !TypeInferrer.isPredicateAssigned(bip)) {
				if (bip.isBinary() && !TypeInferrer.isPredicateAssigned(bip)) {
					CompilerTypeBase arg0 = bip.getArgument(0);
					CompilerTypeBase arg1 = bip.getArgument(1);

					// we want to assign the variable in a functor, if there is one
					// if there isn't one, then just assign all the variables to floats
					if (arg0.isFunctor() || arg1.isFunctor()) {
						if (arg0.isFunctor())
							if (TypeInferrer.assignUnassignedFunctorVariablesToDouble((CompilerFunctor)arg0))
								retval1 = true;
					
						if (arg1.isFunctor())
							if (TypeInferrer.assignUnassignedFunctorVariablesToDouble((CompilerFunctor)arg1))
								retval2 = true;
					} else {
						if (arg0.isVariable() && (arg0.getDataType() == DataType.UNKNOWN)) {
							((CompilerVariable)arg0).setDataType(DataType.DOUBLE);
							retval1 = true;
						}							
						
						if (arg1.isVariable() && (arg1.getDataType() == DataType.UNKNOWN)) {
							((CompilerVariable)arg1).setDataType(DataType.DOUBLE);
							retval2 = true;
						}
					}
				}
			}
		}
		
		return (retval1 || retval2);
	}
	
	private static boolean assignUnassignedFunctorVariablesToDouble(CompilerFunctor func) {
		boolean retval = false;

		for (int i = 0; i < func.getArity(); i++) {
			if (func.getArgument(i).isFunctor()) {
				if (assignUnassignedFunctorVariablesToDouble((CompilerFunctor)func.getArgument(i)))
					retval = true;
			} else if (func.getArgument(i).isVariable()) {
				if (func.getArgument(i).getDataType() == DataType.UNKNOWN) {
					((CompilerVariable)func.getArgument(i)).setDataType(DataType.DOUBLE);
					retval = true;
				}
			}
		}
		
		return retval;
	}
	
	private static boolean promoteNumericDataTypesFromBetterPredicate(Predicate predicate, Predicate betterPredicate) {
		boolean promoted = false;
		for (int i = 0; i < predicate.getArity(); i++) {
			if (betterPredicate.getArgument(i).isVariable()) {
				CompilerVariable mpVar = ((CompilerVariable)betterPredicate.getArgument(i));
				if (predicate.getArgument(i).isVariable()) {
					CompilerVariable predVar = ((CompilerVariable)predicate.getArgument(i));
					if (mpVar.getDataType() == predVar.getDataType())
						continue;
					
					if (DataType.isNumeric(mpVar.getDataType())) {
						TypeInferrer.transferType(predVar, mpVar, true);
						promoted = true;
						continue;
						//break;
					}
				} else if (predicate.getArgument(i).isAnyAggregate()) {
					Aggregate predAgg = ((Aggregate)predicate.getArgument(i));
					if (mpVar.getDataType() == predAgg.getReturnDataType())
						continue;
					
					predAgg.setReturnDataType(mpVar.getDataType());
					promoted = true;
					continue;
					//break;	
				} else if (predicate.getArgument(i).isConstant()) {
					CompilerTypeBase predVar = predicate.getArgument(i);
					if (predVar.getDataType() != mpVar.getDataType())
						predicate.setArgument(i, DataType.cast(predVar, mpVar.getDataType()));
				}
			} else if (betterPredicate.getArgument(i).isAnyAggregate()) {
				Aggregate mpAgg = ((Aggregate)betterPredicate.getArgument(i));
				if (predicate.getArgument(i).isAnyAggregate()) {					
					Aggregate predAgg = ((Aggregate)predicate.getArgument(i));
					if (mpAgg.getReturnDataType() == predAgg.getReturnDataType())
						continue;
					
					predAgg.setReturnDataType(mpAgg.getReturnDataType());
					promoted = true;
					//break;
				} else if (predicate.getArgument(i).isVariable()) {
					CompilerVariable predVar = ((CompilerVariable)predicate.getArgument(i));
					if (mpAgg.getReturnDataType() == predVar.getDataType())
						continue;

					predVar.setDataType(mpAgg.getReturnDataType());
					promoted = true;
					//break;
				} else if (predicate.getArgument(i).isConstant()) {
					CompilerTypeBase predVar = predicate.getArgument(i);
					if (predVar.getDataType() != mpAgg.getReturnDataType())
						predicate.setArgument(i, DataType.cast(predVar, mpAgg.getReturnDataType()));
				}
			}
		}
		return promoted;
	}
	
	private static boolean isRuleAssigned(Rule rule) {
		return TypeInferrer.isPredicateAssigned(rule.getHead()) && TypeInferrer.isBodyAssigned(rule.getBody());
	}
	
	private static boolean isPredicateAssigned(Predicate predicate) {
		CompilerTypeList argumentList = new CompilerTypeList();
		TypeInferrer.collectTypeableArguments(predicate, argumentList);
		TypeInferrer.removeAssignedArguments(argumentList, null);
		return (argumentList.size() == 0);
	}

	private static boolean isBodyAssigned(List<Predicate> body) {
		for (Predicate literal : body)
			if (!isPredicateAssigned(literal))
				return false;
		return true;
	}
		
	private static CompilerTypeList collectTypeableArguments(Predicate predicate, CompilerTypeList argumentList) {				
		for (CompilerTypeBase argument : predicate.getArguments()) {
			if (argument.isVariable()) {
				argumentList.addUnique(argument);
			} else if (argument.isAnyAggregate()){
				if (((Aggregate)argument).getAggregateTerm().isVariable()) {
					argumentList.addUnique(((Aggregate)argument).getAggregateTerm());
				} else {
					if (((Aggregate)argument).getAggregateTerm().isFunctor()) {
						CompilerFunctor func = (CompilerFunctor)((Aggregate)argument).getAggregateTerm();
						for (CompilerTypeBase funcArg : func.getArguments())
							if (funcArg.isVariable())
								argumentList.addUnique(funcArg);
					}
				}
			} else if (argument.isFunctor()) {
				CompilerFunctor func = (CompilerFunctor)argument;
				for (CompilerTypeBase funcArg : func.getArguments())
					if (funcArg.isVariable())
						argumentList.addUnique(funcArg);
			} else if (argument.isList()) {
				CompilerList list = (CompilerList)argument;
				if (!list.isEmpty()) {
					if (list.getHead().isVariable())
						argumentList.addUnique(list.getHead());
					if (list.getTail() != null && list.getTail().isList() && list.getTail().getHead().isVariable())
						argumentList.addUnique(list.getTail().getHead());							
				}
			} else if (argument.getType() == CompilerType.ARITHMETIC_EXPRESSION) {
				CompilerVariableList variableList = new CompilerVariableList();
				Utilities.getVariables(argument, variableList);
				argumentList.appendList(variableList.toCompilerTypeList());
				argumentList.addUnique(argument);
			}
		}
		if (predicate.isBuiltIn()) {
			BuiltInPredicate bip = (BuiltInPredicate)predicate;

			switch (bip.getBuiltInPredicateType()) {
				case IFTHEN:
					for (Predicate literal : ((IfThenPredicate)bip).getIfLiterals())
						collectTypeableArguments(literal, argumentList);
					
					for (Predicate literal : ((IfThenPredicate)bip).getThenLiterals())
						collectTypeableArguments(literal, argumentList);
					break;
				case IFTHENELSE:
					for (Predicate literal : ((IfThenElsePredicate)bip).getIfLiterals())
						collectTypeableArguments(literal, argumentList);
					
					for (Predicate literal : ((IfThenElsePredicate)bip).getThenLiterals())
						collectTypeableArguments(literal, argumentList);
					
					for (Predicate literal : ((IfThenElsePredicate)bip).getElseLiterals())
						collectTypeableArguments(literal, argumentList);
					break;
			}
		}
		
		
		return argumentList;
	}

	private boolean setAggregatesReturnType(Predicate head, List<Predicate> predicates) {
		boolean allAssigned = true;
		for (CompilerTypeBase arg : head.getArguments()) {
			if (arg.isAnyAggregate()) {
				Aggregate aggregate = (Aggregate)arg;
				if (aggregate.getReturnDataType() == DataType.UNKNOWN) {
					DataType dt = this.inferAggregateDataType(aggregate, predicates);
					if (dt == DataType.UNKNOWN)
						allAssigned = false;
					aggregate.setReturnDataType(dt);
				}
			}
		}
		return allAssigned;
	}

	private static void transferType(CompilerTypeBase to, CompilerTypeBase from) {
		transferType(to, from, false);
	}
	
	private static void transferType(CompilerTypeBase to, CompilerTypeBase from, boolean reassign) { 
		if (to == null || from == null)
			return;
		
		if (to.isConstant())
			return;
		// assign the data type if it is not already assigned or we can override to reassign
		if (to.isVariable() && ((to.getDataType() == DataType.UNKNOWN) || reassign)) {
			if (from.isVariable() || from.isConstant())
				((CompilerVariable)to).setDataType(from.getDataType());
			else if (from.isFunctor() || from.isList() || from.isExpression())
				((CompilerVariable)to).setDataType(TypeInferrer.inferTermDataType(from));
		} else if (to.isFunctor()) {
			// we can only transfer to a functor, from a functor
			if (from.isFunctor()) {
				CompilerFunctor fromFunctor = (CompilerFunctor)from;
				CompilerFunctor toFunctor = (CompilerFunctor)to;
				for (int i = 0; i < fromFunctor.getArity(); i++)
					transferType(toFunctor.getArgument(i), fromFunctor.getArgument(i));
			}
		} else if(to.isList()) {
			if (from.isList()) {
				CompilerList fromList = (CompilerList)from;
				CompilerList toList = (CompilerList)to;
				transferType(toList.getHead(), fromList.getHead());
				transferType(toList.getTail(), fromList.getTail());
			}
		} else if ((to.getType() == CompilerType.ARITHMETIC_EXPRESSION) && ((to.getDataType() == DataType.UNKNOWN) || reassign)) {
			((CompilerArithmeticExpression)to).setDataType(from.getDataType());
		}
	}
	
	private static Predicate getMatchingDerivedPredicate(List<Predicate> predicates, String predicateName, int arity) {
		return getMatchingDerivedPredicate(null, predicates, predicateName, arity);
	}
	
	private static Predicate getMatchingDerivedPredicate(Predicate predicateToExclude, List<Predicate> predicates, 
			String predicateName, int arity) {
		// if we have two predicates with arguments with different data types assignments, 
		// we have to choose the 'larger' of the two's numeric types
		// long beats integer, float beats long, float beats integer
		List<Predicate> matchingPredicates = new ArrayList<>();
		for (Predicate predicate : predicates) {
			if (!predicate.isDerived())
				continue;
			if (predicate.getPredicateName().equals(predicateName) && (predicate.getArity() == arity)) {
				if (predicateToExclude != null)
					if (predicateToExclude == predicate)
						continue;
				matchingPredicates.add(predicate);
			}
		}
		
		if (matchingPredicates.size() == 0)
			return null;
		
		if (matchingPredicates.size() == 1)
			return matchingPredicates.get(0);
		
		Predicate bestPredicate = matchingPredicates.get(0);
		for (int i = 1; i < matchingPredicates.size(); i++) {
			for (int j = 0; j < arity; j++) {
				if (bestPredicate.getArgument(j).getType() == matchingPredicates.get(i).getArgument(j).getType()) {
					if (bestPredicate.getArgument(j).isVariable()) {
						CompilerVariable bpVar = ((CompilerVariable)bestPredicate.getArgument(j));
						CompilerVariable mpVar = ((CompilerVariable)matchingPredicates.get(i).getArgument(j));
						if (bpVar.getDataType() == mpVar.getDataType())
							continue;
						
						if (DataType.isNumeric(bpVar.getDataType()) && (mpVar.getDataType().getOrder() > 0)) {
							if (mpVar.getDataType().getOrder() > bpVar.getDataType().getOrder()) {
								bestPredicate = matchingPredicates.get(i);
								break;
							}
						}
					}
				}
					
				if (bestPredicate.getArgument(j).isNumeric()) {					
					DataType bestPredicateDataType = bestPredicate.getArgument(j).getDataType();
					DataType matchingPredicateDataType = matchingPredicates.get(i).getArgument(j).getDataType();
					
					if (matchingPredicateDataType.getOrder() > bestPredicateDataType.getOrder()) {
						bestPredicate = matchingPredicates.get(i);
						break;
					} else if (matchingPredicates.get(i).getArgument(j).isAnyAggregate() 
							&& ((Aggregate)matchingPredicates.get(i).getArgument(j)).getReturnDataType().getOrder() > bestPredicateDataType.getOrder()) {
						bestPredicate = matchingPredicates.get(i);
						break;
					}
				} else if (bestPredicate.getArgument(j).isAnyAggregate()) {
					DataType bestPredicateDataType = ((Aggregate)bestPredicate.getArgument(j)).getReturnDataType();
					DataType matchingPredicateDataType = DataType.UNKNOWN;
					if (matchingPredicates.get(i).getArgument(j).isAnyAggregate())
						matchingPredicateDataType = ((Aggregate)matchingPredicates.get(i).getArgument(j)).getReturnDataType();
					else
						matchingPredicateDataType = TypeInferrer.inferTermDataType(matchingPredicates.get(i).getArgument(j));
					
					if (DataType.isNumeric(bestPredicateDataType) && DataType.isNumeric(matchingPredicateDataType))
						if (matchingPredicateDataType.getOrder() > bestPredicateDataType.getOrder())
							bestPredicate = matchingPredicates.get(i);
				}
			}
		}
		
		return bestPredicate;
	}
	
	private static List<Predicate> getMatchingPredicate(List<Predicate> predicates, String predicateName, int arity, Predicate exclude) {
		List<Predicate> matches = new ArrayList<>();
		for (Predicate predicate : predicates) {
			if (predicate.getPredicateName().equals(predicateName) && (predicate.getArity() == arity)) 
				if (predicate != exclude)
					matches.add(predicate);
		}
		return matches;
	}

	public DataType inferAggregateDataType(Aggregate aggregate, List<Predicate> assignedPredicates) {
		// we can only return INT or REAL - UNKNOWN is also possible, but not helpful
		// if term data type is unknown, aggregate data type is unknown
		DataType dataType = DataType.UNKNOWN;
		if (aggregate instanceof FSAggregate) {
			FSAggregate cagg = (FSAggregate)aggregate;
			switch (cagg.getFSAggregateType()) {
			case FSMAX:
			case FSMIN:
				// fsmax/fsmin term is single argument - should be a variable, but u never know
				CompilerTypeBase aggTerm = cagg.getAggregateTerm();
				dataType = inferTermDataType(aggTerm);
				
				if (!(DataType.isNumeric(dataType)))
					dataType = DataType.UNKNOWN;
				
				break;
			case FSCNT:
				// this might be a single variable or a functor
				if (cagg.getAggregateTerm().isVariable()) {
					CompilerVariable varg = (CompilerVariable)cagg.getAggregateTerm();
					// if string, we're counting integers
					if ((varg.getDataType() == DataType.STRING) || (varg.getDataType() == DataType.INT))
						dataType = this.deALSContext.getConfiguration().getCountDataType();
					else
						dataType = varg.getDataType(); // for floating points
				} else {
					//if not a variable, it is a functor
					CompilerFunctor functor = (CompilerFunctor)cagg.getAggregateTerm();
					// we only care about 2nd functor for purpose of variable type
					// if we don't have a functor, the 2nd arguments's type is the aggregate's type 
					if (!functor.getArgument(1).isFunctor()) {
						dataType = TypeInferrer.inferTermDataType(functor.getArgument(1));
						if (dataType == DataType.STRING || dataType == DataType.INT)
							dataType = this.deALSContext.getConfiguration().getCountDataType();
						// we can only return numeric data types
						if (!(DataType.isNumeric(dataType)))
							dataType = DataType.UNKNOWN;
					} else {
						// if there is a functor, we expect to see multiple entries
						// we have to check them all until one is not an integer
						// only if all are integer, are we dealing with an integer count
					 	functor = (CompilerFunctor)functor.getArgument(1);
					 	DataType dt = DataType.UNKNOWN;
					 	for (CompilerTypeBase arg : functor.getArguments()) {
					 		dt = TypeInferrer.inferTermDataType(arg);
					 		if (dt == DataType.UNKNOWN)
					 			continue;
					 		if (dt == DataType.DOUBLE)
					 			break;
					 	}

					 	// if we found the answer, return it
					 	// if not, default aggregate to real
					 	if (dt != DataType.UNKNOWN)
					 		dataType = dt;
					 	else
					 		dataType = DataType.DOUBLE;					 	
					}
				}
					
				break;
			default:
				return DataType.UNKNOWN;
			}
		} else 	if (aggregate instanceof BuiltInAggregate) {
			BuiltInAggregate biagg = (BuiltInAggregate)aggregate;
			CompilerTypeBase aggTerm = biagg.getAggregateTerm();
			
			switch (biagg.getBuiltInAggregateType()) {
			case MAX:
			case MIN:		
				dataType = inferTermDataType(aggTerm);
				
				if (!(DataType.isNumeric(dataType)))
					dataType = DataType.UNKNOWN;
				
				break;
			case SUM:
				dataType = inferTermDataType(aggTerm);
				
				if (!DataType.isNumeric(dataType))
					dataType = DataType.UNKNOWN;
				/* APS 5/30/15 - set sum to be same datatype as input 
				if (DataType.isNumeric(dataType)) {
					if (!DataType.isDecimal(dataType))
						dataType = DataType.getNextHigherOrderType(dataType); // to prevent overflows
				} else {
					dataType = DataType.UNKNOWN;
				}*/				
				
				break;
			case AVG:
				dataType = DataType.DOUBLE;
				break;
			case COUNT:
			case COUNT_DISTINCT:
				dataType = this.deALSContext.getConfiguration().getCountDataType();
				break;
			}
		} else {
			// UDA - get multi rule head and use last variable's type
			for (Predicate predicate : assignedPredicates) {
				if (predicate.getPredicateName().equals(BuiltInPredicate.MULTI_PREDICATE_NAME) 
						&& (predicate.getArity() == MULTI_RULE_ARITY)) {
					if (((CompilerString)predicate.getArgument(0)).getText().equals(aggregate.getAggregateName())) {
						dataType = TypeInferrer.inferTermDataType(predicate.getArgument(MULTI_RULE_RETURN_ARGUMENT_POSITION));
						break;
					}
				}
			}
		}
		
		return dataType;
	}
	
	public static DataType inferTermDataType(CompilerTypeBase term) {
		if (term.isConstant() || term.isVariable() || term.isInputVariable() || term.isCast() || term.isList() || term.isFunctor())
			return term.getDataType();
			
		DataType dataType = DataType.UNKNOWN;
		if ((term.getType() == CompilerType.ARITHMETIC_EXPRESSION) && ((CompilerArithmeticExpression)term).isBinary()) {
			CompilerArithmeticExpression expr = (CompilerArithmeticExpression)term;	
			DataType leftDataType = TypeInferrer.inferTermDataType(expr.getArgument1());
			DataType rightDataType = TypeInferrer.inferTermDataType(expr.getArgument2());

			// if both arguments have been assigned types, but the types aren't the same
			// and the lesser one is a constant, upgrade it
			if ((leftDataType.getOrder() > 0) && (rightDataType.getOrder() > 0)
					&& (leftDataType.getOrder() != rightDataType.getOrder())) {
				if (leftDataType.getOrder() > rightDataType.getOrder()) {
					if (expr.getArgument2().isConstant()) {
						expr.setArgument2(DataType.cast(expr.getArgument2(), leftDataType));
						rightDataType = leftDataType;
					}
				} else {
					if (expr.getArgument1().isConstant()) {
						expr.setArgument1(DataType.cast(expr.getArgument1(), rightDataType));
						leftDataType = rightDataType;
					}
				}
			}
				
			switch (expr.getOperation()) {
				case ADDITION:
				case SUBTRACTION:
				case MULTIPLICATION:
					if (leftDataType == DataType.UNKNOWN && rightDataType == DataType.UNKNOWN) {
						dataType = DataType.UNKNOWN;
					} else if ((leftDataType == DataType.DOUBLE && rightDataType == DataType.UNKNOWN) 
							|| (leftDataType == DataType.UNKNOWN && rightDataType == DataType.DOUBLE)) {
						dataType = DataType.DOUBLE;
					} else if ((leftDataType == DataType.FLOAT && rightDataType == DataType.UNKNOWN) 
							|| (leftDataType == DataType.UNKNOWN && rightDataType == DataType.FLOAT)) {
						dataType = DataType.FLOAT;
					} else {
						dataType = DataType.BYTE;
						if (leftDataType == DataType.DOUBLE || rightDataType == DataType.DOUBLE)
							dataType = DataType.DOUBLE;
						else if (leftDataType == DataType.FLOAT || rightDataType == DataType.FLOAT)
							dataType = DataType.FLOAT;
						else if (leftDataType == DataType.LONGLONGLONGLONG || rightDataType == DataType.LONGLONGLONGLONG)
							dataType = DataType.LONGLONGLONGLONG;
						else if (leftDataType == DataType.LONGLONG || rightDataType == DataType.LONGLONG)
							dataType = DataType.LONGLONG;
						else if (leftDataType == DataType.LONG || rightDataType == DataType.LONG)
							dataType = DataType.LONG;
						else if (leftDataType == DataType.INT || rightDataType == DataType.INT)
							dataType = DataType.INT;
						else if (leftDataType == DataType.SHORT || rightDataType == DataType.SHORT)
							dataType = DataType.SHORT;
					}
					break;
				case DIVISION:
					//dataType = DataType.UNKNOWN;
					//if (leftDataType != DataType.UNKNOWN && rightDataType != DataType.UNKNOWN)
						dataType = DataType.DOUBLE;
					break;
				case INTEGER_DIVISION:
					dataType = DataType.INT;
					break;
				case MOD:
				case OPC:
					dataType = DataType.BYTE;
					if (leftDataType == DataType.INT || rightDataType == DataType.INT)
						dataType = DataType.INT;
					else if (leftDataType == DataType.SHORT || rightDataType == DataType.SHORT)
						dataType = DataType.SHORT;					
					break;
			}

			// if resulting datatype is known, add casts to those that don't match the resulting datatype of the operation
			if (dataType.getOrder() > 0) {
				if (dataType != leftDataType) {
					if (expr.getArgument1().isConstant())
						expr.setArgument1(DataType.cast(expr.getArgument1(), dataType));
					else if ((leftDataType == DataType.UNKNOWN) && expr.getArgument1().isVariable())
						((CompilerVariable)expr.getArgument1()).setDataType(dataType);
					else if ((leftDataType == DataType.UNKNOWN) && expr.getArgument1().isInputVariable())
						((CompilerInputVariable)expr.getArgument1()).setDataType(dataType);
					else
						expr.setArgument1(CompilerTypeCast.create(expr.getArgument1(), dataType));
				}

				if (dataType != rightDataType) {
					if (expr.getArgument2().isConstant())
						expr.setArgument2(DataType.cast(expr.getArgument2(), dataType));
					else if ((rightDataType == DataType.UNKNOWN) && expr.getArgument2().isVariable())
						((CompilerVariable)expr.getArgument2()).setDataType(dataType);
					else if ((rightDataType == DataType.UNKNOWN) && expr.getArgument2().isInputVariable())
						((CompilerInputVariable)expr.getArgument2()).setDataType(dataType);
					else
						expr.setArgument2(CompilerTypeCast.create(expr.getArgument2(), dataType));							
				}
				
				expr.setDataType(dataType);
			}
		} else if ((term.getType() == CompilerType.ARITHMETIC_EXPRESSION) && ((CompilerArithmeticExpression)term).isUnary()) {
			CompilerArithmeticExpression expr = (CompilerArithmeticExpression)term;
			if (expr.getOperation() == ArithmeticOperation.LOG) {
				dataType = DataType.DOUBLE;
				expr.setDataType(dataType);
			}
			else if (expr.getOperation() == ArithmeticOperation.EXP) {
                dataType = DataType.DOUBLE;
                expr.setDataType(dataType);
            }
            else if (expr.getOperation() == ArithmeticOperation.STEP) {
                dataType = DataType.INT;
                expr.setDataType(dataType);
            }
		}

		return dataType;
	}
	/*
	public static DataType inferTermDataType(Argument term) {
		if ((term instanceof Variable) || (term instanceof InputVariable) || term.isConstant() 
				|| (term instanceof InterpreterList) || (term instanceof InterpreterFunctor)) 
			return term.getDataType();
		
		DataType dataType = DataType.UNKNOWN; 
		if (term instanceof BinaryExpression) {
			BinaryExpression expr = (BinaryExpression)term;
			DataType arg0DataType = TypeInferrer.inferTermDataType(expr.getLeft());
			DataType arg1DataType = TypeInferrer.inferTermDataType(expr.getRight());
				
			switch (expr.getOperation()) {
				case ADDITION:
				case SUBTRACTION:
				case MULTIPLICATION:
					if (arg0DataType == DataType.UNKNOWN && arg1DataType == DataType.UNKNOWN) {
						dataType = DataType.UNKNOWN;
					} else if ((arg0DataType == DataType.FLOAT && arg1DataType == DataType.UNKNOWN) 
							|| (arg0DataType == DataType.UNKNOWN && arg1DataType == DataType.FLOAT)) {
						dataType = DataType.FLOAT;
					} else {
						dataType = DataType.INT;
						if (arg0DataType == DataType.FLOAT || arg1DataType == DataType.FLOAT)
							dataType = DataType.FLOAT;
						else if (arg0DataType == DataType.LONGLONGLONGLONG || arg1DataType == DataType.LONGLONGLONGLONG)
							dataType = DataType.LONGLONGLONGLONG;
						else if (arg0DataType == DataType.LONGLONG || arg1DataType == DataType.LONGLONG)
							dataType = DataType.LONGLONG;
						else if (arg0DataType == DataType.LONG || arg1DataType == DataType.LONG)
							dataType = DataType.LONG;
					}
					break;
				case DIVISION:
					if (arg0DataType == DataType.UNKNOWN || arg1DataType == DataType.UNKNOWN)
						dataType = DataType.UNKNOWN;
					else
						dataType = DataType.FLOAT;
					break;
				case MOD:
					dataType = DataType.INT;
					break;
			}
		}
		
		if (term instanceof UnaryExpression) {
			switch (((UnaryExpression) term).getOperation()) {
				case LOG:
					dataType = DataType.FLOAT;
					break;
				case EXP:
					dataType = DataType.FLOAT;
					break;
				case STEP:
					dataType = DataType.INTEGER;
					break;
			}
		}
		
		return dataType;
	}
	*/
	public static DataType[] getQueryFormSchema(QueryForm queryForm, Module module) {
		DataType[] dataTypes = new DataType[queryForm.getArity()];
		DerivedPredicate predicate = getDerivedPredicate(module, queryForm);
		
		if (predicate != null && predicate.getRules().size() > 0) {
			// all rules should have same typing information, so take the 1st
			Rule rule = getBestRule(predicate);
			Predicate head = rule.getHead();
			for (int i = 0; i < head.getArity(); i++)
				dataTypes[i] = TypeInferrer.inferTermDataType(head.getArgument(i));
			
		} else {
			BasePredicate sr = module.getBasePredicate(queryForm.getPredicateName());
			if (sr == null)
				throw new CompilerException("Can not execute query form " + queryForm.toString() + "  Unable to find predicate for query form.");

			dataTypes = sr.getSchema();
		}
		
		return dataTypes;
	}
	
	private static Rule getBestRule(DerivedPredicate derivedPredicate) {
		Rule rule = derivedPredicate.getRule(0);
		Rule candidate;
		for (int l = 1; l < derivedPredicate.getNumberOfRules(); l++) {
			candidate = derivedPredicate.getRule(l);
			
			for (int i = 0; i < rule.getHead().getArity(); i++) {
				if (candidate.getHead().getArgument(i).isVariable()) {
					CompilerVariable cVar = ((CompilerVariable)candidate.getHead().getArgument(i));
					if (rule.getHead().getArgument(i).isVariable()) {
						CompilerVariable predVar = ((CompilerVariable)rule.getHead().getArgument(i));
						if (cVar.getDataType() == predVar.getDataType())
							continue;
						
						if (DataType.isNumeric(cVar.getDataType())) {
							if (cVar.getDataType() != predVar.getDataType()) {
								rule = candidate;
								break;
							}
						}
					} else if (rule.getHead().getArgument(i).isConstant()) {
						CompilerTypeBase predVar = rule.getHead().getArgument(i);
						if (cVar.getDataType() == DataType.LONGLONGLONGLONG)
							rule = candidate;						
						else if (predVar.getDataType() != cVar.getDataType())
							rule.getHead().setArgument(i, DataType.cast(predVar, cVar.getDataType()));
						
					} else if (rule.getHead().getArgument(i).isNil()) {
						if (!candidate.getHead().getArgument(i).isNil())
							rule = candidate;
					}
				}
			}		
		}
		return rule;
	}

	public static DataType[] getSchema(NodeArguments arguments) {
		DataType[] schema = new DataType[arguments.size()];
		DataType dt;
		for (int i = 0; i < arguments.size(); i++) {
			dt = arguments.get(i).getDataType();
			// at any point we don't know a type, we fail and use the default setup w/ a null schema
			if (dt == DataType.UNKNOWN) {
				schema = null;
				break;
			}
			schema[i] = dt;
		}
		return schema;
	}
	
	public static boolean assignQueryForm(QueryForm queryForm, Module module) {
		// first check if we're calling a clique, only then check for derived predicate
		// there will not be a derived predicate if it is a clique
		DerivedPredicate derivedPredicate = getDerivedPredicate(module, queryForm);
				
		if (derivedPredicate != null) {
			Rule rule = derivedPredicate.getRule(0);	// all rules should have same typing
			for (int i = 0; i < queryForm.getArity(); i++) {
				if (queryForm.getArgument(i).isVariable()) {
					((CompilerVariable)queryForm.getArgument(i)).setDataType(inferTermDataType(rule.getHead().getArgument(i)));
				} else if (queryForm.getArgument(i).isInputVariable()) {
					DataType dataType = inferTermDataType(rule.getHead().getArgument(i));
					CompilerInputVariable inputVariable = (CompilerInputVariable)queryForm.getArgument(i);
					// cast the value if set and if numeric
					if ((inputVariable.getValue() != null)
							&& (inputVariable.getValue().getDataType() != dataType)
							&& DataType.isNumeric(dataType)
							&& DataType.isNumeric(inputVariable.getValue().getDataType()))
						inputVariable.setValue(DataType.cast(inputVariable.getValue(), dataType));

					inputVariable.setDataType(dataType);
				}
			}
			return true;
		}
		
		BasePredicate basePredicate = module.getBasePredicate(queryForm.getPredicateName());
		if (basePredicate != null) {			
			for (int i = 0; i < queryForm.getArity(); i++) {
				if (queryForm.getArgument(i).isVariable())
					((CompilerVariable)queryForm.getArgument(i)).setDataType(basePredicate.getBasePredicateStructuralAttribute(i).getDataType());
			}
			return true;
		}
		return false;
	}
	
	private static DerivedPredicate getDerivedPredicate(Module module, QueryForm queryForm) {
		DerivedPredicate predicate = null;
		BasicClique clique = module.getBasicClique(queryForm.getPredicateName(), queryForm.getArity());
		if (clique != null) {
			for (DerivedPredicate dr : clique.getDerivedPredicates()) {
				if (dr.getPredicateName().equals(queryForm.getPredicateName()) 
						&& dr.getArity() == queryForm.getArity()) {
					predicate = dr;
					break;
				}
			}
		} else {
			predicate = module.getDerivedPredicate(queryForm.getPredicateName(), queryForm.getArity());
		}
		
		return predicate;
	}
	
	private void sychronizePredicateSignatures(Module module, List<Predicate> assignedPredicates) {
		int count;
		while (true) {
			count = synchronizePredicateHeadSignatures(module, assignedPredicates);
			
			this.deALSContext.logInfo(logger, "{} predicate heads synchronized.", count);
			
			if (count == 0) break;
		}		
		
		count = synchronizePredicateLiteralSignatures(assignedPredicates);
		this.deALSContext.logInfo(logger, "{} predicate literals synchronized.", count);
	}
	
	private int synchronizePredicateHeadSignatures(Module module, List<Predicate> assignedPredicates) {
		int count = 0;
		for (DerivedPredicate derivedPredicate : module.getDerivedPredicates()) {	
			if (derivedPredicate.getPredicateName().equals(BuiltInPredicate.SINGLE_PREDICATE_NAME) || 
					derivedPredicate.getPredicateName().equals(BuiltInPredicate.MULTI_PREDICATE_NAME) || 
					derivedPredicate.getPredicateName().equals(BuiltInPredicate.RETURN_PREDICATE_NAME))
				continue;
			
			if (derivedPredicate.getNumberOfRules() > 1) {
				Rule rule1 = derivedPredicate.getRule(0);
				Rule rule2;
				for (int i = 1; i < derivedPredicate.getNumberOfRules(); i++) {
					rule2 = derivedPredicate.getRule(i);
					if (!this.hasMatchingAssignments(rule1.getHead(), rule2.getHead(), assignedPredicates)) {
						if (TypeInferrer.promoteNumericDataTypes(rule1.getHead(), rule2.getHead()))
							count++;

						// check again after numeric promotions
						if (this.hasMatchingAssignments(rule1.getHead(), rule2.getHead(), assignedPredicates))
							continue;
						
						Integer[] mismatches = TypeInferrer.getStringNumericMismatches(rule1.getHead(), rule2.getHead());
						if (mismatches != null) {
							if (!TypeInferrer.reconcileNumericStringMismatch(mismatches, rule1, rule2)) {
								this.deALSContext.logError(logger, "{}", this.showPredicate(rule1.getHead(), true));
								this.deALSContext.logError(logger, "{}", this.showPredicate(rule2.getHead(), true));
								throw new CompilerException("Rule head ("+rule1.getHead().getPredicateName()+") argument string type - numeric type mismatch.");
							}
							count++;
						}
						
						// check again after string promotions
						if (this.hasMatchingAssignments(rule1.getHead(), rule2.getHead(), assignedPredicates))
							continue;
						
						this.deALSContext.logError(logger, "{}", this.showPredicate(rule1.getHead(), true));
						this.deALSContext.logError(logger, "{}", this.showPredicate(rule2.getHead(), true));
						//throw new CompilerException("Rule head ("+rule1.getHead().getPredicateName()+") has unresolvable argument type mismatch.");
					}
				}
			}
		}
		
		return count;
	}
	
	private static int synchronizePredicateLiteralSignatures(List<Predicate> assignedPredicates) {
		int count = 0;
		Set<String> synchronizedPredicates = new HashSet<>();
		
		for (Predicate assignedPredicate : assignedPredicates) {
			if (assignedPredicate.isBuiltIn())	
				continue;
			
			if (!synchronizedPredicates.contains(assignedPredicate.getPredicateName() + "|" + assignedPredicate.getArity())) {
				List<Predicate> predicatesToSynchronize = new ArrayList<>();
				for (Predicate predicate : assignedPredicates) {
					if (predicate.getPredicateName().equals(assignedPredicate.getPredicateName())
							&& predicate.getArity() == assignedPredicate.getArity()) {
						boolean match = true;
						for (int i = 0; i < predicate.getArity(); i++) {
							if (predicate.getArgument(i).isAnyAggregate()) {
								match = false;
								break;
							}
						}
						
						if (match)
							predicatesToSynchronize.add(predicate);
					}
				}
				
				if (predicatesToSynchronize.size() > 1) {
					count += doSynchronizePredicateLiteralSignatures(predicatesToSynchronize);
					synchronizedPredicates.add(assignedPredicate.getPredicateName() + "|" + assignedPredicate.getArity());
				}
			}
		}
		return count;
	}
	
	private static int doSynchronizePredicateLiteralSignatures(List<Predicate> predicates) {
		int count = 0;
		// all predicate instances are of the same predicate
		DataType[] argumentDataTypes = new DataType[predicates.get(0).getArity()];
		
		// find the best set of argument data types for the predicate among all assigned data types
		for (int i = 0; i < argumentDataTypes.length; i++) {
			for (Predicate predicate : predicates) {
				DataType dataType = inferTermDataType(predicate.getArgument(i));
				
				if (dataType != null) {
					if (argumentDataTypes[i] == null)
						argumentDataTypes[i] = dataType;
					else if (argumentDataTypes[i].getOrder() < dataType.getOrder())
						if ((argumentDataTypes[i].getOrder() > 0) && (dataType.getOrder() > 0))
							argumentDataTypes[i] = dataType;
				}
			}
		}
		
		for (Predicate predicate : predicates) {
			for (int i = 0; i < argumentDataTypes.length; i++) {
				if (predicate.getArgument(i).isVariable()) {
					if (predicate.getArgument(i).getDataType() != argumentDataTypes[i]) {
						((CompilerVariable)predicate.getArgument(i)).setDataType(argumentDataTypes[i]);
						count++;
					}
				}
			}
		}
				
		return count;
	}
	
	private static boolean promoteNumericDataTypes(Predicate predicate1, Predicate predicate2) {
		boolean promoted = false;
		for (int i = 0; i < predicate1.getArity(); i++) {		
			if (promoteNumericDataTypes(predicate1.getArguments(), predicate1.getArgument(i), i, 
					predicate2.getArguments(), predicate2.getArgument(i), i))
				promoted = true;
		}
		return promoted;
	}
	
	private static boolean promoteNumericDataTypes(CompilerTypeList predicate1Arguments, CompilerTypeBase arg1, int arg1Index,
			CompilerTypeList predicate2Arguments, CompilerTypeBase arg2, int arg2Index) {
	
		if (arg1.isVariable()) {
			if (arg2.isVariable())		
				return TypeInferrer.promoteNumericDataTypeVariables((CompilerVariable)arg1, (CompilerVariable)arg2);
			else if (arg2.isConstant())
				return TypeInferrer.promoteNumericDataTypeVariableConstant((CompilerVariable) arg1, arg2, predicate2Arguments, arg2Index);
			else if (arg2.isAnyAggregate())
				return TypeInferrer.promoteNumericDataTypeVariableAggregate((CompilerVariable)arg1, (Aggregate)arg2);
		} else if (arg1.isAnyAggregate()) {
			if (arg2.isVariable())
				return TypeInferrer.promoteNumericDataTypeVariableAggregate((CompilerVariable)arg2, (Aggregate)arg1);
			else if (arg2.isConstant())
				return TypeInferrer.promoteNumericDataTypeConstantAggregate(arg2, predicate2Arguments, arg2Index, (Aggregate)arg1);
			else if (arg2.isAnyAggregate())
				return TypeInferrer.promoteNumericDataTypeAggregateAggregate((Aggregate)arg1, (Aggregate)arg2);
		} else if (arg1.isConstant()) {
			if (arg2.isVariable())
				return TypeInferrer.promoteNumericDataTypeVariableConstant((CompilerVariable) arg2, arg1, predicate1Arguments, arg1Index);
			else if (arg2.isConstant())
				return TypeInferrer.promoteNumericDataTypeConstantConstant(arg1, predicate1Arguments, arg1Index, arg2, predicate2Arguments, arg2Index);
			else if (arg2.isAnyAggregate())
				return TypeInferrer.promoteNumericDataTypeConstantAggregate(arg1, predicate1Arguments, arg1Index, (Aggregate)arg2);
		}
		
		return false;
	}
	
	private static boolean promoteNumericDataTypeVariables(CompilerVariable var1, CompilerVariable var2) {
		if (!DataType.isNumeric(var1.getDataType())) return false;
		if (!DataType.isNumeric(var2.getDataType())) return false;
		if (var1.getDataType() == var2.getDataType()) return false;
		
		if (var1.getDataType().getOrder() > var2.getDataType().getOrder())
			TypeInferrer.transferType(var2, var1, true);
		else
			TypeInferrer.transferType(var1, var2, true);			
		
		return true;		
	}
	
	private static boolean promoteNumericDataTypeVariableConstant(CompilerVariable var, CompilerTypeBase constant, 
			CompilerTypeList arguments, int argIndex) {
		
		if (!DataType.isNumeric(var.getDataType())) return false;
		if (!constant.isNumeric()) return false;		
		if (var.getDataType() == constant.getDataType()) return false;
		
		if (var.getDataType().getOrder() > constant.getDataType().getOrder())
			arguments.set(argIndex, DataType.cast(constant, var.getDataType()));
		else
			var.setDataType(constant.getDataType());
				
		return true;
	}
			
	private static boolean promoteNumericDataTypeVariableAggregate(CompilerVariable var, Aggregate aggr) {
		if (!DataType.isNumeric(var.getDataType())) return false;
		if (!DataType.isNumeric(aggr.getReturnDataType())) return false;
		if (var.getDataType() == aggr.getReturnDataType()) return false;

		if (var.getDataType().getOrder() > aggr.getReturnDataType().getOrder())
			aggr.setReturnDataType(var.getDataType());
		else
			var.setDataType(aggr.getReturnDataType());
		
		return true;
	}
		
	private static boolean promoteNumericDataTypeAggregateAggregate(Aggregate aggr1, Aggregate aggr2) {
		if (!DataType.isNumeric(aggr1.getReturnDataType())) return false;
		if (!DataType.isNumeric(aggr2.getReturnDataType())) return false;
		boolean promoted = false;
		
		if (aggr1.getReturnDataType() != aggr2.getReturnDataType()) {
			if (aggr1.getReturnDataType().getOrder() > aggr2.getReturnDataType().getOrder()) 
				aggr2.setReturnDataType(aggr1.getReturnDataType());
			else
				aggr1.setReturnDataType(aggr2.getReturnDataType());
			promoted = true;
		}
		
		promoteNumericDataTypesInAggregateTerms(aggr1, aggr2);
				
		return promoted;
	}
	
	private static boolean promoteNumericDataTypesInAggregateTerms(Aggregate aggr1, Aggregate aggr2) {
		boolean promoted = false;
		// if either is fscnt, special rules apply since the aggregate term can be multiple arguments		
		if (aggr1.getAggregateName().equals(FSAggregate.FSCNT_NAME) || aggr2.getAggregateName().equals(FSAggregate.FSCNT_NAME)) {
			// if both fscnt, loop through arguments and check for equality
			// aggregate term will be a functor. For now we care about 2nd argument only
			if (aggr1.getAggregateName().equals(aggr2.getAggregateName())) {				
				CompilerFunctor functor1 = (CompilerFunctor) aggr1.getAggregateTerm();
				CompilerFunctor functor2 = (CompilerFunctor) aggr2.getAggregateTerm();
				CompilerTypeBase arg1 = functor1.getArgument(1);
				CompilerTypeBase arg2 = functor2.getArgument(1);
				if (arg1.isVariable() && arg2.isVariable()) {
					promoted = promoteNumericDataTypeVariables((CompilerVariable)arg1, (CompilerVariable) arg2);
				} else if (arg1.isConstant() && arg2.isConstant()) {
					promoted = promoteNumericDataTypeConstantConstantInAggregateTerm(aggr1, aggr2);
				} else {
					if (arg1.isVariable())
						promoted = promoteNumericDataTypeConstantVariableInAggregateTerm(aggr2, aggr1);
					else
						promoted = promoteNumericDataTypeConstantVariableInAggregateTerm(aggr1, aggr2);
				}
					
			} else {
				Aggregate fscnt = aggr1;
				Aggregate other = aggr2; 
				if (aggr2.getAggregateName().equals(FSAggregate.FSCNT_NAME)) {
					fscnt = aggr2;
					other = aggr1;
				}

				CompilerFunctor functor1 = (CompilerFunctor) fscnt.getAggregateTerm();
				CompilerTypeBase fscntArg = functor1.getArgument(1);
				CompilerTypeBase otherArg = other.getAggregateTerm();
				if (fscntArg.isVariable() && otherArg.isVariable()) {
					promoted = promoteNumericDataTypeVariables((CompilerVariable)fscntArg, (CompilerVariable)otherArg);
				} else if (fscntArg.isConstant() && otherArg.isConstant()) {
					promoted = promoteNumericDataTypeConstantConstantInAggregateTerm(fscnt, other);
				} else {
					if (fscntArg.isVariable())
						promoted = promoteNumericDataTypeConstantVariableInAggregateTerm(other, fscnt);
					else
						promoted = promoteNumericDataTypeConstantVariableInAggregateTerm(fscnt, other);
				}
			}
		} else {
			if (aggr1.getAggregateName().equals(aggr2.getAggregateName())) {
				if (inferTermDataType(aggr1.getAggregateTerm()) != inferTermDataType(aggr2.getAggregateTerm())) {
					if (aggr1.getAggregateTerm().isVariable() && aggr2.getAggregateTerm().isVariable()) {
						promoted = promoteNumericDataTypeVariables((CompilerVariable)aggr1.getAggregateTerm(), 
								(CompilerVariable)aggr2.getAggregateTerm());
					} else if (aggr1.getAggregateTerm().isConstant() && aggr2.getAggregateTerm().isConstant()) {
						promoted = promoteNumericDataTypeConstantConstantInAggregateTerm(aggr1, aggr2);
					} else {
						if (aggr2.getAggregateTerm().isVariable())
							promoted = promoteNumericDataTypeConstantVariableInAggregateTerm(aggr1, aggr2);
						else
							promoted = promoteNumericDataTypeConstantVariableInAggregateTerm(aggr2, aggr1);						
					}
				}
			}
		}
		return promoted;
	}
	
	private static boolean promoteNumericDataTypeConstantVariableInAggregateTerm(Aggregate aggr1, Aggregate aggr2) {
		CompilerTypeBase constant = aggr1.getAggregateTerm();
		CompilerVariable var = (CompilerVariable) aggr2.getAggregateTerm();
		if (!constant.isNumeric()) return false;
		if (!DataType.isNumeric(var.getDataType())) return false;	

		if (var.getDataType().getOrder() > constant.getDataType().getOrder())
			aggr1.setAggregateTerm(DataType.cast(constant, var.getDataType()));
		else
			var.setDataType(constant.getDataType());
		return true;
	}
	
	private static boolean promoteNumericDataTypeConstantConstantInAggregateTerm(Aggregate aggr1, Aggregate aggr2) {
		CompilerTypeBase constant1 = aggr1.getAggregateTerm();
		CompilerTypeBase constant2 = aggr2.getAggregateTerm();
		if (!constant1.isNumeric()) return false;
		if (!constant2.isNumeric()) return false;		
		if (constant1.getDataType() == constant2.getDataType()) return false;
		
		if (constant1.getDataType().getOrder() > constant2.getDataType().getOrder())
			aggr2.setAggregateTerm(DataType.cast(constant2, constant1.getDataType()));
		else
			aggr1.setAggregateTerm(DataType.cast(constant1, constant2.getDataType()));
					
		return true;
	}
	
	private static boolean promoteNumericDataTypeConstantAggregate(CompilerTypeBase constant, CompilerTypeList arguments, 
			int argIndex, Aggregate aggr) {
		if (!constant.isNumeric()) return false;
		if (!DataType.isNumeric(aggr.getReturnDataType())) return false;
		
		if (constant.getDataType().getOrder() > aggr.getReturnDataType().getOrder())
			aggr.setReturnDataType(constant.getDataType());
		else
			arguments.set(argIndex, DataType.cast(constant, aggr.getReturnDataType()));
		
		return true;
	}
	
	private static boolean promoteNumericDataTypeConstantConstant(CompilerTypeBase constant1, CompilerTypeList predicate1Arguments, int argIndex1, 
			CompilerTypeBase constant2, CompilerTypeList predicate2Arguments, int argIndex2) {		
		if (!constant1.isNumeric()) return false;
		if (!constant2.isNumeric()) return false;
		if (constant1.getDataType() == constant2.getDataType()) return false;
		
		if (constant1.getDataType().getOrder() > constant2.getDataType().getOrder())
			predicate2Arguments.set(argIndex2, DataType.cast(constant2, constant1.getDataType()));
		else
			predicate1Arguments.set(argIndex1, DataType.cast(constant1, constant2.getDataType()));
		
		return true;
	}
		
	private static Integer[] getStringNumericMismatches(Predicate head1, Predicate head2) {
		List<Integer> mismatches = new ArrayList<>();
		DataType head1DataType, head2DataType;
		for (int i = 0; i < head1.getArity(); i++) {
			head1DataType = TypeInferrer.inferTermDataType(head1.getArgument(i));
			head2DataType = TypeInferrer.inferTermDataType(head2.getArgument(i));
			if (head1DataType == DataType.STRING && DataType.isNumeric(head2DataType))
				mismatches.add(i);
			
			if (DataType.isNumeric(head1DataType) && head2DataType == DataType.STRING)
				mismatches.add(i);
		}
		
		Integer[] mismatchPositions = null;
		if (mismatches.size() > 0) {
			mismatchPositions = new Integer[mismatches.size()];
			mismatchPositions = mismatches.toArray(mismatchPositions);
		}
		
		return mismatchPositions;
	}
	
	private static boolean reconcileNumericStringMismatch(Integer[] mismatches, Rule rule1, Rule rule2) {
		Predicate head1 = rule1.getHead();
		Predicate head2 = rule2.getHead();
		DataType head1DataType, head2DataType;
		CompilerTypeBase head1Argument, head2Argument;
		for (int i = 0; i < mismatches.length; i++) {
			head1Argument = head1.getArgument(mismatches[i]);
			head2Argument = head2.getArgument(mismatches[i]);					
			head1DataType = TypeInferrer.inferTermDataType(head1Argument);
			head2DataType = TypeInferrer.inferTermDataType(head2Argument);
			Predicate literal;
			if (head1DataType == DataType.STRING && DataType.isNumeric(head2DataType)) {				
				for (int j = 0; j < rule2.getBody().size(); j++) {
					literal = rule2.getBody().get(j);
					if (head2Argument instanceof CompilerVariable) {
						CompilerVariable head2Variable = (CompilerVariable)head2Argument;
						if ((literal.getPredicateType() == PredicateType.BUILT_IN) && ((BuiltInPredicate)literal).isBinary()) {
							if (TypeInferrer.usedInArithmetic(literal, head2Variable))
								return false;
							head2Variable.setDataType(DataType.STRING);
							mismatches[i] = 0;
						}
					}
				}
						
			} else if (DataType.isNumeric(head1DataType) && head2DataType == DataType.STRING) {
				for (int j = 0; j < rule1.getBody().size(); j++) {
					literal = rule1.getBody().get(j);
					if (head1Argument instanceof CompilerVariable) {
						CompilerVariable head1Variable = (CompilerVariable)head1Argument;
						if ((literal.getPredicateType() == PredicateType.BUILT_IN) && ((BuiltInPredicate)literal).isBinary()) {
							if (TypeInferrer.usedInArithmetic(literal, head1Variable))
								return false;
							head1Variable.setDataType(DataType.STRING);
							mismatches[i] = 0;
						}
					}
				}
			}
		}
		
		for (int i = 0; i < mismatches.length; i++)
			if (mismatches[i] == 1)
				return false;
		
		return true;
	}
	
	private static boolean usedInArithmetic(Predicate literal, CompilerVariable variable) {
		// ok if for assignment
		if (variable.equals(literal.getArgument(0))) return false;
		
		if (variable.equals(literal.getArgument(1))) return false;
		
		if (literal.getArgument(0) instanceof CompilerFunctor)
			if (usedInArithmetic((CompilerFunctor)literal.getArgument(0), variable))
				return true;
		
		if (literal.getArgument(1) instanceof CompilerFunctor)
			if (usedInArithmetic((CompilerFunctor)literal.getArgument(1), variable))
				return true;
	
		return false;
	}
	
	private static boolean usedInArithmetic(CompilerFunctor arithmeticFunctor, CompilerVariable variable) {
		for (int i = 0; i < arithmeticFunctor.getArity(); i++) {
			if (arithmeticFunctor.getArgument(i).equals(variable))
				return true;
			
			if (arithmeticFunctor.getArgument(i) instanceof CompilerFunctor)
				if (usedInArithmetic((CompilerFunctor)arithmeticFunctor.getArgument(i), variable))
					return true;
		}
		return false;
	}
		
	private boolean hasMatchingAssignments(Predicate head1, Predicate head2, List<Predicate> assignedPredicates) {
		DataType head1DataType, head2DataType;
		for (int i = 0; i < head1.getArity(); i++) {
			head1DataType = TypeInferrer.inferTermDataType(head1.getArgument(i));
			head2DataType = TypeInferrer.inferTermDataType(head2.getArgument(i));
 			
			if (head1DataType == DataType.UNKNOWN && head1.getArgument(i).isAnyAggregate())
				head1DataType = this.inferAggregateDataType((Aggregate) head1.getArgument(i), assignedPredicates);
			
			if (head2DataType == DataType.UNKNOWN && head2.getArgument(i).isAnyAggregate())
				head2DataType = this.inferAggregateDataType((Aggregate) head2.getArgument(i), assignedPredicates);
						
			if (head1DataType != head2DataType)
				return false;
		}
		return true;
	}
	
	public void showRule(Rule rule) {
		for (Predicate goal : rule.getBody())
			this.deALSContext.logInfo(logger, "    {}", showPredicate(goal, false));
		
		this.deALSContext.logInfo(logger, "  {}", showPredicate(rule.getHead(), true));
	}
	
	public String showPredicate(Predicate predicate, boolean isHead) {
		StringBuilder retval = new StringBuilder();
		showPredicate(predicate, isHead, retval);
		return retval.toString();
	}

	private static void showPredicate(Predicate predicate, boolean isHead, StringBuilder output) {		
		if (isHead)
			output.append("head:");
		else
			output.append("goal:");					
		
		output.append(predicate.toString());
		output.append(" - Type Assignments:[");

		if (predicate.isBuiltIn() 
				&& ((((BuiltInPredicate)predicate).getBuiltInPredicateType() == BuiltInPredicateType.IFTHENELSE)
						|| (((BuiltInPredicate)predicate).getBuiltInPredicateType() == BuiltInPredicateType.IFTHEN))) {
			
			BuiltInPredicate bip = (BuiltInPredicate)predicate;
			if (bip.getBuiltInPredicateType() == BuiltInPredicateType.IFTHENELSE)
				showIfThenElse((IfThenElsePredicate)bip, output);
			else
				showIfThen((IfThenPredicate)bip, output);
			
		} else {
			int argumentIndex = 0;
			for (CompilerTypeBase argument : predicate.getArguments()) {
				if (argumentIndex > 0) 
					output.append(", ");
				showTerm(argument, output, true);
				argumentIndex++;
			}
		}
		output.append("]");	
	}
	
	private static void showIfThenElse(IfThenElsePredicate ite, StringBuilder output) {
		for (Predicate ifLiteral : ite.getIfLiterals()) {
			output.append("\n      ");
			showPredicate(ifLiteral, false, output);
		}
		
		for (Predicate thenLiteral : ite.getThenLiterals()) {
			output.append("\n      ");
			showPredicate(thenLiteral, false, output);
		}
		
		for (Predicate elseLiteral : ite.getElseLiterals()) {
			output.append("\n      ");
			showPredicate(elseLiteral, false, output);
		}
	}
	
	private static void showIfThen(IfThenPredicate it, StringBuilder output) {
		for (Predicate ifLiteral : it.getIfLiterals()) {
			output.append("\n      ");
			showPredicate(ifLiteral, false, output);
		}
		
		for (Predicate thenLiteral : it.getThenLiterals()) {
			output.append("\n      ");	
			showPredicate(thenLiteral, false, output);
		}
	}
		
	private static void showTerm(CompilerTypeBase term, StringBuilder retval, boolean output) {
		if (term.isAnyAggregate()) {
			retval.append("(" + ((Aggregate)term).getAggregateName() + "=>" + ((Aggregate)term).getReturnDataType() + ")");
			term = ((Aggregate)term).getAggregateTerm();
		}

		if (term.isConstant() || term.isNil() || term.isCast() || term.isVariable()) {
			retval.append(term.toString());
			retval.append(":");
			retval.append(term.getDataType());
		} else if (term.isFunctor()) {
			showFunctor((CompilerFunctor)term, retval);	
		} else if (term.isList()) {
			showList((CompilerList)term, retval);
			if (output)
				retval.append(":list");
		} else if (term.isExpression()) {
			retval.append("(");
			showTerm(((CompilerArithmeticExpression)term).getArgument1(), retval, false);
			retval.append(" ");
			retval.append(((CompilerArithmeticExpression)term).getOperation());
			if (((CompilerArithmeticExpression)term).isBinary()) {
				retval.append(" ");
				showTerm(((CompilerArithmeticExpression)term).getArgument2(), retval, false);
			}
			retval.append(") as ");
			retval.append(term.getDataType());
		}
	}
		
	private static void showFunctor(CompilerFunctor functor, StringBuilder output) {
		int counter = 0;
		for (CompilerTypeBase arg : functor.getArguments()) {
			if (counter > 0) 
				output.append(", ");
			showTerm(arg, output, false);
			counter++;
		}
	}
	
	private static void showList(CompilerList list, StringBuilder output) {
		int counter = 0;
		CompilerList temp = list;
		output.append("[");
		while (temp != null && !temp.isEmpty()) {
			if (counter > 0)
				output.append(", ");
			showTerm(temp.getHead(), output, false);
			temp = temp.getTail();
			counter++;
		}
		output.append("]");
	}
	
	public void inferTypes(PCGOrNode orNode) {
		if (orNode.getBuiltInPredicateType() == BuiltInPredicateType.BINARY)
			TypeChecker.makeSafe(orNode);
				
		for (PCGOrNodeChild child : orNode.getChildren()) {
			if (child instanceof PCGAndNode)
				this.inferTypes((PCGAndNode)child);
		}
	}
	
	public void inferTypes(PCGAndNode andNode) {
		for (PCGOrNode child : andNode.getChildren())
			this.inferTypes(child);
		
	}
}