package edu.ucla.cs.wis.bigdatalog.compiler.rewriting;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.MonotonicRuleType;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.Aggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenElsePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerInt;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerLong;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerLongLong;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerLongLongLongLong;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerNil;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerString;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.system.ExecutionTarget;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

/* APS 4/11/2013
 * This class contains the rewriting methods used for rewriting 
 * aggregates after parsing, but before PCG Generation. 
 */
public class AggregateRewriter {	
	private static Logger logger = LoggerFactory.getLogger(AggregateRewriter.class.getName());

	public static String STRATIFIED_AGGREGATE_NODE_NAME_PREFIX = "aggregate_";
	public static String FS_AGGREGATE_NODE_NAME_PREFIX = "fs_aggregate_";
	//private static String FS_AGGREGATE_NODE_RENAMED_PREFIX = "fs_";
	
	public static String STATE_VARIABLE_NAME = "Result";
	public static String MULTI_VALUE_PREFIX = "MultiValue_";
	public static String OLD_VALUE_PREFIX = "OldValue_";
	public static String OLD_FS_VALUE_PREFIX = "OldMaxValue_";	
	public static String NEW_FS_VALUE_PREFIX = "NewMaxValue_";
	public static String FINAL_VALUE_PREFIX = "FinalValue_";
	public static String AGGR_VALUE_PREFIX = "Aggr_";
	public static String MULTIPLICITY_VALUE_PREFIX = "Multiplicity_";
	public static String FS_AGGREGATE_VALUE_PREFIX = "NewMaxAggrValue_";
	public static String IS_NEW_MAX_VALUE_PREFIX = "IsNewMaxValue_";
	public static String FS_AGGR_VALUE_PREFIX = "FSAggr_";
	public static String CONSTANT_AGGR_TERM_PREFIX = "AggrTerm_";
	
	private DeALSContext deALSContext;
	
	public AggregateRewriter(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	/* APS adds 3/1/2013
	this process expands aggregate rules and generates predicates in their place
	this process encompasses all types of aggregates

	we have following cases
	1) rules with user defined stratified aggregates in the head
	2) rules with a stratified aggregate(s) in the head
	3) rules with a fs (unstratified) aggregate(s) in the head
	
	case 2:
	 for each stratified aggregate usage in a rule head, we need to expand into the following:
	 new rule with:
	 - head of the same name as original rule with
	   - the aggregate replaced with a variable named "FinalValue_1.0"
	 - body with
	   - a new literal "aggregate_[original rule name]_[originalruleid]([non-aggr args], aggr variable, OldValue_1, Result)
	   - a second new ifthenelse literal:
	     - if (Result = 1 then return([built in aggregate type id], OldValue_1, FinalValue_1.0)
	     - else 
	         (if (Result = 2 then single([built in aggregate type id], MultiValue_1)
	          else multi([built in aggregate type id], [aggr variable], OldValue_1, MultiValue_1)) 
	       "aggregate_[original rule name]_[originalruleid]([non-aggr args], MultiValue_1)
	       false)
	  for example: max_price(Z, max<X>) <- price(X, _, Z). becomes:
    -2 : max_price(Z, FinalValue_1.0) <-
            aggregate_max_price_2(Z, X, OldValue_1, Result),
            if( Result = 1 then return(9, OldValue_1, FinalValue_1.0) 
            else if( Result = 2 then single(9, X, MultiValue_1) 
            	else multi(9, X, OldValue_1, MultiValue_1) ), 
            aggregate_max_price_2(Z, MultiValue_1), false ).
    
    -1 : aggregate_max_price_2(Z, X, nil, nil) <-
            price(X, _, Z).           
  	
	case 3:
	 for each fs aggregate usage in a rule head, we need to expand into the following:
	 new rule with:
	 - head of the same name as original rule with
	   - the aggregate replaced with a variable named "FinalValue_1.0"
	 - body with
	   - a new literal "fs_aggregate_[original rule name]_[originalruleid]([non-aggr args], aggr variable, OldValue_1, Result)
	   . TBD
	   .
	   .  
	  for example: max_price(Z, fsmax<X>) <- price(X, _, Z).
	  becomes:
	
	 */	
	public void rewriteAggregateRules(Module module) {
		this.deALSContext.logTrace(logger, "Entering rewriteAggregateRules for {}", module.getModuleName());
		
		this.deALSContext.logInfo(logger, "\n[BEGIN Rewrite Aggregate Rules for Module '{}' BEGIN]", module.getModuleName());
		this.deALSContext.logInfo(logger, "\n[BEGIN Rules Before Rewriting BEGIN]");
		this.deALSContext.logInfo(logger, AggregateRewriter.toStringRules(module));
		this.deALSContext.logInfo(logger, "[END Rules Before Rewriting END]\n");
		
		// this is for info below
		List<Pair<Rule, Rule>> rewrittenRules = new ArrayList<>();
		List<Rule> newRules = new ArrayList<>();	
		List<DerivedPredicate> newDerivedPredicates = new ArrayList<>();
		
		// save off original derived relations before we rewrite any
		module.takeSnapshotOfDerivedPredicates();
		
		for (DerivedPredicate derivedPredicate : module.getDerivedPredicates()) {
			// need to use this form of iteration because we manipulate the collection below
			for (int j = 0; j < derivedPredicate.getNumberOfRules(); j++) {
				Rule originalRule = derivedPredicate.getRule(j);
				
				if (!originalRule.getHead().containsAnyAggregate())
					continue;

				ExecutionTarget executionTarget = 
						ExecutionTarget.getExecutionTarget(this.deALSContext.getConfiguration().getProperty("deals.interpreter.executiontarget"));
				Pair<Rule, Predicate> retvalPair = null;
				if (originalRule.getHead().containsFSAggregate()) {
					if (executionTarget == ExecutionTarget.DeALS_PIPELINED) {
						boolean hasUDA = false;
						for (int i = 0; i < originalRule.getHead().getArity(); i++)
							if (originalRule.getHead().getArgument(i).isUserDefinedAggregate())
								hasUDA = true;
						
						//if (this.deALSConfiguration.getConfiguration().compareProperty("deals.aggregates.useudaframework", "true") || hasUDA)
						if (hasUDA)
							retvalPair = rewriteFSAggregateRulePipelinedUDA(originalRule);
						else
							retvalPair = rewriteFSAggregateRulePipelined(originalRule);
					} else {
						retvalPair = rewriteFSAggregateRuleMaterialized(originalRule);
					}
				} else {
					if (executionTarget == ExecutionTarget.DeALS_PIPELINED) {
						boolean hasUDA = false;
						for (int i = 0; i < originalRule.getHead().getArity(); i++)
							if (originalRule.getHead().getArgument(i).isUserDefinedAggregate())
								hasUDA = true;
						
						//if (this.deALSConfiguration.getConfiguration().compareProperty("deals.aggregates.useudaframework", "true") || hasUDA)
						if (hasUDA)
							retvalPair = rewriteStratifiedAggregateRulePipelinedUDA(originalRule);
						else
							retvalPair = rewriteStratifiedAggregateRulePipelined(originalRule);												
					} else {
						retvalPair = rewriteStratifiedAggregateRuleMaterialized(originalRule);
					}
				}
				
				if (retvalPair != null) {
					Rule rewrittenRule = retvalPair.getFirst();
					rewrittenRules.add(new Pair<>(originalRule, rewrittenRule));
					originalRule.setRewritten(true);
					rewrittenRule.setOriginalRule(originalRule);
					
					// add expanded rule to derived rule - this rule will replace the original
					derivedPredicate.addRule(rewrittenRule);

					// create new rule for aggregate rules in body of new rule
					Predicate newPredicate = retvalPair.getSecond();

					CompilerVariableList variableList = new CompilerVariableList();					
					Predicate newHeadPredicate = this.buildNewRuleHead(originalRule, newPredicate, variableList);					
					List<Predicate> newBody = new ArrayList<>();
					for (Predicate literal : originalRule.getBody())
						newBody.add(literal.copy(variableList));

					Rule newRule = new Rule(originalRule.getRuleId() + ".1", newHeadPredicate, newBody);
					newRule.setResultOfRewrite(true);
					newRules.add(newRule);

					DerivedPredicate newDerivedPredicate = new DerivedPredicate(newPredicate.getPredicateName(), 
							newPredicate.getArity());
					
					newDerivedPredicate.addRule(newRule);
					newDerivedPredicates.add(newDerivedPredicate);					
				}
			}
		}
		
		for (DerivedPredicate newDerivedPredicate : newDerivedPredicates) 
			module.getDerivedPredicates().add(newDerivedPredicate);
				
		this.removeRewrittenRules(module);
				
		this.deALSContext.logInfo(logger, "\n[BEGIN Rewrite Aggregate Rules After Rewriting BEGIN]");
		if (rewrittenRules.size() > 0)
			this.deALSContext.logInfo(logger, AggregateRewriter.toStringRules(module));
		else
			this.deALSContext.logInfo(logger, "NO RULES REWRITTEN!");
		
		this.deALSContext.logInfo(logger, "[END Rules After Rewriting END]\n");
		this.deALSContext.logInfo(logger, "[END Rewrite Aggregate Rules for Module '{}' END]\n", module.getModuleName());
		
		this.deALSContext.logTrace(logger, "Exiting rewriteAggregateRules for {}", module.getModuleName());
	}
		
	private Predicate buildNewRuleHead(Rule originalRule, Predicate newPredicate, CompilerVariableList variableList) {
		CompilerTypeList arguments = new CompilerTypeList();
		List<BuiltInAggregateType> builtInAggregateTypes = new ArrayList<>();
		
		for (CompilerTypeBase argument : originalRule.getHead().getArguments()) {
			if (argument.containsBuiltInAggregate() || argument.containsUserDefinedAggregate()) {
				CompilerTypeBase aggTerm = ((Aggregate)argument).getAggregateTerm();
				arguments.add(aggTerm.copy(variableList));
				if (argument.containsBuiltInAggregate())
					builtInAggregateTypes.add(((BuiltInAggregate)argument).getBuiltInAggregateType());
			} else if (argument.containsFSAggregate()) {
				FSAggregate fsAggregate = (FSAggregate)argument;
				CompilerTypeBase aggTerm = fsAggregate.getAggregateTerm();
				
				if (fsAggregate.getFSAggregateType() == FSAggregateType.FSCNT) {					
					if (aggTerm.isFunctor()) {
						arguments.add(aggTerm.copy(variableList));
					} else if (aggTerm.isVariable()) {
						CompilerTypeBase one;
						switch(this.deALSContext.getConfiguration().getCountDataType()) {
							case LONG:
								one = new CompilerLong(1);
								break;
							case LONGLONG:
								one = new CompilerLongLong(1);
								break;
							case LONGLONGLONGLONG:
								one = new CompilerLongLongLongLong(1);
								break;
							default:
								one = new CompilerInt(1);								
						}
						// TODO fix the case for companycontrol, where fscnt<P> could work
						arguments.add(CompilerFunctor.createFunctor(new CompilerTypeBase[]{aggTerm.copy(variableList), one}));
					} else {
						throw new CompilerException("Invalid argument found in fscnt<> aggregate");
					}
				} else {
					arguments.add(aggTerm.copy(variableList));
				}
			} else {
				arguments.add(argument.copy(variableList));
			}
		}
		
		if (newPredicate.getArgument(0).toString().equals("read"))
			arguments.add(0, new CompilerString("read"));
		
		// finish off the remaining argument places with nils
		for (int k = arguments.size(); k < newPredicate.getArity(); k++)
			arguments.add(new CompilerNil());

		Predicate predicate = new Predicate(newPredicate.getPredicateName(), arguments);
		predicate.setFSAggregateType(newPredicate.getFSAggregateType());
		return predicate;
	}

	private static Pair<Rule, Predicate> rewriteStratifiedAggregateRulePipelinedUDA(Rule rule) {
		CompilerVariableList variableList = new CompilerVariableList();
		
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();

		CompilerTypeList newRuleHeadArgs = new CompilerTypeList();
		CompilerTypeList multiValueArgs = new CompilerTypeList();
		CompilerTypeList finalValueArgs = new CompilerTypeList();
		CompilerTypeList oldValueArgs = new CompilerTypeList();
		CompilerTypeList aggregateArgs = new CompilerTypeList();
		CompilerTypeList nonAggregateArgs = new CompilerTypeList();
		
		List<Predicate> newRuleBody = new ArrayList<>();		
		int aggregateCount = 0;
		Aggregate aggregate;
		// Step.1
		// create aggregate args 
		// collect non aggregate args 
		for (CompilerTypeBase argument : rule.getHead().getArguments()) {
			if (argument.containsAnyAggregate()) {
				aggregateCount++;
				aggregate = (Aggregate)argument;

				CompilerVariable newVariable = new CompilerVariable(FINAL_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(aggregate.getReturnDataType());
				finalValueArgs.add(newVariable);
				newRuleHeadArgs.add(newVariable);
			
				newVariable = new CompilerVariable(MULTI_VALUE_PREFIX + aggregateCount);
				if (aggregate.isBuiltInAggregate() && (((BuiltInAggregate)aggregate).getBuiltInAggregateType() == BuiltInAggregateType.AVG))
					newVariable.setDataType(DataType.COMPLEX);
				else
					newVariable.setDataType(aggregate.getReturnDataType());
				multiValueArgs.add(newVariable);

				newVariable = new CompilerVariable(OLD_VALUE_PREFIX + aggregateCount);
				if (aggregate.isBuiltInAggregate() && (((BuiltInAggregate)aggregate).getBuiltInAggregateType() == BuiltInAggregateType.AVG))
					newVariable.setDataType(DataType.COMPLEX);
				else
					newVariable.setDataType(aggregate.getReturnDataType());
				oldValueArgs.add(newVariable);
				
				aggregateArgs.add(argument.copy(variableList));
			} else {
				newRuleHeadArgs.add(argument.copy(variableList));
				nonAggregateArgs.add(argument);
			}
		}

		// first add the access table predicate
		CompilerTypeList readAggregatePredicateArgs = new CompilerTypeList();

		AggregateRewriter.appendAggregateArguments(readAggregatePredicateArgs, nonAggregateArgs, variableList, true);
		AggregateRewriter.appendAggregateTermArguments(readAggregatePredicateArgs, aggregateCount, aggregateArgs, variableList);
		AggregateRewriter.appendAggregateArguments(readAggregatePredicateArgs, oldValueArgs, variableList, false);			

		CompilerVariable resultVariable = new CompilerVariable(STATE_VARIABLE_NAME);
		resultVariable.setDataType(DataType.INT);		
		readAggregatePredicateArgs.addUnique(resultVariable);
		
		String aggregatePredicateName = STRATIFIED_AGGREGATE_NODE_NAME_PREFIX + rule.getHead().getPredicateName();
		aggregatePredicateName += "_" + rule.getRuleId();
		
		BuiltInPredicate readAggregatePredicate = new BuiltInPredicate(aggregatePredicateName, readAggregatePredicateArgs, BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE);

		newRuleBody.add(readAggregatePredicate);
		newDerivedPredicatePredicate = readAggregatePredicate.copy();

		// build the outer ifthenelse predicate
		List<Predicate> outerIfLiterals = new ArrayList<>();
		List<Predicate> outerThenLiterals = new ArrayList<>();
		List<Predicate> outerElseLiterals = new ArrayList<>();

		// add Result = 1 if predicate
		CompilerTypeList outerIfPredicateArgs = new CompilerTypeList();
		outerIfPredicateArgs.add(resultVariable);
		outerIfPredicateArgs.add(new CompilerInt(1)); // 1 for return
		BuiltInPredicate outerIfPredicate = new BuiltInPredicate(BuiltInPredicate.EQUALITY_PREDICATE_NAME, 
				outerIfPredicateArgs, BuiltInPredicateType.BINARY);

		outerIfLiterals.add(outerIfPredicate);
		int aggregateType = 0;
		String aggregateName;
		CompilerTypeBase argument;
		CompilerTypeBase aggregateArgument; 
		
		for (int i = 0; i < aggregateCount; i++) {
			CompilerTypeList outerThenPredicateArgs = new CompilerTypeList();
			aggregateArgument = aggregateArgs.get(i);

			if (aggregateArgument.isBuiltInAggregate() || aggregateArgument.isFSAggregate()) {
				aggregateType = ((BuiltInAggregate)aggregateArgument).getBuiltInAggregateType().getId();
				outerThenPredicateArgs.add(new CompilerInt(aggregateType));
			} else {
				aggregateName = ((Aggregate)aggregateArgument).getAggregateName();
				outerThenPredicateArgs.add(new CompilerString(aggregateName));
				outerThenPredicateArgs.add(new CompilerString(CompilerTypeBase.NIL));
			}
			
			argument = oldValueArgs.get(i);
			outerThenPredicateArgs.add(argument);

			argument = finalValueArgs.get(i);
			outerThenPredicateArgs.add(argument);

			Predicate outerThenPredicate;
			if (aggregateArgument.isBuiltInAggregate() || aggregateArgument.isFSAggregate()) {
				if (((BuiltInAggregate)aggregateArgument).getBuiltInAggregateType() == BuiltInAggregateType.AVG) {
					if (((CompilerVariable)outerThenPredicateArgs.get(2)).getDataType() == DataType.COMPLEX)
						((CompilerVariable)outerThenPredicateArgs.get(2)).setDataType(DataType.DOUBLE);
				}
				outerThenPredicate = new BuiltInPredicate(BuiltInPredicate.RETURN_PREDICATE_NAME, 
						outerThenPredicateArgs, BuiltInPredicateType.RETURN);
			} else {
				// if it is a user defined, we don't specify the return predicate as built-in
				 outerThenPredicate = new Predicate(BuiltInPredicate.RETURN_PREDICATE_NAME, 
						 outerThenPredicateArgs);
			}
			
			outerThenLiterals.add(outerThenPredicate);
		}

		// build inner ifthenelse predicate							
		List<Predicate> innerIfLiterals = new ArrayList<>();
		List<Predicate> innerThenLiterals = new ArrayList<>();
		List<Predicate> innerElseLiterals = new ArrayList<>();

		CompilerTypeList innerIfPredicateArgs = new CompilerTypeList();
		innerIfPredicateArgs.add(resultVariable);
		innerIfPredicateArgs.add(new CompilerInt(2));		// 2 for single node to be processed
		BuiltInPredicate innerIfPredicate = new BuiltInPredicate(BuiltInPredicate.EQUALITY_PREDICATE_NAME, 
				innerIfPredicateArgs, BuiltInPredicateType.BINARY);

		innerIfLiterals.add(innerIfPredicate);

		// add single(...) then predicates
		for (int i = 0; i < aggregateCount; i++) {
			CompilerTypeList innerThenPredicateArgs = new CompilerTypeList();
			aggregateArgument = aggregateArgs.get(i);

			if (aggregateArgument.isBuiltInAggregate()|| aggregateArgument.isFSAggregate()) {
				aggregateType = ((BuiltInAggregate)aggregateArgument).getBuiltInAggregateType().getId();
				innerThenPredicateArgs.add(new CompilerInt(aggregateType));				
			} else {
				aggregateName = ((Aggregate)aggregateArgument).getAggregateName();
				innerThenPredicateArgs.add(new CompilerString(aggregateName));
			}			

			innerThenPredicateArgs.add(((Aggregate)aggregateArgument).getAggregateTerm().copy(variableList));

			argument = multiValueArgs.get(i);
			innerThenPredicateArgs.add(argument);

			Predicate innerThenPredicate;			
			if (aggregateArgument.isBuiltInAggregate()|| aggregateArgument.isFSAggregate()) {
				innerThenPredicate = new BuiltInPredicate(BuiltInPredicate.SINGLE_PREDICATE_NAME, 
						innerThenPredicateArgs, BuiltInPredicateType.SINGLE);				
			} else {
				innerThenPredicate = new Predicate(BuiltInPredicate.SINGLE_PREDICATE_NAME, 
						innerThenPredicateArgs);
			}
			
			innerThenLiterals.add(innerThenPredicate);
		}

		for (int i = 0; i < aggregateCount; i++) {
			CompilerTypeList innerElsePredicateArgs = new CompilerTypeList();
			aggregateArgument = aggregateArgs.get(i);

			if (aggregateArgument.isBuiltInAggregate()|| aggregateArgument.isFSAggregate()) {
				aggregateType = ((BuiltInAggregate)aggregateArgument).getBuiltInAggregateType().getId();
				innerElsePredicateArgs.add(new CompilerInt(aggregateType));
			} else {
				aggregateName = ((Aggregate)aggregateArgument).getAggregateName();
				innerElsePredicateArgs.add(new CompilerString(aggregateName));
			}
					
			innerElsePredicateArgs.add(((Aggregate)aggregateArgument).getAggregateTerm().copy(variableList));

			argument = oldValueArgs.get(i);
			innerElsePredicateArgs.add(argument);

			argument = multiValueArgs.get(i);
			innerElsePredicateArgs.add(argument);
			
			Predicate innerElsePredicate;
			if (aggregateArgument.isBuiltInAggregate()|| aggregateArgument.isFSAggregate()) {
				innerElsePredicate = new BuiltInPredicate(BuiltInPredicate.MULTI_PREDICATE_NAME, 
						innerElsePredicateArgs, BuiltInPredicateType.MULTI);
			} else {
				innerElsePredicate = new Predicate(BuiltInPredicate.MULTI_PREDICATE_NAME, 
						innerElsePredicateArgs);
			}

			innerElseLiterals.add(innerElsePredicate);
		}

		// add the else predicate, which is inner ifthenelse predicate
		IfThenElsePredicate innerIfThenElsePredicate = new IfThenElsePredicate(innerIfLiterals, innerThenLiterals, innerElseLiterals);
		outerElseLiterals.add(innerIfThenElsePredicate);

		// add the write table predicate to store the value into the aggregate table
		CompilerTypeList writeAggregatePredicateArgs = new CompilerTypeList();
		AggregateRewriter.appendAggregateArguments(writeAggregatePredicateArgs, nonAggregateArgs, variableList, true);
		AggregateRewriter.appendAggregateArguments(writeAggregatePredicateArgs, multiValueArgs, variableList, false);		
		
		BuiltInPredicate writeAggregatePredicate = new BuiltInPredicate(aggregatePredicateName, writeAggregatePredicateArgs, 
				BuiltInPredicateType.WRITE_USER_DEFINED_AGGREGATE);
		outerElseLiterals.add(writeAggregatePredicate);

		// built in aggregate rules fail in order to backtrack.  This is how the aggregate is stratified
		// custom aggregates will need a return rule to be specified if they want.
		BuiltInPredicate falsePredicate = new BuiltInPredicate(BuiltInPredicate.FALSE_PREDICATE_NAME, 
				new CompilerTypeList(), BuiltInPredicateType.FALSE);
		outerElseLiterals.add(falsePredicate);

		Predicate outerIfThenElsePredicate = new IfThenElsePredicate(outerIfLiterals, outerThenLiterals, outerElseLiterals);		
		newRuleBody.add(outerIfThenElsePredicate);

		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArgs);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
		
		Rule newRule = new Rule(rule.getRuleId() + ".2", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}
	
	private static Pair<Rule, Predicate> rewriteStratifiedAggregateRulePipelined(Rule rule) {
		CompilerVariableList variableList = new CompilerVariableList();
		
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();

		CompilerTypeList newRuleHeadArgs = new CompilerTypeList();
		
		List<Predicate> newRuleBody = new ArrayList<>();
		int aggregateCount = 0;
		Aggregate aggregate;
		// collect non aggregate args & create aggregate args  
		for (CompilerTypeBase argument : rule.getHead().getArguments()) {
			if (argument.containsAnyAggregate()) {
				aggregateCount++;
				aggregate = (Aggregate)argument;

				CompilerVariable newVariable = new CompilerVariable(AGGR_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(aggregate.getReturnDataType());
				newRuleHeadArgs.add(newVariable);
				// APS 2/18/2015 - to support constant terms in aggregates, we add  
				// rewriting so the rest of the compiler's algorithms work without change 
				// if the aggregate term is a constant (e.g. count<1>):
				//  1) create variable
				//  2) replace constant term with variable 
				//  3) add assignemnt predicate in body assigning constant term to new variable
				CompilerTypeBase aggregateTerm = aggregate.getAggregateTerm();
				if (aggregateTerm.isConstant() 
						|| (aggregateTerm.isVariable() && ((CompilerVariable)aggregateTerm).isAnonymous())) {
					
					if ((aggregateTerm.isVariable() && ((CompilerVariable)aggregateTerm).isAnonymous()) 
							&& (BuiltInAggregateType.getBuiltInAggregateType(aggregate.getAggregateName()) != BuiltInAggregateType.COUNT))
						throw new CompilerException("Invalid aggregate term for " + aggregate.getAggregateName());
					
					String variableName = AggregateRewriter.CONSTANT_AGGR_TERM_PREFIX + aggregateCount;
					CompilerVariable newAggregateTerm = new CompilerVariable(variableName);
					newAggregateTerm.setDataType(aggregateTerm.getDataType());
					
					CompilerTypeList args = new CompilerTypeList();
					args.add(newAggregateTerm);
					args.add(aggregateTerm.isConstant() ? aggregateTerm : new CompilerInt(1));	
					rule.getBody().add(new BuiltInPredicate(BuiltInPredicate.EQUALITY_PREDICATE_NAME, args, BuiltInPredicateType.BINARY));
					
					aggregate.setAggregateTerm(newAggregateTerm);
				}
			} else {
				newRuleHeadArgs.add(argument.copy(variableList));
			}
		}

		String aggregatePredicateName = STRATIFIED_AGGREGATE_NODE_NAME_PREFIX + rule.getHead().getPredicateName();
		
		BuiltInPredicate aggregatePredicate = new BuiltInPredicate(aggregatePredicateName, rule.getHead().getArguments().copy(variableList), 
				BuiltInPredicateType.AGGREGATE);
		newRuleBody.add(aggregatePredicate);

		newDerivedPredicatePredicate = aggregatePredicate.copy();

		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArgs);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
		
		Rule newRule = new Rule(rule.getRuleId() + ".2", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}
	
	private static Pair<Rule, Predicate> rewriteStratifiedAggregateRuleMaterialized(Rule rule) {
		CompilerVariableList variableList = new CompilerVariableList();
		
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();

		CompilerTypeList newRuleHeadArgs = new CompilerTypeList();
		CompilerTypeList finalValueArgs = new CompilerTypeList();
		CompilerTypeList aggregateArgs = new CompilerTypeList();
		CompilerTypeList nonAggregateArgs = new CompilerTypeList();
		
		List<Predicate> newRuleBody = new ArrayList<>();
		int aggregateCount = 0;
		Aggregate aggregate;
		// Step.1
		// create aggregate args 
		// collect non aggregate args 
		for (CompilerTypeBase argument : rule.getHead().getArguments()) {
			if (argument.containsAnyAggregate()) {
				aggregateCount++;
				aggregate = (Aggregate)argument;

				CompilerVariable newVariable = new CompilerVariable(AGGR_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(aggregate.getReturnDataType());
				finalValueArgs.add(newVariable);
				newRuleHeadArgs.add(newVariable);
		
				aggregateArgs.add(argument.copy(variableList));
			} else {
				newRuleHeadArgs.add(argument.copy(variableList));
				nonAggregateArgs.add(argument);
			}
		}

		// first add the access table predicate
		CompilerTypeList aggregatePredicateArgs = new CompilerTypeList();

		AggregateRewriter.appendAggregateArguments(aggregatePredicateArgs, nonAggregateArgs, variableList, true);
		AggregateRewriter.appendAggregateArguments(aggregatePredicateArgs, finalValueArgs, variableList, true);
		
		String aggregatePredicateName = STRATIFIED_AGGREGATE_NODE_NAME_PREFIX + rule.getHead().getPredicateName();
		aggregatePredicateName += "_" + rule.getRuleId();
		
		BuiltInPredicate readAggregatePredicate = new BuiltInPredicate(aggregatePredicateName, rule.getHead().getArguments().copy(), 
				BuiltInPredicateType.AGGREGATE);

		newRuleBody.add(readAggregatePredicate);
		newDerivedPredicatePredicate = readAggregatePredicate.copy();

		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArgs);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
		
		Rule newRule = new Rule(rule.getRuleId() + ".2", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}
	
	@SuppressWarnings("cast")
	private Pair<Rule, Predicate> rewriteFSAggregateRulePipelinedUDA(Rule rule) {
		CompilerVariableList variableList = new CompilerVariableList();
		
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();

		CompilerTypeList newRuleHeadArguments = new CompilerTypeList();
		CompilerTypeList aggregateValueArguments = new CompilerTypeList();
		//CompilerTypeList newValueArguments = new CompilerTypeList();
		CompilerTypeList oldValueArguments = new CompilerTypeList();
		CompilerTypeList multiplicityValueArguments = new CompilerTypeList();
		CompilerTypeList isNewMaxValueArguments = new CompilerTypeList();
		
		List<Pair<FSAggregateType, CompilerTypeBase>> aggregateArguments = new ArrayList<>();
		CompilerTypeList nonAggregateArguments = new CompilerTypeList();
		List<Predicate> newRuleBody = new ArrayList<>();
		int[] fsAggregateTypeCounter = new int[4];
		for (int i = 0; i < 4; i++)
			fsAggregateTypeCounter[i] = 0;
		
		int aggregateCount = 0;
		FSAggregate aggregate;	
		
		// first, go through the head and determine what each argument does
		// initialize variable we will use throughout this procedure
		for (CompilerTypeBase argument : rule.getHead().getArguments()) {
			if (argument.containsFSAggregate()) {
				aggregateCount++;
				aggregate = (FSAggregate)argument;				

				CompilerVariable newVariable = new CompilerVariable(FS_AGGREGATE_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(aggregate.getReturnDataType());
				aggregateValueArguments.add(newVariable);
				newRuleHeadArguments.add(newVariable);
				
				// Old FS Value
				newVariable = new CompilerVariable(OLD_FS_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(aggregate.getReturnDataType());
				oldValueArguments.add(newVariable);
				
				newVariable = new CompilerVariable(IS_NEW_MAX_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(DataType.INT);
				isNewMaxValueArguments.add(newVariable);

				FSAggregate fsAggregate = (FSAggregate)argument;
				CompilerTypeBase aggTerm = fsAggregate.getAggregateTerm();
				if (fsAggregate.getFSAggregateType() == FSAggregateType.FSCNT) {
					if (aggTerm.isFunctor()) {
						// if the 2nd argument of the functor is a functor, we're expected calculate the multiplicity
						// add a variable that will be assigned the value from the read aggregate predicate and replace this functor 
						if (((CompilerFunctor)aggTerm).getArgument(1) instanceof CompilerFunctor) {
							newVariable = new CompilerVariable(MULTIPLICITY_VALUE_PREFIX + (multiplicityValueArguments.size() + 1));
							multiplicityValueArguments.add(newVariable);
							CompilerFunctor newAggTerm = (CompilerFunctor)aggTerm.copy(variableList);
							newAggTerm.getArguments().set(1,newVariable);
							aggregateArguments.add(new Pair<>(FSAggregateType.FSCNT, (CompilerTypeBase)newAggTerm));
						} else {
							aggregateArguments.add(new Pair<>(FSAggregateType.FSCNT, aggTerm.copy(variableList)));
						}
					} else if (aggTerm.isVariable()) {
						// TODO fix the case for companycontrol, where fscnt<P> could work
						CompilerTypeBase one;
						switch(this.deALSContext.getConfiguration().getCountDataType()) {
							case LONG:
								one = new CompilerLong(1);
								break;
							case LONGLONG:
								one = new CompilerLongLong(1);
								break;
							case LONGLONGLONGLONG:
								one = new CompilerLongLongLongLong(1);
								break;
							default:
								one = new CompilerInt(1);								
						}
						aggregateArguments.add(new Pair<>(FSAggregateType.FSCNT, 
								(CompilerTypeBase)CompilerFunctor.createFunctor(new CompilerTypeBase[]{aggTerm.copy(variableList), one})));
					} else {
						throw new CompilerException("Invalid argument found in fscnt<> aggregate");
					}
				} else {
					aggregateArguments.add(new Pair<>(fsAggregate.getFSAggregateType(), aggTerm.copy(variableList)));
				}
				
				switch (fsAggregate.getFSAggregateType()) {
				case FSMAX:
					fsAggregateTypeCounter[0]++;
					break;
				case FSMIN:
					fsAggregateTypeCounter[1]++;
					break;
				case FSCNT:
					fsAggregateTypeCounter[2]++;
					break;
				case FSSUM:
					fsAggregateTypeCounter[3]++;
				}
				
			} else {
				newRuleHeadArguments.add(argument.copy(variableList));
				nonAggregateArguments.add(argument);
			}
		}
		
		// first add the read table predicate
		CompilerTypeList readAggregatePredicateArgs = new CompilerTypeList();
		AggregateRewriter.appendAggregateArguments(readAggregatePredicateArgs, nonAggregateArguments, variableList, true);
		
		// add the arguments for the table
		for (int i = 0; i < aggregateCount; i++) {
			Pair<FSAggregateType, CompilerTypeBase> arg = aggregateArguments.get(i);
			readAggregatePredicateArgs.add(arg.getSecond().copy(variableList));
		}
		
		AggregateRewriter.appendAggregateArguments(readAggregatePredicateArgs, oldValueArguments, variableList, false);

		readAggregatePredicateArgs.add(0, new CompilerString("read"));
		String aggregatePredicateName = determineFSAggregateType(fsAggregateTypeCounter).name().toLowerCase() + rule.getHead().getPredicateName() + "_" + rule.getRuleId();

		BuiltInPredicate readAggregatePredicate = new BuiltInPredicate(aggregatePredicateName, readAggregatePredicateArgs, 
				BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS);
		readAggregatePredicate.setFSAggregateType(determineFSAggregateType(fsAggregateTypeCounter));
		newRuleBody.add(readAggregatePredicate);
		newDerivedPredicatePredicate = readAggregatePredicate.copy();
		
		for (int i = 0; i < aggregateCount; i++) {			
			BuiltInPredicate aggregateFunctionPredicate = null;
			
			CompilerTypeList args = new CompilerTypeList();
			
			if (aggregateArguments.get(i).getFirst() == FSAggregateType.FSMAX) {
				args.add(aggregateArguments.get(i).getSecond().copy(variableList));
				args.add(oldValueArguments.get(i));

				if (aggregateCount > 1) {
					args.add(isNewMaxValueArguments.get(i));
					aggregateFunctionPredicate = new BuiltInPredicate(BuiltInPredicate.FS_MAX_PREDICATE_NAME, 
							args, BuiltInPredicateType.FS_MAX);
				} else {
					aggregateFunctionPredicate = new BuiltInPredicate(BuiltInPredicate.GREATER_THAN_PREDICATE_NAME, 
						args, BuiltInPredicateType.BINARY);
				}
			} else if (aggregateArguments.get(i).getFirst() == FSAggregateType.FSMIN) {
				args.add(aggregateArguments.get(i).getSecond().copy(variableList));
				args.add(oldValueArguments.get(i));
				if (aggregateCount > 1)
					args.add(isNewMaxValueArguments.get(i));
								
				aggregateFunctionPredicate = new BuiltInPredicate(BuiltInPredicate.LESS_THAN_PREDICATE_NAME, 
					args, BuiltInPredicateType.BINARY);
			} else if (aggregateArguments.get(i).getFirst() == FSAggregateType.FSCNT) {
				// get the 2nd argument in the functor as it has the value to aggregate 
				CompilerFunctor func = (CompilerFunctor)aggregateArguments.get(i).getSecond();
				args.add(func.getArgument(1).copy(variableList));
				args.add(oldValueArguments.get(i));

				if (aggregateCount > 1) {
					args.add(isNewMaxValueArguments.get(i));
					aggregateFunctionPredicate = new BuiltInPredicate(BuiltInPredicate.FS_MAX_PREDICATE_NAME, args, BuiltInPredicateType.FS_MAX);
				} else {
					aggregateFunctionPredicate = new BuiltInPredicate(BuiltInPredicate.GREATER_THAN_PREDICATE_NAME, args, BuiltInPredicateType.BINARY);
				}
			}
			
			newRuleBody.add(aggregateFunctionPredicate);
		}
		
		// add the isNewMax predicate to ensure we have a new max before entering the write aggregate predicate
		if (aggregateCount > 1) {
			CompilerTypeList isNewMaxArgs = new CompilerTypeList();		
			AggregateRewriter.appendAggregateArguments(isNewMaxArgs, isNewMaxValueArguments, variableList, false);		
			BuiltInPredicate isNewMaxPredicate = new BuiltInPredicate(BuiltInPredicate.FS_IS_NEW_MAX_PREDICATE_NAME, 
					isNewMaxArgs, BuiltInPredicateType.FS_IS_NEW_MAX);
					
			newRuleBody.add(isNewMaxPredicate);
		}
		
		// add the write table predicate to retrieve value from table
		CompilerTypeList writeAggregatePredicateArgs = new CompilerTypeList();
		writeAggregatePredicateArgs.add(0, new CompilerString("write"));
		AggregateRewriter.appendAggregateArguments(writeAggregatePredicateArgs, nonAggregateArguments, variableList, true);

		for (Pair<FSAggregateType, CompilerTypeBase> aggArgument : aggregateArguments) {
			if (aggArgument.getFirst() == FSAggregateType.FSCNT) {
				CompilerFunctor func = (CompilerFunctor)aggArgument.getSecond();
				writeAggregatePredicateArgs.add(func);
			} else {
				writeAggregatePredicateArgs.add(aggArgument.getSecond());
			}
		}

		AggregateRewriter.appendAggregateArguments(writeAggregatePredicateArgs, aggregateValueArguments, variableList, false);
		BuiltInPredicate writeAggregatePredicate = new BuiltInPredicate(aggregatePredicateName, writeAggregatePredicateArgs, 
				BuiltInPredicateType.WRITE_USER_DEFINED_AGGREGATE_FS);
		writeAggregatePredicate.setFSAggregateType(determineFSAggregateType(fsAggregateTypeCounter));

		newRuleBody.add(writeAggregatePredicate);
		
		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArguments);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
		newRuleHead.setFSAggregateType(determineFSAggregateType(fsAggregateTypeCounter));
		
		Rule newRule = new Rule(rule.getRuleId() + ".2", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}
	
	private Pair<Rule, Predicate> rewriteFSAggregateRulePipelined(Rule rule) {
		CompilerVariableList variableList = new CompilerVariableList();
		
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();

		CompilerTypeList newRuleHeadArgs = new CompilerTypeList();
		CompilerTypeList returnValueArgs = new CompilerTypeList();
		
		List<Predicate> newRuleBody = new ArrayList<>();
		int[] fsAggregateTypeCounter = new int[4];
		for (int i = 0; i < 4; i++)
			fsAggregateTypeCounter[i] = 0;
		
		int aggregateCount = 0;
		FSAggregate aggregate;	
		
		// first, go through the head and determine what each argument does
		// initialize variable we will use throughout this procedure
		for (CompilerTypeBase argument : rule.getHead().getArguments()) {
			if (argument.containsFSAggregate()) {
				aggregateCount++;
				aggregate = (FSAggregate)argument;				
				
				CompilerVariable newVariable = new CompilerVariable(FS_AGGR_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(aggregate.getReturnDataType());
				returnValueArgs.add(newVariable);

				FSAggregate fsAggregate = (FSAggregate)argument;
				
				switch (fsAggregate.getFSAggregateType()) {
				case FSMAX:
					fsAggregateTypeCounter[0]++;
					break;
				case FSMIN:
					fsAggregateTypeCounter[1]++;
					break;
				case FSCNT:
					fsAggregateTypeCounter[2]++;
					break;
				case FSSUM:
					fsAggregateTypeCounter[3]++;
				}
			} else if (argument.isConstant()){
				// make sure the constants are the same object
				newRuleHeadArgs.add(argument);
			} else {
				newRuleHeadArgs.add(argument.copy(variableList));
			}
		}
		
		newRuleHeadArgs.appendList(returnValueArgs);
		
		String aggregatePredicateName = FS_AGGREGATE_NODE_NAME_PREFIX + rule.getHead().getPredicateName();
		aggregatePredicateName += "_" + rule.getRuleId();
		
		CompilerTypeList aggregatePredicateArguments = rule.getHead().getArguments().copy(variableList);
		aggregatePredicateArguments.appendList(returnValueArgs);
		
		for (int i = 0; i < rule.getHead().getArguments().size(); i++)
			if (rule.getHead().getArgument(i).isConstant())
				aggregatePredicateArguments.set(i, rule.getHead().getArgument(i));

		for (int i = 0; i < aggregatePredicateArguments.size(); i++) {
			if (aggregatePredicateArguments.get(i).isFSAggregate()) {
				FSAggregate fsAggr = (FSAggregate)aggregatePredicateArguments.get(i);
				if ((fsAggr.getFSAggregateType() == FSAggregateType.FSCNT) 
						&& (fsAggr.getAggregateTerm().isVariable())) {
					CompilerVariable var = (CompilerVariable)fsAggr.getAggregateTerm();
					CompilerTypeBase one;
					switch(this.deALSContext.getConfiguration().getCountDataType()) {
						case LONG:
							one = new CompilerLong(1);
							break;
						case LONGLONG:
							one = new CompilerLongLong(1);
							break;
						case LONGLONGLONGLONG:
							one = new CompilerLongLongLongLong(1);
							break;
						default:
							one = new CompilerInt(1);								
					}
					fsAggr.setAggregateTerm(CompilerFunctor.createFunctor(new CompilerTypeBase[]{var.copy(variableList), one}));
					break;
				}
			}
		}
		
		BuiltInPredicate aggregatePredicate = new BuiltInPredicate(aggregatePredicateName, aggregatePredicateArguments, 
				BuiltInPredicateType.AGGREGATE_FS);
		
		aggregatePredicate.setFSAggregateType(determineFSAggregateType(fsAggregateTypeCounter));
		newRuleBody.add(aggregatePredicate);
		
		newDerivedPredicatePredicate = aggregatePredicate.copy();
		
		// after copy, make sure the constants are the same object
		for (int i = 0; i < aggregatePredicate.getArity(); i++)
			if (aggregatePredicate.getArgument(i).isConstant())
				newDerivedPredicatePredicate.getArguments().set(i, aggregatePredicate.getArgument(i));

		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArgs);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
		newRuleHead.setFSAggregateType(determineFSAggregateType(fsAggregateTypeCounter));
		
		Rule newRule = new Rule(rule.getRuleId() + ".2", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}
	
	private Pair<Rule, Predicate> rewriteFSAggregateRuleMaterialized(Rule rule) {
		CompilerVariableList variableList = new CompilerVariableList();
		
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();

		CompilerTypeList newRuleHeadArgs = new CompilerTypeList();
		CompilerTypeList returnValueArgs = new CompilerTypeList();
		
		List<Predicate> newRuleBody = new ArrayList<>();
		int[] fsAggregateTypeCounter = new int[4];
		for (int i = 0; i < 4; i++)
			fsAggregateTypeCounter[i] = 0;
		
		int aggregateCount = 0;
		FSAggregate aggregate;	
		
		// first, go through the head and determine what each argument does
		// initialize variable we will use throughout this procedure
		for (CompilerTypeBase argument : rule.getHead().getArguments()) {
			if (argument.containsFSAggregate()) {
				aggregateCount++;
				aggregate = (FSAggregate)argument;				
				
				CompilerVariable newVariable = new CompilerVariable(FS_AGGR_VALUE_PREFIX + aggregateCount);
				newVariable.setDataType(aggregate.getReturnDataType());
				returnValueArgs.add(newVariable);

				FSAggregate fsAggregate = (FSAggregate)argument;
				
				switch (fsAggregate.getFSAggregateType()) {
				case FSMAX:
					fsAggregateTypeCounter[0]++;
					break;
				case FSMIN:
					fsAggregateTypeCounter[1]++;
					break;
				case FSCNT:
					fsAggregateTypeCounter[2]++;
					break;
				case FSSUM:
					fsAggregateTypeCounter[3]++;
				}
			} else {
				newRuleHeadArgs.add(argument.copy(variableList));
			}
		}
		
		newRuleHeadArgs.appendList(returnValueArgs);
		
		String aggregatePredicateName = FS_AGGREGATE_NODE_NAME_PREFIX + rule.getHead().getPredicateName();
		aggregatePredicateName += "_" + rule.getRuleId();
		
		CompilerTypeList aggregatePredicateArguments = rule.getHead().getArguments().copy(variableList);
		aggregatePredicateArguments.appendList(returnValueArgs);
				
		for (int i = 0; i < aggregatePredicateArguments.size(); i++) {
			if (aggregatePredicateArguments.get(i).isFSAggregate()) {
				FSAggregate fsAggr = (FSAggregate)aggregatePredicateArguments.get(i);
				if ((fsAggr.getFSAggregateType() == FSAggregateType.FSCNT) 
						&& (fsAggr.getAggregateTerm().isVariable())) {
					CompilerVariable var = (CompilerVariable)fsAggr.getAggregateTerm();
					CompilerTypeBase one;
					switch(this.deALSContext.getConfiguration().getCountDataType()) {
						case LONG:
							one = new CompilerLong(1);
							break;
						case LONGLONG:
							one = new CompilerLongLong(1);
							break;
						case LONGLONGLONGLONG:
							one = new CompilerLongLongLongLong(1);
							break;
						default:
							one = new CompilerInt(1);								
					}
					fsAggr.setAggregateTerm(CompilerFunctor.createFunctor(new CompilerTypeBase[]{var.copy(variableList), one}));
					break;
				}
			}
		}
		
		BuiltInPredicate aggregatePredicate = new BuiltInPredicate(aggregatePredicateName, rule.getHead().getArguments().copy(), 
				BuiltInPredicateType.AGGREGATE_FS);
		
		aggregatePredicate.setFSAggregateType(determineFSAggregateType(fsAggregateTypeCounter));
		newRuleBody.add(aggregatePredicate);
		
		newDerivedPredicatePredicate = aggregatePredicate.copy();

		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArgs);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
		newRuleHead.setFSAggregateType(determineFSAggregateType(fsAggregateTypeCounter));
		
		Rule newRule = new Rule(rule.getRuleId() + ".2", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}
	
	private static FSAggregateType determineFSAggregateType(int[] fsAggregateTypeCounter) {
		int totalCount = 0;
		for (int i = 0; i < fsAggregateTypeCounter.length; i++)
			totalCount += fsAggregateTypeCounter[i];
		
		if (totalCount > 1)
			return FSAggregateType.FSMANY;
		
		if (fsAggregateTypeCounter[0] > 0)
			return FSAggregateType.FSMAX;
		
		if (fsAggregateTypeCounter[1] > 0)
			return FSAggregateType.FSMIN;
		
		if (fsAggregateTypeCounter[2] > 0)
			return FSAggregateType.FSCNT;
		
		if (fsAggregateTypeCounter[3] > 0)
			return FSAggregateType.FSSUM;
		
		return FSAggregateType.NONE;
	}

	/*APS added 3/6/2013
	 * If we are rewriting rules before RPCG extraction, we will have have two versions of the same rule.  
	 * Remove the original rule, since it was rewritten */
	public int removeRewrittenRules(Module module) {
		this.deALSContext.logTrace(logger, "Entering removeRewrittenRules");
		
		int numberOfRulesRemoved = 0;
		for (DerivedPredicate derivedPredicate : module.getDerivedPredicates()) {
			for (int j = derivedPredicate.getNumberOfRules() - 1; j >= 0; j--) {
				if (derivedPredicate.getRule(j).isRewritten()) {
					this.deALSContext.logDebug(logger, "Removing Derived Rule :");
					this.deALSContext.logDebug(logger, "{}", derivedPredicate.getRule(j));

					derivedPredicate.removeRule(j);
					numberOfRulesRemoved++;
				}
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting removeRewrittenRules");
		
		return numberOfRulesRemoved;
	}
	
	public void expandFSRules(Module module) {		
		this.deALSContext.logInfo(logger, "\n[BEGIN expand FS rules in Module '{}' BEGIN]", module.getModuleName());
		// if we are materializing fs aggregates, there is no need to generate sealing aggregate rules
		//if (!this.deALSConfiguration.getConfiguration().compareProperty("deals.aggregates.fs.materialize", "true"))
		//	AggregateRewriter.generateSealingAggregateRules(module);
		// this.rewriteFSSumRules(module);
		
		this.deALSContext.logInfo(logger, "[END expand FS rules in Module '{}' END]\n", module.getModuleName());
	}
	
	private void rewriteFSSumRules(Module module) {
		// rewrite all fssum rules into fscnt rules
		for (int i = 0; i < module.getDerivedPredicates().size(); i++) { 
			DerivedPredicate derivedPredicate = module.getDerivedPredicates().get(i);
			for (Rule rule : derivedPredicate.getRules()) {
				for (CompilerTypeBase argument : rule.getHead().getArguments()) {
					if (argument.isFSAggregate() 
							&& (((FSAggregate)argument).getFSAggregateType() == FSAggregateType.FSSUM))
						this.rewriteFSSumRule(rule);
				}
			}
		}
	}
	
	private void rewriteFSSumRule(Rule rule) {
		String oldHead = rule.getHead().toString();
		
		// to rewrite this rule using fscnt, we need to replace the fssum aggregate 
		//   in the head with an fscnt one as follows:
		// 1) change the fssum fsaggregate argument in the head from an 'fssum' to an 'fscnt'
		// 2) add predicate (N > 0), where N is the 2nd part of the fscnt aggregate term to ensure only for positive numbers are added
		for (int i = 0; i < rule.getHead().getArity(); i++) {
			CompilerTypeBase argument = rule.getHead().getArgument(i);
			if (argument.isFSAggregate()) {
				FSAggregate fsAggregate = (FSAggregate)argument;
				if (fsAggregate.getFSAggregateType() == FSAggregateType.FSSUM) {
					FSAggregate fscnt = new FSAggregate(FSAggregate.FSCNT_NAME, fsAggregate.getAggregateTerm());
					// replace previous aggregate
					rule.getHead().setArgument(i, fscnt);
					
					// add N > 0 predicate to ensure only positive numbers are aggregates					
					if (fsAggregate.getAggregateTerm().isFunctor() 
							&& ((CompilerFunctor)fsAggregate.getAggregateTerm()).getArity() == 2) {
						CompilerFunctor functor = (CompilerFunctor)fsAggregate.getAggregateTerm();
						CompilerTypeList positiveNumberEnforcementPredicateArguments = new CompilerTypeList();
						positiveNumberEnforcementPredicateArguments.add(functor.getArgument(1));
						positiveNumberEnforcementPredicateArguments.add(new CompilerInt(0));
						Predicate positiveNumberEnforcementPredicate = new BuiltInPredicate(BuiltInPredicate.GREATER_THAN_PREDICATE_NAME, 
								positiveNumberEnforcementPredicateArguments, BuiltInPredicateType.BINARY);

						rule.getBody().add(positiveNumberEnforcementPredicate);
					}
				}
			}
		}
		this.deALSContext.logInfo(logger, "Rewrote fssum rule from {} to {}", oldHead, rule.getHead());
	}
	
	public static void optimizeMonotonicRules(Module module) {
		// if we find fs rules that
		List<Rule> rulesToRewrite = new ArrayList<>();

		for (int i = 0; i < module.getDerivedPredicates().size(); i++) {
			// find derived relation that uses 
			DerivedPredicate derivedPredicate = module.getDerivedPredicates().get(i);
			for (Rule rule : derivedPredicate.getRules()) {
				if (rule.getMonotonicRuleType() == MonotonicRuleType.N2FS
						|| rule.getMonotonicRuleType() == MonotonicRuleType.FS2FS) {
					for (int j = 0; j < module.getDerivedPredicates().size(); j++) {						
						DerivedPredicate derivedPredicate2 = module.getDerivedPredicates().get(j);
						for (Rule rule2 : derivedPredicate2.getRules()) {
							// this would be the type of rule with a sealing aggregate
							if (rule2.getMonotonicRuleType() == MonotonicRuleType.FS2N) {
								for (Predicate literal : rule2.getBody()) {
									if (literal.getPredicateName().equals(rule.getHead().getPredicateName()) 
											&& literal.getArity() == rule.getHead().getArity()) {
										if (rule2.getHead().getArgumentTypeAdornment().get(rule2.getHead().getArity() - 1) == ArgumentType.AGGREGATE) {
											BuiltInAggregate bia = (BuiltInAggregate)rule2.getHead().getArgument(rule2.getHead().getArity() - 1);
											if (bia.getAggregateTerm().isVariable()) {
												if (AggregateRewriter.isMonotonicAggregateValue(rule2, (CompilerVariable)bia.getAggregateTerm()))
													if (!rulesToRewrite.contains(rule2))
														rulesToRewrite.add(rule2);
													break;												
											}
										}										
									}
								}
							}
						}
					}					
				}
			}
		}
		
		System.out.println("Replacing Stratified Aggregate rules with Negation rules for Eager Monotonic");
		for (int i = rulesToRewrite.size()- 1; i >=0; i--) {
			System.out.println(rulesToRewrite.get(i));
		
			Rule rule = rulesToRewrite.get(i);
			CompilerTypeList originalVariables = new CompilerTypeList();
			// replace aggregate in head
			for (int j = 0; j < rule.getHead().getArity(); j++) {
				if (rule.getHead().getArgumentTypeAdornment().get(j) == ArgumentType.AGGREGATE) {
					originalVariables.add(((BuiltInAggregate)rule.getHead().getArgument(j)).getAggregateTerm());
					rule.getHead().setArgument(j, ((BuiltInAggregate)rule.getHead().getArgument(j)).getAggregateTerm());
				} else {
					originalVariables.add(rule.getHead().getArgument(j));
				}				
			}
			Predicate originalHead = rule.getHead().copy();
			// rename this rule with a "2"
			rule.getHead().setPredicateName(rule.getHead().getPredicateName() + "2");
			// add false predicate to fail the rule 
			rule.getBody().add(new BuiltInPredicate(BuiltInPredicate.FALSE_PREDICATE_NAME, new CompilerTypeList(), BuiltInPredicateType.FALSE));
			
			List<Predicate> newOuterRuleBody = new ArrayList<>();
			
			CompilerTypeList annonymousVariables = new CompilerTypeList();
			for (int a = 0; a < rule.getHead().getArity(); a++)
				annonymousVariables.add(new CompilerVariable("_"));
			
			// first, add negated literal to stratify the fs clique
			Predicate negatedLiteral = new Predicate(rule.getHead().getPredicateName(), annonymousVariables);
			negatedLiteral.setAsNegative();			
			newOuterRuleBody.add(negatedLiteral);
			
			// now add the literal to read the materialized fs clique 
			for (Predicate literal : rule.getBody()) {
				for (int a = 0; a < literal.getArity(); a++) {
					if (literal.getArgumentTypeAdornment().get(a) == ArgumentType.FSAGGREGATE)
						newOuterRuleBody.add(literal);					
				}
			}
			
			Rule newOuterRule = new Rule(rule.getRuleId() + ".1", originalHead, newOuterRuleBody);
			DerivedPredicate newDerivedPredicate = new DerivedPredicate(newOuterRule.getHead().getPredicateName(), newOuterRule.getHead().getArity());
			newDerivedPredicate.addRule(newOuterRule);
			
			module.getDerivedPredicates().add(newDerivedPredicate);
			
			rulesToRewrite.remove(i);
		}
	}

	private static boolean isMonotonicAggregateValue(Rule rule, CompilerVariable aggregateTerm) {
		return true;
	}

	/* BEGIN HELPERS */	
	public static String toStringRules(Module module) {
		StringBuilder retval = new StringBuilder();

		int counter = 0;
		for (DerivedPredicate derivedPredicate : module.getDerivedPredicates()) {
			if (counter > 0) retval.append("\n");
			retval.append(derivedPredicate.toString() + "\n");
			counter++;
		}
		return retval.toString();
	}
	
	private static void appendAggregateArguments(CompilerTypeList argumentsToAppendTo, CompilerTypeList argumentsToAppendFrom, 
			CompilerVariableList variableList, boolean nonAggregateArguments) {		
		for (CompilerTypeBase argument : argumentsToAppendFrom)
			if (nonAggregateArguments) 	
				argumentsToAppendTo.add(argument.copy(variableList));
			else
				argumentsToAppendTo.add(argument);
	}

	private static void appendAggregateTermArguments(CompilerTypeList argumentsToAppendTo, int aggregateCount, 
			CompilerTypeList aggregateArguments, CompilerVariableList variableList) {
		for (int i = 0; i < aggregateCount; i++) {
			CompilerTypeBase argument = aggregateArguments.get(i);
			argumentsToAppendTo.add(((Aggregate)argument).getAggregateTerm().copy(variableList));
		}
	}
	/* END HELPERS */
}
