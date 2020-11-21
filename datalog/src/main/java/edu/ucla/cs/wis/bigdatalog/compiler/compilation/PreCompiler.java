package edu.ucla.cs.wis.bigdatalog.compiler.compilation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.adornment.AggregateAdorner;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.RuleAnalyzer;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.OrderingRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeChecker;
import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeInferrer;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class PreCompiler {
	private static Logger logger = LoggerFactory.getLogger(PreCompiler.class.getName());

	private DeALSContext deALSContext;
	
	public PreCompiler(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	
	public void prepareForCompilation(Module module) {
		//STEP 1 - reset derived predicates so we can processes them
		module.resetRecursiveDerivedPredicates();

		//STEP 2 - check that all predicates used are valid
		this.checkAllRulesValid(module);
		
		//STEP 3 - check that all variables in head will be assigned
		this.checkAllHeadVariablesValid(module);
		
		//STEP 4 - expand certain fs aggregate rules
		new AggregateRewriter(this.deALSContext).expandFSRules(module);
						
		//STEP 5 - starting from base predicates, set the type of all variables in rules so we can find them out later if needed 
		new TypeInferrer(this.deALSContext).inferTypes(module);
		if (this.deALSContext.getConfiguration().compareProperty("deals.compiler.typecheck", "true"))
			TypeChecker.checkTypes(module);
		
		//STEP 6 - set aggregate adornment for later usage
		new AggregateAdorner(this.deALSContext).adorn(module);
		
		//STEP 8 - verify aggregate rules
		this.checkAggregateRulesValid(module);
		
		//STEP 9 - optimize fs rules
		//aggregateRewriter.optimizeMonotonicRules();
		
		//STEP 10 - expand aggregate rules
		new AggregateRewriter(this.deALSContext).rewriteAggregateRules(module);
		
		//STEP 11 - expand sort rules
		new OrderingRewriter(this.deALSContext).rewriteOrderingRules(module);
		
		//STEP 12 - assign types again, in case rules were generated that need typing
		new TypeInferrer(this.deALSContext).inferTypes(module);
		
		//STEP 13 - generate cliques
		new RuleAnalyzer(this.deALSContext).generateCliques(module);
	}
	
	private void checkAllRulesValid(Module module) {
		this.deALSContext.logTrace(logger, "Entering checkAllRulesValid");		
		this.deALSContext.logInfo(logger, "\n[BEGIN Check All Rules Valid in Module '{}' BEGIN]", module.getModuleName());

		DerivedPredicate 	derivedPredicate;
		Rule 				rule;
		Predicate 			head;
		String 				predicateName;
		int 				arity;
		
		for (int i = module.getDerivedPredicates().size() - 1; i >= 0; i--) {
			derivedPredicate = module.getDerivedPredicates().get(i);

			for (int j = derivedPredicate.getNumberOfRules() - 1; j >= 0; j--) {
				rule = derivedPredicate.getRule(j);
				head = rule.getHead();

				// built in predicates cannot be redefined unless they are for defining UDAs
				if (head.isBuiltIn()) {
					BuiltInPredicate bip = (BuiltInPredicate)head;				
					if (//!bip.isEmpty() &&
							!bip.isSingle() &&
							!bip.isMulti() &&
							!bip.isReturn()) {
						String message = "Can not redefine built-in predicate '" + head.getPredicateName().toString() + "'"; 
						logger.error(message);
						throw new CompilerException(message);
					}
				}
				
				this.deALSContext.logInfo(logger, "[Predicate '{}' passed built-in predicate redefinition check]", head.toString());
				
				for (Predicate literal : rule.getBody()) {
					predicateName = literal.getPredicateName();
					arity = literal.getArity();

					// if it is not a built-in predicate
					if (!literal.isBuiltIn()) {
						// if it is not a derived predicate
						if (module.getDerivedPredicate(predicateName, arity) == null) {
							// if it is not a base predicate
							//if (this.deALSConfiguration.getModuleManager().getActiveModule().getBasePredicate(predicateName) == null) {
							if (module.getBasePredicate(predicateName) == null) {
								// it must be unknown
								String message = "Undefined literal '" + literal.toString() + "' in body"; 
								logger.error(message);
								throw new CompilerException(message);
							}
						}
					}
					
					this.deALSContext.logInfo(logger, "[Predicate '{}' passed derived predicate check]", literal.toString());
				}
			}
		}
		
		// APS 1/22/2014
		// check that anonymous variables are used where needed
		for (DerivedPredicate dr : module.getDerivedPredicates()) {
			HashMap<CompilerVariable, Integer> variableUsage = new HashMap<>();
			for (Rule r : dr.getRules()) {
				Utilities.getVariableCounts(r.getHead(), variableUsage);

				Utilities.getVariableCounts(r.getBody(), variableUsage);
								
				for (Map.Entry<CompilerVariable, Integer> entry : variableUsage.entrySet()) {
					if (!entry.getKey().isAnonymous() && entry.getValue() < 2) {
						String message = "Variable "+entry.getKey()+" used only once.  Use anonymous variable instead.";
						this.deALSContext.logWarn(logger, message);
						//throw new CompilerException(message);						
					}
				}
			}
		}
		
		this.deALSContext.logInfo(logger, "[END Check All Rules Valid in Module '{}' END]\n", module.getModuleName());		
		this.deALSContext.logTrace(logger, "Exiting checkAllRulesValid");
	}
	
	private void checkAllHeadVariablesValid(Module module) {
		this.deALSContext.logTrace(logger, "Entering checkAllHeadVariablesValid");
		this.deALSContext.logInfo(logger, "\n[BEGIN Check All Head Variables Valid in Module '{}' BEGIN]", module.getModuleName());

		DerivedPredicate 			derivedPredicate;
		Rule 						rule;
		Predicate 					head;
		List<Predicate>				body;
		CompilerVariableList 		headVariableList;
		CompilerVariableList 		bodyVariableList;
		
		for (int i = module.getDerivedPredicates().size() - 1; i >= 0; i--) {
			derivedPredicate = module.getDerivedPredicates().get(i);

			for (int j = derivedPredicate.getNumberOfRules() - 1; j >= 0; j--) {
				rule = derivedPredicate.getRule(j);
				if (rule.isResultOfRewrite()/* || rule.isRewritten()*/)
					continue;
				head = rule.getHead();
				body = rule.getBody();
				
				// hope the parser caught any missing but required bodies
				if (body.isEmpty())
					continue;
				
				if (head.isBuiltIn()) {
					// let user defined aggregates pass - they're sloppy 
					BuiltInPredicate bip = (BuiltInPredicate)head;
					if (bip.isMulti())
						continue;
				}
				
				headVariableList = new CompilerVariableList();
				Utilities.getVariables(head, headVariableList);
				
				for (int h = 0; h < headVariableList.size(); h++) {
					CompilerVariable headVariable = headVariableList.get(h);
					if (headVariable.isAnonymous())
						break;
					
					boolean found = false;
					for (Predicate goal : body) {
						bodyVariableList = new CompilerVariableList();
						Utilities.getVariables(goal, bodyVariableList);
						for (int b = 0; b < bodyVariableList.size(); b++) {
							if (bodyVariableList.get(b).equals(headVariable)) {
								found = true;
								break;
							}
						}
						if (found)
							break;
					}
					
					if (!found) {
						// one last check to see if list variables are trying to match heads
						List<Integer> countLists = new ArrayList<>();
						for (int l = 0; l < head.getArity(); l++) {
							if (head.getArgument(l).isList())
								countLists.add(l);
						}
						
						if (countLists.size() > 1) {
							for (int l = 0; l < countLists.size(); l++) {
								CompilerList list1 = (CompilerList)head.getArgument(l);
								for (int k = 1; k < countLists.size(); k++) {									
									CompilerList list2 = (CompilerList)head.getArgument(k);
									if ((list1.getHead() == headVariable) && (list2.getHead() == headVariable)) {
										found = true;
										break;
									}
								}
								if (found)
									break;
							}
						}						
						
						if (!found) {
							String message = "Can not compile rule " + head.toString() + ". Variable " + headVariable.getVariableName() + " in head is not found in body.";
							//logger.error(message);
							//throw new CompilerException(message);
						}
					}
				}
				
				this.deALSContext.logInfo(logger, "[Predicate '{}' passed head variable assignment check]", head.toString());
			}
		}

		this.deALSContext.logInfo(logger, "[END Check All Head Variables Valid in Module '{}' END]\n", module.getModuleName());
			
		this.deALSContext.logTrace(logger, "Exiting checkAllHeadVariablesValid");
	}
	
	private void checkAggregateRulesValid(Module module) {
		this.deALSContext.logTrace(logger, "Entering checkAggregateRulesValid");		
		this.deALSContext.logInfo(logger, "\n[BEGIN Check Aggregate Rules Valid in Module '{}' BEGIN]", module.getModuleName());

		List<ArgumentType> argumentAdornment;
		//STEP 1 - check that all rules in derived predicate have same arguments (for aggregates)
		for (int i = 0; i < module.getDerivedPredicates().size(); i++) {
			for (Rule rule : module.getDerivedPredicates().get(i).getRules()) {
				argumentAdornment = rule.getHead().getArgumentTypeAdornment();
				this.deALSContext.logInfo(logger, "{} : {}", rule.getHead().toString(), argumentAdornment.toString());
				
				for (int j = 0; j < argumentAdornment.size(); j++)
					if (argumentAdornment.get(j) == ArgumentType.FSAGGREGATE)
						verifyAllHeadsMatch(module.getDerivedPredicates().get(i), j);
			}
		}

		//STEP 2 - check that fs aggregate rules have valid functions in bodies
		/*for (DerivedPredicate derivedPredicate : module.derivedPredicates) {
			for (Rule rule : derivedPredicate.getRules()) {
				MonotonicRuleType ruleType = MonotonicityAnalyzer.analyzeFSRule(rule);
				if (Runtime.isInfoEnabled()) {
					this.deALSContext.logInfo(logger, "Rule Type: " + ruleType.name());
					this.deALSContext.logInfo(logger, rule.toString());
				}
				rule.setMonotonicRuleType(ruleType);
			}
		}*/
		
		this.deALSContext.logInfo(logger, "[END Check Aggregate Rules Valid in Module '{}' END]\n", module.getModuleName());			
		this.deALSContext.logTrace(logger, "Exiting checkAggregateRulesValid");
	}
	
	private static void verifyAllHeadsMatch(DerivedPredicate derivedPredicate, int positionOfFSAggregate) {
		for (Rule rule : derivedPredicate.getRules()) {
			if (rule.getHead().isGround())
				continue;
			List<ArgumentType> argumentAdornment = rule.getHead().getArgumentTypeAdornment();
			if (argumentAdornment.get(positionOfFSAggregate) != ArgumentType.FSAGGREGATE)
				throw new CompilerException("Derived predicates with an FS Aggregate Rules must have ALL FS Aggregate rules! Derived predicate [" + derivedPredicate.getPredicateName() + "|" + derivedPredicate.getArity() + "] is invalid.");			
		}
	}
	
}
