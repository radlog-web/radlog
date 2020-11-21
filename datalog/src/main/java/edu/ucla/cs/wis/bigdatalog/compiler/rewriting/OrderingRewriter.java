package edu.ucla.cs.wis.bigdatalog.compiler.rewriting;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.*;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class OrderingRewriter {
	private static Logger logger = LoggerFactory.getLogger(OrderingRewriter.class.getName());
	
	public static String SORT_NODE_NAME_PREFIX = "sort";
	public static String TOPK_NODE_NAME_PREFIX = "topK";
	private DeALSContext deALSContext;
	
	public OrderingRewriter(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;		
	}
	
	public void rewriteOrderingRules(Module module) {
		this.deALSContext.logTrace(logger, "Entering rewriteOrderingRules for {}", module.getModuleName());

		this.deALSContext.logInfo(logger, "\n[BEGIN Rewrite Ordering Rules for Module '{}' BEGIN]", module.getModuleName());
		this.deALSContext.logInfo(logger, "\n[BEGIN Rules Before Rewriting BEGIN]");
		this.deALSContext.logInfo(logger, AggregateRewriter.toStringRules(module));
		this.deALSContext.logInfo(logger, "[END Rules Before Rewriting END]\n");
		
		// this is for info below
		//List<Pair<Rule, Rule>> rewrittenRules = new ArrayList<>();
		List<Rule> newRules = new ArrayList<>();	
		List<DerivedPredicate> newDerivedPredicates = new ArrayList<>();
		
		for (DerivedPredicate derivedPredicate : module.getDerivedPredicates()) {
			int numberOfRules = derivedPredicate.getNumberOfRules();
			for (int j = 0; j < numberOfRules; j++)
				rewriteRule(derivedPredicate, derivedPredicate.getRule(j), newRules, newDerivedPredicates);	
		}
		
		for (DerivedPredicate newDerivedPredicate : newDerivedPredicates) 
			module.getDerivedPredicates().add(newDerivedPredicate);
				
		int numberOfRulesRemoved = new AggregateRewriter(this.deALSContext).removeRewrittenRules(module);
				
		if (this.deALSContext.isInfoEnabled()) {
			this.deALSContext.logInfo(logger, "\n[BEGIN Rewrite Ordering Rules After Rewriting BEGIN]");
			if (numberOfRulesRemoved > 0)
				this.deALSContext.logInfo(logger, AggregateRewriter.toStringRules(module));
			else
				this.deALSContext.logInfo(logger, "NO RULES REWRITTEN!");
			
			this.deALSContext.logInfo(logger, "[END Rules After Rewriting END]\n");
			this.deALSContext.logInfo(logger, "[END Rewrite Ordering Rules for Module '{}' END]\n", module.getModuleName());
		}
		
		this.deALSContext.logTrace(logger, "Exiting rewriteOrderingRules for {}", module.getModuleName());
	}
	
	private static void rewriteRule(DerivedPredicate derivedPredicate, Rule rule, List<Rule> newRules, 
			List<DerivedPredicate> newDerivedPredicates) {		
		//int ruleType = 0;
		List<Predicate> newRuleBody = new ArrayList<>();
		// although they are specified in the same rule, limit must be applied after sort 
		boolean hasSort = false;
		boolean hasLimit = false;
		for (Predicate literal : rule.getBody()) {
			if (literal.isBuiltIn() && ((BuiltInPredicate)literal).getBuiltInPredicateType() == BuiltInPredicateType.SORT)
				hasSort = true;
			else if (literal.isBuiltIn() && ((BuiltInPredicate)literal).getBuiltInPredicateType() == BuiltInPredicateType.LIMIT) 
				hasLimit = true;
			else
				newRuleBody.add(literal);
		}
		
		Pair<Rule, Predicate> retvalPair = null;
		if (hasSort) {
			if (!hasLimit)
				retvalPair = rewriteSortRule(rule);
			else
				retvalPair = rewriteTopKRule(rule);
		}			
				
		if (retvalPair != null) {
			rule.setRewritten(true);

			// add expanded rule to derived rule - this rule will replace the original
			derivedPredicate.addRule(retvalPair.getFirst());
	
			// create new rule for body of original rule
			Predicate newPredicate = retvalPair.getSecond();	
			Predicate newHeadPredicate = retvalPair.getSecond().copy();

			Rule newRule = new Rule(rule.getRuleId() + ".1", newHeadPredicate, newRuleBody);
			newRule.setResultOfRewrite(true);
			newRules.add(newRule);		
		
			DerivedPredicate existingDP = null;
			for (DerivedPredicate dp : newDerivedPredicates) {
				if ((dp.getArity() == newPredicate.getArity())
						&& (dp.getPredicateName().equals(newPredicate.getPredicateName()))) {
					existingDP = dp;
					break;
				}
			}
		
			if (existingDP == null) {
				DerivedPredicate newDerivedPredicate = new DerivedPredicate(newPredicate.getPredicateName(), 
						newPredicate.getArity());
				
				newDerivedPredicate.addRule(newRule);
				newDerivedPredicates.add(newDerivedPredicate);	
			} else {
				existingDP.addRule(newRule);
			}
		}
	}
	
	private static Pair<Rule, Predicate> rewriteSortRule(Rule rule) {
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();
		
		CompilerTypeList newRuleHeadArguments = new CompilerTypeList();
		CompilerTypeList newSortPredicateArguments = new CompilerTypeList();
		List<Predicate> newRuleBody = new ArrayList<>();
		List<Predicate> newSortRuleBody = new ArrayList<>();
		Predicate sortPredicate = null;
		//Predicate limitPredicate = null;
		
		for (Predicate literal : rule.getBody()) {
			if (literal.isBuiltIn() && ((BuiltInPredicate)literal).getBuiltInPredicateType() == BuiltInPredicateType.SORT)
				sortPredicate = literal;
			/*else if (hasLimit && (literal.isBuiltIn() && ((BuiltInPredicate)literal).getBuiltInPredicateType() == BuiltInPredicateType.LIMIT))
				limitPredicate = literal;*/
			else
				newSortRuleBody.add(literal);
		}
		
		for (int i = 0; i < head.getArity(); i++) {
			newRuleHeadArguments.add(head.getArgument(i));
			newSortPredicateArguments.add(head.getArgument(i));
		}
		
		BuiltInPredicate newSortPredicate = new BuiltInPredicate(OrderingRewriter.SORT_NODE_NAME_PREFIX + "_" + head.getPredicateName() + rule.getRuleId(), 
				newSortPredicateArguments, BuiltInPredicateType.SORT);
		
		CompilerTypeList ordinalSortArguments = new CompilerTypeList();
		for (CompilerTypeBase sortArgument : sortPredicate.getArguments()) {
			if (!newSortPredicateArguments.contains(((CompilerFunctor)sortArgument).getArgument(0)))
				newSortPredicateArguments.add(((CompilerFunctor)sortArgument).getArgument(0));
			
			int ordinalPosition = 0;
			for (CompilerTypeBase arg : newSortPredicateArguments) {
				if (arg == ((CompilerFunctor)sortArgument).getArgument(0))
					break;
				ordinalPosition++;
			}
			
			CompilerFunctor ordinalSortArgument = CompilerFunctor.createFunctor(new CompilerTypeBase[]{new CompilerInt(ordinalPosition), 
					((CompilerFunctor)sortArgument).getArgument(1)});
			ordinalSortArguments.add(ordinalSortArgument);
		}

		CompilerList sortArguments = new CompilerList(ordinalSortArguments);
		
		newSortPredicateArguments.add(sortArguments);
		newDerivedPredicatePredicate = newSortPredicate.copy();
		newRuleBody.add(newSortPredicate);
		
		// limitPredicate will be non null if we found a limit predicate while doing sort rewrite
		// limitPredicate without sort will be ignored
		//if (limitPredicate != null)	newRuleBody.add(limitPredicate);
			
		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArguments);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
			
		Rule newRule = new Rule(rule.getRuleId() + ".3", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		//System.out.println(newRule);
		
		//System.out.println(newDerivedPredicatePredicate);
		
		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}

	private static Pair<Rule, Predicate> rewriteTopKRule(Rule rule) {
		Predicate newDerivedPredicatePredicate;
		Predicate head = rule.getHead();
		
		CompilerTypeList newRuleHeadArguments = new CompilerTypeList();
		CompilerTypeList newTopKPredicateArguments = new CompilerTypeList();
		List<Predicate> newRuleBody = new ArrayList<>();
		List<Predicate> newSortRuleBody = new ArrayList<>();
		Predicate sortPredicate = null;
		Predicate limitPredicate = null;
		
		for (Predicate literal : rule.getBody()) {
			if (literal.isBuiltIn() && ((BuiltInPredicate)literal).getBuiltInPredicateType() == BuiltInPredicateType.SORT)
				sortPredicate = literal;
			else if (literal.isBuiltIn() && ((BuiltInPredicate)literal).getBuiltInPredicateType() == BuiltInPredicateType.LIMIT)
				limitPredicate = literal;
			else
				newSortRuleBody.add(literal);
		}
		
		for (int i = 0; i < head.getArity(); i++) {
			newRuleHeadArguments.add(head.getArgument(i));
			newTopKPredicateArguments.add(head.getArgument(i));
		}
		
		BuiltInPredicate newTopKPredicate = new BuiltInPredicate(OrderingRewriter.TOPK_NODE_NAME_PREFIX + "_" + head.getPredicateName() + rule.getRuleId(), 
				newTopKPredicateArguments, BuiltInPredicateType.TOPK);
		
		CompilerTypeList ordinalSortArguments = new CompilerTypeList();
		for (CompilerTypeBase sortArgument : sortPredicate.getArguments()) {
			if (!newTopKPredicateArguments.contains(((CompilerFunctor)sortArgument).getArgument(0)))
				newTopKPredicateArguments.add(((CompilerFunctor)sortArgument).getArgument(0));
			
			int ordinalPosition = 0;
			for (CompilerTypeBase arg : newTopKPredicateArguments) {
				if (arg == ((CompilerFunctor)sortArgument).getArgument(0))
					break;
				ordinalPosition++;
			}
			
			CompilerFunctor ordinalSortArgument = CompilerFunctor.createFunctor(new CompilerTypeBase[]{new CompilerInt(ordinalPosition), 
					((CompilerFunctor)sortArgument).getArgument(1)});
			ordinalSortArguments.add(ordinalSortArgument);
		}

		CompilerList sortArguments = new CompilerList(ordinalSortArguments);
		
		newTopKPredicateArguments.add(sortArguments);
		
		newTopKPredicateArguments.add(limitPredicate.getArgument(0));
		
		newDerivedPredicatePredicate = newTopKPredicate.copy();
		newRuleBody.add(newTopKPredicate);
			
		Predicate newRuleHead = new Predicate(head.getPredicateName(), newRuleHeadArguments);
		newRuleHead.setArgumentTypeAdornment(head.getArgumentTypeAdornment());
			
		Rule newRule = new Rule(rule.getRuleId() + ".3", newRuleHead, newRuleBody);
		newRule.setResultOfRewrite(true);

		//System.out.println(newRule);
		
		//System.out.println(newDerivedPredicatePredicate);
		
		return new Pair<>(newRule, newDerivedPredicatePredicate);
	}
	
}
