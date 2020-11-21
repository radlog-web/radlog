package edu.ucla.cs.wis.bigdatalog.compiler.adornment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class AggregateAdorner {
	private static Logger logger = LoggerFactory.getLogger(AggregateAdorner.class.getName());

	private DeALSContext deALSContext;
	private List<DerivedPredicate> derivedPredicates;
	
	public AggregateAdorner(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	
	public void adorn(Module module) {		
		this.deALSContext.logInfo(logger, "Entering adorn for {}", module.getModuleName());
		this.deALSContext.logInfo(logger, "\n[BEGIN Adorn Aggregate Predicates in Module '{}' BEGIN]", module.getModuleName());
		
		this.derivedPredicates = module.getDerivedPredicates();
		
		// predicate name, adornment list
		Map<String, List<ArgumentType>> predicateAdornments = new HashMap<>();
		Map<String, FSAggregateType> fsAggregatePredicateMappings = new HashMap<>();
		Predicate predicate;
		
		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			for (Rule rule : derivedPredicate.getRules()) {
				predicate = rule.getHead();
				predicate.generateArgumentTypeAdornment();	
				predicate.determineFSAggregatePredicateType();
				fsAggregatePredicateMappings.put(predicate.getPredicateName(), predicate.getFSAggregateType());
				predicateAdornments.put(predicate.getPredicateName(), predicate.getArgumentTypeAdornment());
			}
		}
		
		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			for (Rule rule : derivedPredicate.getRules()) {
				for (Predicate literal : rule.getBody()) {
					if (predicateAdornments.containsKey(literal.getPredicateName())) {
						literal.setArgumentTypeAdornment(predicateAdornments.get(literal.getPredicateName()));
						literal.setFSAggregateType(fsAggregatePredicateMappings.get(literal.getPredicateName()));
					}
				}
			}
		}
		
		this.deALSContext.logInfo(logger, "[END Adorn Aggregate Predicates in Module '{}' END]\n", module.getModuleName());
		this.deALSContext.logTrace(logger, "Exiting adorn for {}", module.getModuleName());
	}
}
