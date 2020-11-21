package edu.ucla.cs.wis.bigdatalog.compiler;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.BasicClique;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;

public class ProgramRules implements Serializable {
	private static final long serialVersionUID = 1L;
	protected List<BasePredicate> 		basePredicates;
	protected List<DerivedPredicate> 	derivedPredicates;
	protected List<BasicClique> 		cliques;
	
	public ProgramRules() {
		this.basePredicates = new ArrayList<>();
		this.derivedPredicates = new ArrayList<>();
		this.cliques = new ArrayList<>();
	}

	public List<DerivedPredicate> getDerivedPredicates() { return this.derivedPredicates; }
	
	public void addDerivedPredicate(DerivedPredicate derivedPredicate) {
		if (!this.derivedPredicates.contains(derivedPredicate))
			this.derivedPredicates.add(derivedPredicate); 
	}
	
	public List<BasePredicate> getBasePredicates() { return this.basePredicates; }
	
	public void addBasePredicate(BasePredicate basePredicate) {
		if (!this.basePredicates.contains(basePredicate))
			this.basePredicates.add(basePredicate); 
	}

	public List<BasicClique> getCliques() { return this.cliques; }
	
	public void addClique(BasicClique clique) {
		if (!this.cliques.contains(clique))
			this.cliques.add(clique); 
	}
	
	public DerivedPredicate getDerivedPredicate(String predicateName, int arity) {
		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			if (derivedPredicate.getPredicateName().equals(predicateName)  
					&& arity == derivedPredicate.getArity())
				return derivedPredicate;
		}

		return null;
	}

	public BasicClique getBasicClique(String predicateName, int arity) {
		for (BasicClique clique : this.cliques) {
			if (clique.getDerivedPredicate(predicateName, arity) != null)
				return clique;
		}

		return null;
	}
	
	public List<Rule> getRules(String predicateName, int arity) {
		List<Rule> list = new ArrayList<>();
		
		DerivedPredicate dp = this.getDerivedPredicate(predicateName, arity);
		if (dp != null) {
			list.addAll(dp.getRules());
		} else {
			BasicClique clique = this.getBasicClique(predicateName, arity);
			dp = clique.getDerivedPredicate(predicateName, arity);
			if (dp != null)
				list.addAll(dp.getRules());
		}

		return list;
	}
	
	public List<Rule> getRules() {
		// we use a set but use contains() to ensure no duplicates are captured
		List<Rule> rules = new ArrayList<>();
		
		for (DerivedPredicate dp : this.derivedPredicates)
			for (Rule rule : dp.getRules())
				if (!rules.contains(rule))
					rules.add(rule);
				
		for (BasicClique clique : this.cliques) {
			for (DerivedPredicate dp : clique.getDerivedPredicates())	
				for (Rule rule : dp.getRules())
					if (!rules.contains(rule))
						rules.add(rule);
		}
		
		return rules;
	}
	
	public List<Rule> getOriginalRules() {
		// we use a set but use contains() to ensure no duplicates are captured
		List<Rule> rules = new ArrayList<>();
		
		for (DerivedPredicate dp : this.derivedPredicates) {
			for (Rule rule : dp.getRules()) {
				Rule r = null;
				if (rule.isResultOfRewrite) {
					if (rule.getOriginalRule() != null)
						r = rule.getOriginalRule();
				} else {
					r = rule;
				}
				
				if (r != null && !rules.contains(r))
					rules.add(r);
			}
		}
		
		for (BasicClique clique : this.cliques) {
			for (DerivedPredicate dp : clique.getDerivedPredicates()) {	
				for (Rule rule : dp.getRules()) {
					Rule r = null;
					if (rule.isResultOfRewrite) {
						if (rule.getOriginalRule() != null)
							r = rule.getOriginalRule();
					} else {
						r = rule;
					}
					
					if (r != null && !rules.contains(r))
						rules.add(r);
				}
			}
		}
		
		return rules;
	}

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.toStringBasePredicates());
		output.append("\n");
		output.append(this.toStringRules());
		return output.toString();
	}
	
	public String toStringBasePredicates() {
		StringBuilder output = new StringBuilder();
		if (this.basePredicates.size() == 0) {
			output.append("  No Base Predicates.");
		} else {
			if (this.basePredicates.size() > 0) {
				output.append("  Base Predicates\n");
				
				for (BasePredicate basePredicate : this.basePredicates)  {					
					output.append("    " + basePredicate.toString());
					output.append("\n");

					for (int j = 0; j < basePredicate.getNumberOfKeys(); j++) {						
						output.append("      " + basePredicate.getBasePredicateKey(j).toString());
						output.append("\n");
					}

					for (int j = 0; j < basePredicate.getNumberOfIndices(); j++) {
						output.append("      " + basePredicate.getBasePredicateIndex(j).toString());
						output.append("\n");
					}
				}
			}
	    }
		return output.toString();
	}

	public String toStringRules() {
		StringBuilder output = new StringBuilder();
		if (this.derivedPredicates.size() > 0 || this.cliques.size() > 0) {
			output.append("  Rules\n");

			for (DerivedPredicate derivedPredicate : this.derivedPredicates)
				output.append("   " + derivedPredicate.toString() + "\n");
	
			for (BasicClique clique : this.cliques) {
				for (DerivedPredicate derivedPredicate : clique.getDerivedPredicates())
					output.append("   " + derivedPredicate.toString() + "\n");
			}
			
		} else {
			output.append("  Module contains no rules.");
		}		

		return output.toString();
	}
}
