package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;

public class DerivedPredicate extends PredicateBase implements Serializable {
	private static final long serialVersionUID = 1L;
	protected List<Rule> rules;
	protected boolean materialize;
	
	public DerivedPredicate(String predicateName, int arity, CompilerType compilerType) {
		super(predicateName, arity, compilerType);
		this.rules = new ArrayList<>();
	}
	
	public DerivedPredicate(String predicateName, int arity) {
		this(predicateName, arity, CompilerType.DERIVED_PREDICATE);
	}
	
	public List<Rule> getRules() { return this.rules; }

	public boolean addRule(Rule rule) {
		return this.rules.add(rule);
	}

	public int getNumberOfRules() { return this.rules.size(); }

	public Rule getRule(int position) { return this.rules.get(position); }

	public void setAsMaterialized() { this.materialize = true; }
	
	public boolean doMaterialize() { return this.materialize; }
	
	public Rule removeRule(int position) { return this.rules.remove(position); }

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append(this.predicateName.toString());
		retval.append("|");
		retval.append(this.arity);
		retval.append("");

		for (Rule rule : this.rules)	
			retval.append(rule.toStringIndent(4));
				
		return retval.toString();
	}
	
	public String toJson() {
		StringBuilder retval = new StringBuilder();
		retval.append("{\"name\":\"");
		retval.append(this.predicateName);
		retval.append("\", \"arity\":");
		retval.append(this.arity);
		retval.append(",\"rules\":[");
		for (int i = 0; i < this.rules.size(); i++) {
			if (i > 0)
				retval.append(",");
			retval.append(this.rules.get(i).toJson());
		}
		retval.append("]}");
		return retval.toString();
	}
	
	public List<Integer> getPositionsOfFSAggregatesInHead() {		
		// we should only have to check one of the rules, since fs aggregates should be paired in rules
		Rule dr = this.rules.get(0);
		
		if (!dr.getHead().containsFSAggregate())
			return new ArrayList<>();
		
		List<Integer> positions = new ArrayList<>();
		for (int i = 0; i < dr.getHead().getArity(); i++) {
			CompilerTypeBase arg = dr.getHead().getArgument(i);
			if (arg.containsFSAggregate())
				positions.add(i);
		}
		
		return positions;
	}
		
	public DerivedPredicate copy() {
		DerivedPredicate derivedPredicate = new DerivedPredicate(this.predicateName, this.arity);
		for (Rule rule : this.rules)
			derivedPredicate.addRule(rule.copy());
		
		return derivedPredicate;
	}
}
