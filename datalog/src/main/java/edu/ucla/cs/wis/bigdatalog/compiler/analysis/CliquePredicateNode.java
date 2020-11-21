package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;

/// used in regenerating cliques
public class CliquePredicateNode extends AnalysisNode<CliquePredicate> {
	
	// in this derived class, the relation will be a clique predicate 
	public CliquePredicateNode() {
		super(CompilerType.CLIQUE_PREDICATE_NODE);
	}

	public Binding getBindingPattern() { return this.predicate.getBindingPattern(); }

	public void setBindingPattern(Binding binding) {
		this.predicate.setBindingPattern(binding);
	}

	public void resetCliquePredicate() {
		this.predicate = null;
	}

	public void setCliquePredicate(CliquePredicate cliquePredicate) {
		this.predicate = cliquePredicate;
	}

	public CliquePredicate getCliquePredicate() { return this.predicate; }
}
