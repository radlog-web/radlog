package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;

public class DerivedPredicateNode extends AnalysisNode<DerivedPredicate> {
	
	public DerivedPredicateNode() {
		super(CompilerType.DERIVED_RELATION_NODE);
		this.predicate = null;
	}

	public void clearDerivedPredicate() {
		this.predicate = null;
	}

	public void setDerivedPredicate(DerivedPredicate derivedPredicate) {
		this.predicate = derivedPredicate;
	}

	public DerivedPredicate getDerivedPredicate() { return this.predicate; }
}
