package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;

public class PrimedPredicateNode extends AnalysisNode<PredicateBase> {
	
	public PrimedPredicateNode(String predicateName, int arity) {
		super(CompilerType.PRIMED_PREDICATE_NODE);
		this.predicate = new PredicateBase(predicateName, arity);
	}
 
	public String getPredicateName() { return this.predicate.getPredicateName(); }

	public int getArity() { return this.predicate.getArity(); }
}
