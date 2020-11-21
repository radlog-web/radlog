package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class PredicateBase extends CompilerTypeBase {
	protected String predicateName;
	protected int arity;
		
	public PredicateBase(String predicateName, int arity) {
		this(predicateName, arity, CompilerType.RELATION);
	}

	public PredicateBase(String predicateName, int arity, CompilerType compilerType) {
		super(compilerType);
		this.predicateName = predicateName;
		this.arity = arity;
	}

	public String getPredicateName() { return this.predicateName; }

	public void setPredicateName(String predicateName) {this.predicateName = predicateName;}

	public int getArity() { return this.arity; }

	public String toString() {
		StringBuilder retval = new StringBuilder();	
		retval.append(this.predicateName.toString());
		retval.append("|");
		retval.append(this.arity);	
		return retval.toString();
	}

	public PredicateBase copy() {
		return new PredicateBase(this.predicateName, this.arity);
	}

	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof PredicateBase))
			return false;

		PredicateBase otherRelation = (PredicateBase)other;

		return (this.predicateName.equals(otherRelation.getPredicateName()) 
				&& this.arity == otherRelation.getArity());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}	
}
