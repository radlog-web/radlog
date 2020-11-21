package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.io.Serializable;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class BasePredicateIndex extends CompilerTypeBase implements Serializable {
	private static final long serialVersionUID = 1L;
	protected String basePredicateName;
	protected int[]	ordinalTerms;
		
	public BasePredicateIndex(String basePredicateName, List<Integer> ordinalTerms) {
		super(CompilerType.SCHEMA_INDEX);
		this.basePredicateName = basePredicateName;
		this.ordinalTerms = new int[ordinalTerms.size()];

		for (int i = 0; i < ordinalTerms.size(); i++)
			this.ordinalTerms[i] = ordinalTerms.get(i);
	}

	public String getBasePredicateName() { return this.basePredicateName; }

	public int[] getOrdinalTerms() { return this.ordinalTerms; }

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("index(" + this.basePredicateName + ", [");
			  
		for (int i = 0; i < this.ordinalTerms.length; i++) {
			if (i > 0)
				retval.append(", ");
			retval.append(this.ordinalTerms[i]);
		}
		
		retval.append("]).");
		return retval.toString();
	}

	@Override
	public CompilerTypeBase copy() {
		throw new RuntimeException("Base Predicate Index type can not be copied.");
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		throw new RuntimeException("Base Predicate Index type can not be copied with variable list.");
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof BasePredicateIndex))
			return false;
		
		BasePredicateIndex otherBasePredicateIndex = (BasePredicateIndex)other;
		
		if (this.basePredicateName == null && otherBasePredicateIndex.getBasePredicateName() != null)
			return false;
		
		if (!this.basePredicateName.equals(otherBasePredicateIndex.getBasePredicateName()))
			return false;
		
		if (this.ordinalTerms == null && otherBasePredicateIndex.getOrdinalTerms() != null)
			return false;
		
		if (this.ordinalTerms.length != otherBasePredicateIndex.getOrdinalTerms().length)
			return false;
		
		for (int i = 0; i < this.ordinalTerms.length; i++)
			if (this.ordinalTerms[i] != otherBasePredicateIndex.ordinalTerms[i])
				return false;
		
		return true;
	}
}
