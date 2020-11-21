package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import java.io.Serializable;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class BasePredicateKey extends CompilerTypeBase implements Serializable {
	private static final long serialVersionUID = 1L;
	protected String basePredicateName;
	protected int[]	ordinalTerms;
	
	public BasePredicateKey(String basePredicateName, List<Integer> ordinalTerms) {
		super(CompilerType.SCHEMA_KEY);
	  
		this.basePredicateName = basePredicateName;
		this.ordinalTerms = new int[ordinalTerms.size()];

		for (int i = 0; i < ordinalTerms.size(); i++)
			this.ordinalTerms[i] = ordinalTerms.get(i);
	}

	public String getBasePredicateName() { return this.basePredicateName; }

	public int[] getOrdinalTerms() { return this.ordinalTerms; }

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("key(" + this.basePredicateName + ", [");
			 
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
		throw new RuntimeException("Base Predicate Key type can not be copied.");
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		throw new RuntimeException("Base Predicate Key type can not be copied with variable list.");
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof BasePredicateKey))
			return false;
		
		BasePredicateKey otherBasePredicateKey = (BasePredicateKey)other;
		
		if (this.basePredicateName == null && otherBasePredicateKey.getBasePredicateName() != null)
			return false;
		
		if (!this.basePredicateName.equals(otherBasePredicateKey.getBasePredicateName()))
			return false;
		
		if (this.ordinalTerms == null && otherBasePredicateKey.getOrdinalTerms() != null)
			return false;
		
		if (this.ordinalTerms.length != otherBasePredicateKey.getOrdinalTerms().length)
			return false;
		
		for (int i = 0; i < this.ordinalTerms.length; i++)
			if (this.ordinalTerms[i] != otherBasePredicateKey.ordinalTerms[i])
				return false;
		
		return true;
	}
}
