package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

// this is a clique with only derived relations
public class BasicClique extends CompilerTypeBase implements Serializable {
	private static final long serialVersionUID = 1L;
	protected String cliqueId;
	protected List<DerivedPredicate> derivedPredicates;
	
	public BasicClique(String cliqueId) {
		super(CompilerType.BASIC_CLIQUE);
		this.cliqueId = cliqueId;
		this.derivedPredicates = new ArrayList<>();	
	}

	public String getCliqueId() { return this.cliqueId; }

	public void addDerivedPredicate(DerivedPredicate derivedPredicate) {
		this.derivedPredicates.add(derivedPredicate);
	}

	public List<DerivedPredicate> getDerivedPredicates() { return this.derivedPredicates;}
	
	public int getNumberOfDerivedPredicates() { return this.derivedPredicates.size(); }

	public DerivedPredicate removeDerivedPredicate(int position) {
		return this.derivedPredicates.remove(position);
	}

	public DerivedPredicate getDerivedPredicate(int position) {
		return this.derivedPredicates.get(position);
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("BasicClique ");
		output.append(this.cliqueId);
		output.append("\n");

		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			output.append(derivedPredicate.toString());
			output.append("\n");
		}

		output.append("End BasicClique ");
		output.append(this.cliqueId);
		return output.toString();
	}

	public DerivedPredicate getDerivedPredicate(String predicateName, int arity) {
		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			if (derivedPredicate.getPredicateName().equals(predicateName) 
					&& (arity == derivedPredicate.getArity()))
				return derivedPredicate;
	    }
		return null;
	}

	@Override
	public CompilerTypeBase copy() {
		throw new RuntimeException("BasicClique type can not be copied.");
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		throw new RuntimeException("BasicClique type can not be copied with variable list.");
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof BasicClique))
			return false;
		
		return other.getType() == CompilerType.BASIC_CLIQUE;
	}
}
