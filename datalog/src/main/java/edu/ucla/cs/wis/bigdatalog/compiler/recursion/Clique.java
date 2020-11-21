package edu.ucla.cs.wis.bigdatalog.compiler.recursion;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;

public class Clique extends CliqueBase {

	protected List<CliquePredicate> cliquePredicateList;
	
	public Clique(String cliqueId) {
		super(cliqueId, CompilerType.CLIQUE); 
		this.cliquePredicateList = new ArrayList<>();
	}

	public void addCliquePredicate(CliquePredicate cliquePredicate) {
		this.cliquePredicateList.add(cliquePredicate);
	}
	
	public int getNumberOfCliquePredicates() { return this.cliquePredicateList.size(); }

	public List<CliquePredicate> getCliquePredicates() { return this.cliquePredicateList; }
	
	public CliquePredicate getCliquePredicate(int index) { return this.cliquePredicateList.get(index); }

	public CliquePredicate removeCliquePredicate(int index) { return this.cliquePredicateList.remove(index); }

	public CliquePredicate getCliquePredicate(String predicateName, int arity) {
		for (CliquePredicate cliquePredicate : this.cliquePredicateList) {
			if (cliquePredicate.getPredicateName().equals(predicateName) 
					&& (arity == cliquePredicate.getArity())) {
				return cliquePredicate;
			}
	    }
		return null;
	}

	public CliquePredicate getCliquePredicate(String predicateName, int arity, Binding binding) {
		for (CliquePredicate cliquePredicate : this.cliquePredicateList) {
			if (cliquePredicate.getPredicateName().equals(predicateName) 
					&& (arity == cliquePredicate.getArity()) 
					&& cliquePredicate.getBindingPattern().equals(binding)) {
				return cliquePredicate;
			}
	    }
		return null;
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();

		retval.append("Clique ");
		retval.append(this.cliqueId);

		retval.append(this.toStringParents());
		retval.append("\nClique Predicates: ");

		for (int i = 0; i < this.cliquePredicateList.size(); i++)
			retval.append("\n" + (i + 1) + ") " + cliquePredicateList.get(i).toString());
		
		retval.append("\nEnd Clique ");
		retval.append(this.cliqueId);
		
		return retval.toString();
	}

	public String toStringTree(int level) {
		StringBuilder retval = new StringBuilder();
		retval.append(this.toStringIndent(level));
		retval.append("Clique ");
		retval.append(this.cliqueId);
		
		if (this.isDescribed) {
			retval.append("(ALREADY DESCRIBED)");
		} else {
			retval.append(this.toStringTreeParents(level));
			
			retval.append(this.toStringIndent(level + 1));
			retval.append("Clique Predicates: ");
			
			for (int i = 0; i < this.cliquePredicateList.size(); i++)
				retval.append(cliquePredicateList.get(i).toStringTree(this, level + 1, new String((i + 1) + ") ")));
			
			this.isDescribed = true;
	    }
	  
		retval.append(this.toStringIndent(level));
		retval.append("End Clique ");
		retval.append(this.cliqueId);
		return retval.toString();
	}

	public void resetIsDescribed() {
		if (this.isDescribed) {
			this.isDescribed = false;
	      
			for (CliquePredicate cliquePredicate : this.cliquePredicateList)
				cliquePredicate.resetIsDescribed();
	    }
	}

	public void determineRecursiveType() {
		this.recursiveType = RecursiveType.LINEAR;

		for (CliquePredicate cliquePredicate : this.cliquePredicateList) {
			for (PCGAndNode recursiveRule : cliquePredicate.getRecursiveRules()) {
				if (recursiveRule.isNonLinearRecursive(this)) {
					this.recursiveType = RecursiveType.NON_LINEAR;
					return;
				}
			}
		}
	}
	 
	public Clique copy() {
		// After copying, this clique will be identical to the original clique except that
		//  we do not point to the non-local parents
		Clique newClique = new Clique(this.cliqueId);
		CliquePredicate cp;

		for (CliquePredicate cliquePredicate : this.cliquePredicateList) {
			cp = new CliquePredicate(cliquePredicate.getPredicateName(), cliquePredicate.getArity());
			cp.setBindingPattern(cliquePredicate.getBindingPattern());
			cp.isRewritten = cliquePredicate.isRewritten;

			for (PCGAndNode andNode : cliquePredicate.exitRules)
				cp.addExitRule(andNode.copyTree());
			
			for (PCGAndNode andNode : cliquePredicate.recursiveRules)
				cp.addRecursiveRule(this.copyRecursiveRule(andNode, newClique));
	      
			newClique.addCliquePredicate(cp);
	    }

		newClique.setRewritingMethod(this.rewritingMethod);
		newClique.setRecursiveType(this.recursiveType);

		return newClique;
	}

	public void clearBinding() {
		for (CliquePredicate cliquePredicate : this.cliquePredicateList)
			cliquePredicate.clearBinding();
	}
}
