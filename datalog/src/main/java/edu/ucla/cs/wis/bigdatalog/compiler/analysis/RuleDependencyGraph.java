package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenElsePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;

public class RuleDependencyGraph {
	
	protected Module 						module;
	protected List<DerivedPredicate>		derivedPredicates;
	protected List<DerivedPredicateNode> 	derivedPredicateNodes;
	
	public RuleDependencyGraph(Module module) {
		this.module = module;
		this.derivedPredicates = module.getDerivedPredicates();
		this.derivedPredicateNodes = new ArrayList<>();
	}
	
	public List<BasicClique> getCliques() { return this.module.getCliques(); }
	
	public List<DerivedPredicate> getDerivedPredicates() { return this.derivedPredicates; }
	
	public List<DerivedPredicateNode> getDerivedPredicateNodes() { return this.derivedPredicateNodes; }
	
	public void generate() {
		this.derivedPredicateNodes.clear();
		
		Predicate head;
		DerivedPredicateNode derivedPredicateNode;
		DerivedPredicateNode headDerivedPredicateNode;

		// for each derivedpredicate, create one node
		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			derivedPredicateNode = new DerivedPredicateNode();
			derivedPredicateNode.setDerivedPredicate(derivedPredicate);
			this.derivedPredicateNodes.add(derivedPredicateNode);
		}

		for (DerivedPredicate derivedPredicate : this.derivedPredicates) {
			// for each derived predicate, get each rule
			for (Rule rule : derivedPredicate.getRules()) {
				head = rule.getHead();
				headDerivedPredicateNode = this.getDerivedPredicateNode(head.getPredicateName(), head.getArity());
				// for each rule, find the dependencies for each goal	  
				this.generateDependencyGraphFromLiterals(rule.getBody(), headDerivedPredicateNode);
			}
		}
	}

	private void generateDependencyGraphFromLiterals(List<Predicate> literals, DerivedPredicateNode headDerivedPredicateNode) {
		for (Predicate literal : literals) {
			if (literal instanceof IfThenElsePredicate) {
				IfThenElsePredicate itePredicate = (IfThenElsePredicate)literal;

				this.generateDependencyGraphFromLiterals(itePredicate.getIfLiterals(), headDerivedPredicateNode);
				this.generateDependencyGraphFromLiterals(itePredicate.getThenLiterals(), headDerivedPredicateNode);
				this.generateDependencyGraphFromLiterals(itePredicate.getElseLiterals(), headDerivedPredicateNode);
			} else if (literal instanceof IfThenPredicate) {
				IfThenPredicate itPredicate = (IfThenPredicate)literal;

				this.generateDependencyGraphFromLiterals(itPredicate.getIfLiterals(), headDerivedPredicateNode);
				this.generateDependencyGraphFromLiterals(itPredicate.getThenLiterals(), headDerivedPredicateNode);
			} else {
				DerivedPredicateNode derivedPredicateNode;
				// for each literal, check for existence of node, if it is there, then there is a dependency				
				if ((derivedPredicateNode = this.getDerivedPredicateNode(literal.getPredicateName(), literal.getArity())) != null)
					headDerivedPredicateNode.addDependentNode(derivedPredicateNode);
			}
		}
	}
	
	private DerivedPredicateNode getDerivedPredicateNode(String predicateName, int arity) {
		for (DerivedPredicateNode derivedPredicateNode : this.derivedPredicateNodes) {
			if ((derivedPredicateNode.getDerivedPredicate() != null)
					&& (derivedPredicateNode.getPredicateName().equals(predicateName)) 
					&& (arity == derivedPredicateNode.getArity())) {
				return derivedPredicateNode;
			}
		}

		return null;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		//retval.append("[BEGIN Dependency Graph for Module '" + this.module.getModuleName() + "' BEGIN]");

		int counter = 0;
		for (DerivedPredicateNode derivedPredicateNode : this.derivedPredicateNodes) {
			if (counter > 0) retval.append("\n");
			retval.append(derivedPredicateNode.toString() + "\n");
			counter++;
		}

		//retval.append("[END Dependency Graph for Module '" + this.module.getModuleName() + "' END]");
		return retval.toString();
	}	
}
