package edu.ucla.cs.wis.bigdatalog.compiler.xy;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.RecursiveType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;

public class XYClique extends CliqueBase {
	private static final long serialVersionUID = 1L;
	protected List<XYCliquePredicate> xyCliquePredicates;
	
	public XYClique(String cliqueId) {
		super(cliqueId, CompilerType.XY_CLIQUE);
		this.xyCliquePredicates = new ArrayList<>();
	}
	
	public void addCliquePredicate(XYCliquePredicate xyCliquePredicate) { 
		this.xyCliquePredicates.add(xyCliquePredicate);
	}

	public int getNumberOfCliquePredicates() { return this.xyCliquePredicates.size(); }
	
	public List<XYCliquePredicate> getCliquePredicates() { return this.xyCliquePredicates; }

	public XYCliquePredicate getCliquePredicate(String predicateName, int arity) {
		for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates) {
			if (xyCliquePredicate.getPredicateName().equals(predicateName)  
					&& (arity == xyCliquePredicate.getArity())) {
				return xyCliquePredicate;
			}
	    }

		return null;
	}

	public Pair<XYCliquePredicate, Integer> getCliquePredicate(String predicateName, 
			int arity, Binding binding) {
		
		int position = 0;
		for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates) {
			if (xyCliquePredicate.getPredicateName().equals(predicateName) 
					&& arity == xyCliquePredicate.getArity() 
					&& xyCliquePredicate.getBindingPattern().equals(binding)) {
				return new Pair<>(xyCliquePredicate, position);
			}
			position++;
	    }
		
		return new Pair<>(null, -1);
	}

	public void determineRecursiveType() {
		this.recursiveType = RecursiveType.LINEAR;

		for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates) {
			for (PCGAndNode copyRule : xyCliquePredicate.getCopyRules()) {
				if (copyRule.isNonLinearRecursive(this)) {
					this.recursiveType = RecursiveType.NON_LINEAR;
					return;
				}
			}

			for (PCGAndNode deleteRule : xyCliquePredicate.getDeleteRules()) {
				if (deleteRule.isNonLinearRecursive(this)) {
					this.recursiveType = RecursiveType.NON_LINEAR;
					return;
				}
			}

			for (PCGAndNode xRule : xyCliquePredicate.getXRules()) {
				if (xRule.isNonLinearRecursive(this)) {
					this.recursiveType = RecursiveType.NON_LINEAR;
					return;
				}
			}

			for (PCGAndNode yRule : xyCliquePredicate.getYRules()) {
				if (yRule.isNonLinearRecursive(this)) {
					this.recursiveType = RecursiveType.NON_LINEAR;
					return;
				}
			}
	    }
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("XYClique ");
		output.append(this.cliqueId);
		output.append(this.toStringParents());

		output.append("\nClique Predicates: ");
		
		for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates)
			output.append(xyCliquePredicate.toString());

		output.append("\nEnd of XYClique ");
		output.append(this.cliqueId);
		return output.toString();
	}

	public String toStringTree(int level) {
		StringBuilder output = new StringBuilder();
		output.append(this.toStringIndent(level));
		output.append("XYClique: ");
		output.append(this.cliqueId);

		if (this.isDescribed) {
			output.append("(ALREADY DESCRIBED)"); 
		} else {      
			this.isDescribed = true;
			output.append(this.toStringTreeParents(level));

			int counter = 0;
			for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates) {
				if (counter > 0) output.append(",");
				output.append(xyCliquePredicate.toStringTree(this, level + 1));
				counter++;
			}
	    }
	  
		output.append(this.toStringIndent(level));
		output.append("End of XYClique: ");
		output.append(this.cliqueId);
		return output.toString();				
	}
	
	public void resetIsDescribed() {
		if (this.isDescribed) {
			this.isDescribed = false;

			for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates)
				xyCliquePredicate.resetIsDescribed();
	    }
	}

	public XYClique copy() {
	   XYClique newClique = new XYClique(this.cliqueId);
	   XYCliquePredicate newXYCP;

	   for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates) {
		   newXYCP = new XYCliquePredicate(xyCliquePredicate.getPredicateName(), xyCliquePredicate.getArity());
		   newXYCP.setBindingPattern(xyCliquePredicate.getBindingPattern());

		   for (PCGAndNode andNode : xyCliquePredicate.getExitRules())
			   newXYCP.addExitRule(this.copyRecursiveRule(andNode, newClique));

		   for (PCGAndNode andNode : xyCliquePredicate.getXRules())
			   newXYCP.addXRule(this.copyRecursiveRule(andNode, newClique));
		   
		   for (PCGAndNode andNode : xyCliquePredicate.getYRules())
			   newXYCP.addYRule(this.copyRecursiveRule(andNode, newClique));
				   
		   for (PCGAndNode andNode : xyCliquePredicate.getCopyRules())
			   newXYCP.addCopyRule(this.copyRecursiveRule(andNode, newClique));

		   for (PCGAndNode andNode : xyCliquePredicate.getDeleteRules())
			   newXYCP.addDeleteRule(this.copyRecursiveRule(andNode, newClique));
		   
		   newClique.addCliquePredicate(newXYCP);
	   }

	   newClique.setRewritingMethod(this.rewritingMethod);
	   newClique.setRecursiveType(this.recursiveType);

	   return newClique;
	}

	public void clearBinding() {
		for (XYCliquePredicate xyCliquePredicate : this.xyCliquePredicates)			
			xyCliquePredicate.clearBinding();
	}
}
