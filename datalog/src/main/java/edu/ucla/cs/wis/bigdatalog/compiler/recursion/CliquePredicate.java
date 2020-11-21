package edu.ucla.cs.wis.bigdatalog.compiler.recursion;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateBase;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class CliquePredicate extends PredicateBase {
	private static Logger logger = LoggerFactory.getLogger(CliquePredicate.class.getName());
	
	protected boolean 			isRewritten;
	protected List<PCGAndNode> 	recursiveRules;
	protected List<PCGAndNode> 	exitRules;
	protected Binding 				binding;

	public CliquePredicate(String predicateName, int arity, CompilerType compilerType) {
		super(predicateName, arity, compilerType);
		this.isRewritten = false;
		this.recursiveRules = new ArrayList<>();
		this.exitRules = new ArrayList<>();
		this.binding = new Binding(arity);
	} 

	public CliquePredicate(String predicateName, int arity) {
		this(predicateName, arity, CompilerType.CLIQUE_PREDICATE);
	}
	
	public void delete(DeALSContext deALSContext) {
		deALSContext.logTrace(logger, "Entering delete for {} {}", this.predicateName, this.binding.toString());
		
		for (int i = this.getNumberOfExitRules() - 1; i >= 0; i--)
			this.removeExitRule(i).delete();
		
		for (int i = this.getNumberOfRecursiveRules() - 1; i >= 0; i--)
			this.removeRecursiveRule(i).delete();
				
		this.exitRules = null;
		this.recursiveRules = null;
		
		deALSContext.logTrace(logger, "Exiting delete");
	}

	public int getNumberOfExitRules() { return this.exitRules.size(); }

	public List<PCGAndNode> getExitRules() { return this.exitRules; }
	
	public PCGAndNode getExitRule(int index) { return this.exitRules.get(index); }

	public void addExitRule(PCGAndNode rule) { this.exitRules.add(rule); }

	public PCGAndNode removeExitRule(int index) { return this.exitRules.remove(index); }

	public void setExitRule(int index, PCGAndNode rule) { this.exitRules.set(index, rule); }
	
	public List<PCGAndNode> getRecursiveRules() { return this.recursiveRules; }

	public int getNumberOfRecursiveRules() { return this.recursiveRules.size(); }

	public PCGAndNode getRecursiveRule(int index) { return this.recursiveRules.get(index); }

	public void addRecursiveRule(PCGAndNode rule) { this.recursiveRules.add(rule); }

	public PCGAndNode removeRecursiveRule(int index) { return this.recursiveRules.remove(index); }

	public void setRecursiveRule(int index, PCGAndNode rule) { this.recursiveRules.set(index, rule); }

	public boolean isRewritten() {
		return this.isRewritten;
		/*if (this.isRewritten)
			return true;
		
		boolean allFSRules = true;
		// if all rules are from rewritting fs aggregates, then this cliquepredicate is considered rewritten
		for (PCGAndNode exitRule : this.exitRules) {
			if (exitRule.getNumberOfChildren() == 1)
				if (exitRule.getChild(0).getPredicateName().startsWith(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX))
					continue;
			allFSRules = false;
			break;
		}
		
		if (allFSRules) {
			for (PCGAndNode recursiveRule : this.recursiveRules) {
				if (recursiveRule.getNumberOfChildren() == 1)
					if (recursiveRule.getChild(0).getPredicateName().startsWith(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX))
						continue;
				allFSRules = false;
				break;
			}
		}
				
		return allFSRules; */
	}

	public void setRewritten() { this.isRewritten = true; }

	public String toStringTree(Clique clique) {
		return this.toStringTree(clique, 0, "");
	}

	public String toStringTree(Clique clique, int level, String prefix) {
		StringBuilder output = new StringBuilder();
		CliqueBase baseClique;

		output.append(this.toStringIndent(level));
		output.append(prefix + "CliquePredicate(" + this.predicateName.toString());

		if (!this.binding.hasNoBinding()) {  // no adornment yet
			output.append("_");
			output.append(this.binding.toString());
		}

		output.append(", binding: ");
		output.append(this.binding.toString());
		output.append(", #ExitRules: ");
		output.append(this.getNumberOfExitRules());
		output.append(", #RecursiveRules: ");
		output.append(this.getNumberOfRecursiveRules());
		output.append(")");

		output.append(this.toStringIndent(level + 1));

		output.append("Exit Rules:");
		if (this.exitRules.size() == 0) {
			output.append(" NONE");
		} else {
			for (PCGAndNode andNode : this.exitRules) {
				output.append(this.toStringIndent(level + 2));
				output.append(andNode.toString());
	
				for (PCGOrNode literal : andNode.getChildren()) {
					if (literal.isRecursive()) {
						output.append(this.toStringIndent(level + 3));
						output.append(literal.toString());
	
						// sometimes, after rewriting, some exit rules may become recursive
						if (!literal.isRecursiveLiteral(clique)) {
							baseClique = literal.getBaseClique();
							output.append(baseClique.toStringTree(level + 4));
						}
					} else {
						output.append(literal.toStringTree(level + 3));
					}
				}
			}
		}

		output.append(this.toStringIndent(level + 1));

		output.append("Recursive Rules:");
		
		if (this.recursiveRules.size() == 0) {
			output.append(" NONE");
		} else {
			for (PCGAndNode andNode : this.recursiveRules) {
				output.append(this.toStringIndent(level + 2));
				output.append(andNode.toString());
	
				for (PCGOrNode literal : andNode.getChildren()) {
					if (literal.isRecursive()) {
						output.append(this.toStringIndent(level + 3));
						output.append(literal.toString());
	
						if (!literal.isRecursiveLiteral(clique)) {
							baseClique = literal.getBaseClique();
							output.append(baseClique.toStringTree(level + 4));
						}
					} else {
						output.append(literal.toStringTree(level + 3));
					}
				}
			}
		}
		return output.toString();
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("CliquePredicate(" + this.predicateName);

		if (!this.binding.hasNoBinding()) { // no adornment yet
			output.append("_");
			output.append(this.binding.toString());
		}

		output.append(", binding: ");
		output.append(this.binding.toString());
		output.append(", #ExitRules: " + this.getNumberOfExitRules());
		output.append(", #RecursiveRules: " + this.getNumberOfRecursiveRules());
		output.append(")");
		
		output.append("\n  Exit Rules:");
		
		if (this.getNumberOfExitRules() > 0) {
			for (PCGAndNode andNode : this.exitRules)
				output.append(andNode.toStringAsRule());
		} else {
			output.append(" NONE");
		}
		
		output.append("\n  Recursive Rules:");

		if (this.recursiveRules.size() == 0) {
			output.append(" NONE");
		} else {
			for (PCGAndNode andNode : this.recursiveRules)
				output.append(andNode.toStringAsRule());
		}
		
		output.append("\n");
		return output.toString();
	}

	public void resetIsDescribed() {
		for (PCGAndNode andNode : this.exitRules) 
			andNode.resetIsDescribed();
		
		for (PCGAndNode andNode : this.recursiveRules) 
			andNode.resetIsDescribed();
	}

	public CliquePredicate copy() {
		CliquePredicate	cp = new CliquePredicate(this.predicateName, this.arity);
		cp.setBindingPattern(this.binding);
		cp.isRewritten = this.isRewritten;

		for (PCGAndNode andNode : this.exitRules)
			cp.addExitRule(andNode.copyTree());

		for (PCGAndNode andNode : this.recursiveRules)
			cp.addRecursiveRule(andNode.copyTree());

		return cp;
	}

	public void clearBinding() {
		this.binding.setAsNoBinding();

		for (int i = 0; i < this.getNumberOfExitRules(); i++)
			this.getExitRule(i).clearBinding();

		for (int i = 0; i < this.getNumberOfRecursiveRules(); i++)
			this.getRecursiveRule(i).clearBinding();
	}
	
	public Binding getBindingPattern() {return (this.binding);}

	public void setBindingPattern(Binding binding) { 
		this.binding.assignBinding(binding);
	}

	public void setBindingPattern(int i, BindingType bindingType) {
		this.binding.setBinding(i, bindingType);
	}

	public void clearBindingPattern() {
		this.binding.setAsAllFreeBinding();
	}

	public BindingType getBinding(int i) {
		return (this.binding.getBinding(i));
	}	
}
