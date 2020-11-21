package edu.ucla.cs.wis.bigdatalog.compiler.recursion;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.RewritingMethodType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYClique;

abstract public class CliqueBase extends CompilerTypeBase implements PCGOrNodeChild {

	protected String 				cliqueId;
	protected List<PCGOrNode>	 	parents;
	protected boolean 			isDescribed;
	protected boolean 			isStageVariableEliminated;
	protected RecursiveType 		recursiveType;
	protected RewritingMethodType 	rewritingMethod;
	
	public CliqueBase(String cliqueId, CompilerType compilerType) {
		super(compilerType);
		this.cliqueId = cliqueId;
		this.parents = new ArrayList<>();
		this.isDescribed = false;
		this.rewritingMethod = RewritingMethodType.UNKNOWN;
		this.recursiveType = RecursiveType.UNKNOWN;
		this.isStageVariableEliminated = false;
	}

	public String getCliqueId() {return this.cliqueId;}

	public void setRecursiveType(RecursiveType recursiveType){
		this.recursiveType = recursiveType;
	}

	public boolean isLinearRecursive() { return (this.recursiveType == RecursiveType.LINEAR); }

	public boolean isNonLinearRecursive() { return (this.recursiveType == RecursiveType.NON_LINEAR); }

	public boolean isRewritten() { return (this.rewritingMethod != RewritingMethodType.UNKNOWN); }

	public void setRewritingMethod(RewritingMethodType rewritingMethod) {
		this.rewritingMethod = rewritingMethod;
	}

	public RewritingMethodType getRewritingMethod() { return this.rewritingMethod; }

	public void addParent(PCGOrNode parent) {
		if (!this.parents.contains(parent))
			this.parents.add(parent);
	}

	public int getNumberOfParents() { return this.parents.size(); }
	
	public List<PCGOrNode> getParents() { return this.parents; }

	public PCGOrNode getParent(int position) { return this.parents.get(position); }

	public void removeParent(int position) {
		this.parents.remove(position);
	}

	public void substituteParent(PCGOrNode oldParent, PCGOrNode newParent) {
	  for (int i = 0; i < this.parents.size(); i++)
		  if (this.parents.get(i) == oldParent) {
			  this.parents.set(i, newParent);
			  break;
	      }
	}

	public void removeParent(PCGOrNode oldParent) {
		for (int i = 0; i < this.parents.size(); i++) {
			if (this.parents.get(i) == oldParent) {
				this.parents.remove(i);
				break;
			}
		}
	}

	public String toStringParents() {
		StringBuilder retval = new StringBuilder();
		retval.append(" Parents: ");

		int count = 0;
		for (PCGOrNode parent : this.parents) {
			retval.append("\n  ");
			retval.append(count++ + 1);
			retval.append(") ");
			retval.append(parent.toString());
	    }
		return retval.toString();
	}

	public String toStringTreeParents(int level) {
		StringBuilder retval = new StringBuilder();
		retval.append(this.toStringIndent(level + 1));
		retval.append("Parents: ");

		int count = 0;
		for (PCGOrNode parent : this.parents) {		
			retval.append(this.toStringIndent(level + 2));
			retval.append(count++ + 1);
			retval.append(") ");
			retval.append(parent.toString());
	    }
		return retval.toString();
	}

	public String toStringTree() {
		return toStringTree(0);
	}
	
	public abstract String toStringTree(int level);
	
	public void resetIsDescribed() {
		if (this.getType() == CompilerType.CLIQUE)
			((Clique)this).resetIsDescribed();
		else if (this.getType() == CompilerType.XY_CLIQUE)
			((XYClique)this).resetIsDescribed();
	}

	public PCGAndNode copyRecursiveRule(PCGAndNode andNode, CliqueBase clique) {
		CompilerVariableList	variableList = new CompilerVariableList();
		PCGOrNode 		newOrNode;
		PCGAndNode 		newAndNode = andNode.copyNode(variableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			if (orNode.isRecursiveLiteral(this)) {
				newOrNode = orNode.copyNode(variableList);
				// make sure that self-recursive literals point to new clique
				newOrNode.addChild(clique);
				// make sure that the new clique point to this new self-recursive literal
				clique.addParent(newOrNode);
			} else {
				newOrNode = orNode.copyTree(variableList);
			}

			newAndNode.addChild(newOrNode);
	    }

		return newAndNode;
	}

	@Override
	public CompilerTypeBase copy() {
		throw new RuntimeException("BaseClique type can not be copied.");
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		throw new RuntimeException("BaseClique type can not be copied with variable list.");
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		return (other instanceof CliqueBase);
	}
	
	public boolean stageVariableEliminated() {return this.isStageVariableEliminated;}
	
	public void setStageVariableEliminatedAs(boolean status) {this.isStageVariableEliminated = status;}

	@Override
	public void unsetChildrenVariables() {
		//do nothing - for PCGNodeChild 		
	}

	@Override
	public void unsetVariables() {
		//do nothing - for PCGNodeChild 		
	}
	
	public abstract String toString();
	
	abstract public CliquePredicate getCliquePredicate(String predicateName, int arity);
	
}
