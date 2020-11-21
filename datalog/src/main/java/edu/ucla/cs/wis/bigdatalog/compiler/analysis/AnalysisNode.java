package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public abstract class AnalysisNode<T extends PredicateBase> extends CompilerTypeBase {
	protected boolean 			isVisited;
	protected int 					lowlink;
	protected int 					depth;
	protected String 				cliqueId;
	protected T					predicate;
	protected List<AnalysisNode<T>>	dependentNodes;
	
	public AnalysisNode(CompilerType compilerType) {
		super(compilerType);
		this.isVisited 		= false;
		this.lowlink	 	= -1;
		this.depth 			= -1;
		this.cliqueId 		= null;
		this.dependentNodes = new ArrayList<>();
	}

	public String getPredicateName() { return this.predicate.getPredicateName(); }

	public int getArity() { return this.predicate.getArity(); }
	
	public int getNumberOfDependentNodes() { return this.dependentNodes.size(); }

	public AnalysisNode<T> getDependentNode(int position) { return this.dependentNodes.get(position); }
	
	public List<AnalysisNode<T>> getDependentNodes() { return this.dependentNodes; }

	public boolean visited() { return this.isVisited; }

	public void setVisited() { this.isVisited = true; }
	
	public void setUnVisited() { this.isVisited = false; }

	public void setLowLink(int lowLink) { this.lowlink = lowLink; }

	public void setDepth(int depth) { this.depth = depth; }

	public void setCliqueId(String cliqueId) { this.cliqueId = cliqueId; }

	public int getLowLink() { return this.lowlink; }

	public int getDepth() { return this.depth; }

	public String getCliqueId() { return this.cliqueId; }

	public void addDependentNode(AnalysisNode<T> baseNode) {
		for (AnalysisNode<T> node : this.dependentNodes) {
			if (node == baseNode)
				return;
		}

		// didn't find it, so add it
		this.dependentNodes.add(baseNode);
	}

	@Override
	public CompilerTypeBase copy() {
		throw new RuntimeException("BaseNode type can not be copied.");
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		throw new RuntimeException("BaseNode type can not be copied with variable list.");		
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		throw new RuntimeException("BaseNode.equals called.  Investigate.");
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("(");
		if (this.predicate != null) {
			retval.append(this.getPredicateName());
			retval.append("|");
			retval.append(this.getArity());
		} else {
			retval.append("unknown");
		}		
		retval.append(", {");

		if (this.dependentNodes != null) {
			int count = 0;
			for (AnalysisNode<T> node : this.dependentNodes) {
				if (count > 0) 
					retval.append(", ");
				if (node.predicate != null) {
					retval.append(node.getPredicateName());
					retval.append("|");
					retval.append(node.getArity());
				} else {
					retval.append("unknown");
				}
				count++;
			}
		}

		retval.append("})\n");		
		if (this.predicate != null)
			retval.append(this.predicate.toString());
		else
			retval.append("NO RELATION!");
		return retval.toString();
	}
}
