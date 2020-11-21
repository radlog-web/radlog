package edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph;

import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class PCGAndNode extends PCGNode<PCGOrNode> implements PCGOrNodeChild {	
	private static final long serialVersionUID = 1L;
	protected String ruleId;	// we use strings, but it might look like a float - rewritten rules are appended with a ".1", ".2", etc.
	protected Rule rule;  // original deal rule for this node
	
	public PCGAndNode(String predicateName, CompilerTypeList arguments) {
		super(predicateName, arguments, CompilerType.PCG_AND_NODE);
		this.ruleId = "0";
	}

	public void setRuleId(String ruleId) { this.ruleId = ruleId; }

	public String getRuleId() { return this.ruleId; }

	public void setRule(Rule rule) { 
		this.rule = rule; 
	}
	
	public Rule getRule() { return this.rule; }
	
	public boolean isRewritten() { return this.ruleId.contains(".");}
	
	public PCGAndNode copyNode(CompilerVariableList variableList) {
		PCGAndNode andNode = new PCGAndNode(this.getPredicateName(), this.getArguments().copy(variableList));

	  	andNode.setPredicateOperatorType(this.getPredicateOperatorType());
	  	andNode.setPredicateType(this.getPredicateType());
	  	andNode.setBuiltInPredicateType(this.getBuiltInPredicateType());
	  	
		andNode.setBindingPattern(this.binding);
		andNode.setExecutionBindingPattern(this.executionBinding);
	  	andNode.setRuleId(this.ruleId);
	  	if (this.rule != null)
	  		andNode.setRule(this.rule);
	  	
	  	andNode.setNoBacktrack(this.noBacktrack);
	  	andNode.getPredicate().setArgumentTypeAdornment(this.getPredicate().getArgumentTypeAdornment());
	  	andNode.getPredicate().setFSAggregateType(this.getPredicate().getFSAggregateType());
	  	andNode.originalArguments = this.originalArguments;
	  	return andNode;
	}

	public boolean isRecursiveRule(CliqueBase baseClique) {
		for (PCGOrNode node : this.children) {
			if (node.isRecursiveLiteral(baseClique))
				return true;
		}
		
		return false;
	}
	
	/* deprecated - use !isNonLinearRecursive(CliqueBase baseClique)
	public boolean isLinearRecursive(CliqueBase baseClique) {
		int recursiveLiteralCount = 0;
		
		for (PCGOrNode node : this.children) {
			if (node.isRecursiveLiteral(baseClique)) {
				recursiveLiteralCount++;
				
				if (recursiveLiteralCount >= 2)
					return false;					
			}
		}
		
		return true;
	}*/
	
	public boolean isNonLinearRecursive(CliqueBase baseClique) {
		int	recursiveLiteralCount = 0;

		for (PCGOrNode node : this.children) {			
			if (node.isRecursiveLiteral(baseClique)) {
				recursiveLiteralCount++;

				if (recursiveLiteralCount >= 2)
					return true;
			}
		}
		
		return false;
	}

	public void delete() {
		for (int i = this.getNumberOfChildren() - 1; i >= 0; i--)
			this.removeChild(i).delete();
	}
	
	public String toStringTree() {
		return toStringTree(0);
	}

	public String toStringTree(int level) {
		StringBuilder output = new StringBuilder();
		output.append(this.toStringIndent(level));

		output.append(this.toString());

		for (PCGOrNode node : this.children)
			output.append(node.toStringTree(level + 1));
		return output.toString();
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("PCGAndNode(");
		output.append(super.toStringAsPredicate());
		output.append(", rule#: ");
		output.append(this.ruleId);
		output.append(", #Children: ");
		output.append(this.children.size());
		output.append(", predicateType: ");
		output.append(this.getPredicateType().name());
		output.append(")");
	  		  	
	  	return output.toString();
	}

	public void resetIsDescribed() {
		for(PCGOrNode node : this.children)
			node.resetIsDescribed();
	}

	public String toStringAsRule() {		
		StringBuilder output = new StringBuilder();
		output.append("\n    ");
		output.append(this.ruleId);
		output.append(") ");
		output.append(this.toStringAsPredicate()); // head

		if (this.children.size() > 0) {
			output.append(" <-\n           ");
			String literal;
			for (int i = 0; i < this.children.size(); i++) {
				if (i > 0) output.append(",\n           ");
				
				literal = this.children.get(i).toStringAsPredicate();
				output.append(literal);
			}
	    }

		output.append(".");
		return output.toString();
	}

	public String toStringAsPCGRule() {
		StringBuilder output = new StringBuilder();
		output.append("\n    ");
		output.append(this.ruleId);
		output.append(") ");
		output.append(this.toString());

		if (this.children.size() > 0) {
			output.append(" <-\n           ");

			for (int i = 0; i < this.children.size(); i++) {
				if (i > 0)
					output.append(",\n           ");
				output.append(this.children.get(i).toString());
			}
		}

		output.append(".");
		return output.toString();
	}
	
	public PCGAndNode copy(CompilerVariableList variableList) {
		PCGAndNode andNode = this.copyNode(variableList);

		for (PCGOrNode node : this.children)
			andNode.addChild(node.copy(variableList));

		return andNode;
	}

	public PCGAndNode copy() {
		return this.copy(new CompilerVariableList());
	}

	public PCGAndNode copyTree(CompilerVariableList variableList) {
		PCGAndNode andNode = this.copyNode(variableList);

		for (PCGOrNode node : this.children)
			andNode.addChild(node.copyTree(variableList));

		return andNode;
	}

	public PCGAndNode copyTree() {
		return this.copyTree(new CompilerVariableList());
	}

	public void clearBinding() {
		this.binding.setAsNoBinding();

		for (PCGOrNode node : this.children)
			node.clearBinding();
	}

	public boolean hasChoiceLiteral() {
		for(PCGOrNode node : this.children)
			if (node.hasChoiceLiteral())
				return true;
		
		return false;
	}

	public void unsetChildrenVariables() {
		this.unsetChildrenVariablesAux();
	}

	@Override	
	public boolean equals(CompilerTypeBase other) {
		if (!(other instanceof PCGAndNode))
			return false;
		
		PCGAndNode otherNode = (PCGAndNode)other;
		return this.predicate.equals(otherNode.predicate);
	}	
	
	public boolean containsXYClique() {
		for (PCGOrNode orNode : this.children) {			
			if (orNode.containsXYClique())
				return true;
		}
		return false;
	}
}
