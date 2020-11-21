package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;

public class CliqueOperator extends Operator implements Serializable {
	private static final long serialVersionUID = 1L;

	private Operator exitRulesOperator;
	private Operator recursiveRulesOperator;
	private List<Operator> mutualCliqueList;
	private Clique clique;
	private boolean isLinearRecursion; 
	private EvaluationType evaluationType;
	
	public CliqueOperator(String name, OperatorType operatorType, OperatorArguments arguments, 
			Clique clique, EvaluationType evaluationType) {
		super(name, operatorType, arguments);
		this.mutualCliqueList = new ArrayList<>();
		this.clique = clique;
		this.isLinearRecursion = clique.isLinearRecursive();
		this.evaluationType = evaluationType;
	}

	public void setArgument(OperatorArguments arguments) { this.arguments = arguments; }
	
	public void addExitRulesOperator(Operator operator) { this.exitRulesOperator = operator; }
	
	public void addRecursiveRulesOperator(Operator operator) { this.recursiveRulesOperator = operator; }
	
	public Operator getExitRulesOperator() { return this.exitRulesOperator; }
	
	public void setExitRulesOperator(Operator exitRulesOperator) { this.exitRulesOperator = exitRulesOperator; }
	
	public Operator getRecursiveRulesOperator() { return this.recursiveRulesOperator; }
	
	public void setRecursiveRulesOperator(Operator recursiveRulesOperator) { this.recursiveRulesOperator = recursiveRulesOperator; }

	public List<Operator> getMutualCliqueList() { return this.mutualCliqueList; }
	
	public void setMutualCliqueList(List<Operator> mutualCliqueList) { this.mutualCliqueList = mutualCliqueList; }
	
	public boolean isLinearRecursion() { return this.isLinearRecursion; }
	
	public EvaluationType getEvaluationType() { return this.evaluationType; }
	
	public Clique getClique() { return this.clique; }
	
	@Override
	public String toStringTree() {
		StringBuilder output = new StringBuilder();
		output.append(Operator.toStringIndent());
		output.append(this.toString());
		output.append("(Recursion: " + (this.isLinearRecursion() ? "LINEAR" : "NONLINEAR"));
		output.append(", Evaluation Type: " + this.evaluationType);
		output.append(")\n");
		
		for (int i = 0; i < displayIndentLevel; i++)
			output.append(" ");

		output.append("Exit Rules: ");

		if (this.exitRulesOperator != null) {
			displayIndentLevel++;
			output.append(this.exitRulesOperator.toStringTree());
			displayIndentLevel--;
		}
		
		output.append("\n");			
		for (int i = 0; i < displayIndentLevel; i++)
			output.append(" ");
		output.append("Recursive Rules: ");
		
		if (this.recursiveRulesOperator != null) {
			displayIndentLevel++;
			output.append(this.recursiveRulesOperator.toStringTree());
			displayIndentLevel--;
		}
		
		return output.toString();
	}
}
