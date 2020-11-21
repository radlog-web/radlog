package edu.ucla.cs.wis.bigdatalog.partitioning;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.CliqueOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.Operator;

public class PartitioningManager {

	protected Operator root;
	protected List<CliqueOperator> cliqueOperators;
	protected List<Pair<CliqueOperator, List<CompilerVariable>>> decomposableCliqueOperators;
	protected List<CliqueOperator> nonDecomposableCliqueOperators;
	
	public PartitioningManager(Operator root) { 
		this.root = root;
		
		this.cliqueOperators = new LinkedList<>();
		this.getOperatorCliques(this.root, new Stack<CliqueOperator>());
	}
	
	private void getOperatorCliques(Operator operator, Stack<CliqueOperator> stack) {
		if (operator instanceof CliqueOperator) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator;
			if (stack.contains(operator))
				return;			
			
			stack.add(cliqueOperator);
			this.cliqueOperators.add(cliqueOperator);
						
			if (cliqueOperator.getExitRulesOperator() != null) {
				printRules(cliqueOperator.getExitRulesOperator());
								
				getOperatorCliques(cliqueOperator.getExitRulesOperator(), stack);
			}
			
			printRules(cliqueOperator.getRecursiveRulesOperator());
			
			getOperatorCliques(cliqueOperator.getRecursiveRulesOperator(), stack);
			
			stack.pop();
		} else {		
			for (Operator child : operator.getChildren())
				getOperatorCliques(child, stack);
		}
	}
	
	private static void printRules(Operator operator) {
		/*if (operator.getPCGAndNode() != null && operator.getPCGAndNode().getRule() != null) {
			System.out.println(operator.getPCGAndNode().getRule());
		} else {
			if (operator.getOperatorType() == OperatorType.UNION) {
				for (Operator child : operator.getChildren())
					printRules(child);		
			}
		}*/
	}
}
