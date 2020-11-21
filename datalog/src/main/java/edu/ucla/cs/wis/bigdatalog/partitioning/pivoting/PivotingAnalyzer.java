package edu.ucla.cs.wis.bigdatalog.partitioning.pivoting;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.CliqueOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.Operator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.OperatorType;

public class PivotingAnalyzer {

	protected Operator root;
	protected List<CliqueOperator> cliqueOperators;
	protected List<Pair<CliqueOperator, List<CompilerVariable>>> decomposableCliqueOperators;
	protected List<CliqueOperator> nonDecomposableCliqueOperators;
	
	public PivotingAnalyzer(Operator root) {
		this.root = root;
		this.cliqueOperators = new LinkedList<>();
		this.getOperatorCliques(this.root, new Stack<CliqueOperator>());
	}
		
	public void analyzeForPivoting() {
		List<CliqueOperator> allCliqueOperators = new LinkedList<>(this.cliqueOperators);
		
		this.decomposableCliqueOperators = new LinkedList<>();

		// determine if clique is decomposable
		for (Pair<CliqueOperator, List<CompilerVariable>> dcoPair : determineDecomposableCliques(allCliqueOperators)) {
			System.out.println(dcoPair.getFirst().getName() + " is decomposable on " + dcoPair.getSecond());
			this.decomposableCliqueOperators.add(dcoPair);
			allCliqueOperators.remove(dcoPair.getFirst());
		}
		
		this.nonDecomposableCliqueOperators = new LinkedList<>();
		for (CliqueOperator co : allCliqueOperators)
			this.nonDecomposableCliqueOperators.add(co);
		
		// TODO - non decomposable programs can still be modified to limit communication
		// now see if the remaining clique operators can have partitioning applied
		// if we cannot determine what to do, then we'll use the execution engine's default partitioning (i.e. spark's)
		
	}
		
	private List<Pair<CliqueOperator, List<CompilerVariable>>> determineDecomposableCliques(List<CliqueOperator> cliqueOperators) {
		List<Pair<CliqueOperator, List<CompilerVariable>>> decomposableCliques = new LinkedList<>(); 
		for (CliqueOperator co : cliqueOperators) {
			List<CompilerVariable> restrictedVariables = analyzeForDecomposability(co);
			if (restrictedVariables != null)
				decomposableCliques.add(new Pair<>(co, restrictedVariables));
		}
		return decomposableCliques;
	}
	
	// return null if clique is not decomposable
	// otherwise, return a list of the variables that need to be used in exit rule restricting predicate
	// i.e. {X} returned means we (eventually) add X mod N = 1 to the exit rule body, where N is the # of partitions
	private List<CompilerVariable> analyzeForDecomposability(CliqueOperator cliqueOperator) {
		List<Pair<Operator, int[]>> exitRuleAnalysis;
		List<Triple<Operator, int[], Boolean>> recursiveRuleAnalysis;	
		if (cliqueOperator.getArity() == 1) {
			System.out.println(cliqueOperator.toStringTree() + "\n Unary Recursive Predicate Is Not Decomposable");
			return null;
		}
								
		exitRuleAnalysis = analyzeExitRules(cliqueOperator.getExitRulesOperator());
		
		if (exitRuleAnalysis.size() > 0)
			System.out.println("\nExit Rules:");
		
		for (Pair<Operator, int[]> pair : exitRuleAnalysis) {
			System.out.println(pair.getFirst().getPCGAndNode().getRule());
			System.out.println("Operator tree:\n" + pair.getFirst().toStringTree().substring(1));
			System.out.println("Analysis: " + interpretAnalysis(pair.getSecond(), false));
		}

		recursiveRuleAnalysis = analyzeRecursiveRules(cliqueOperator.getRecursiveRulesOperator(), cliqueOperator);
		
		System.out.println("\nRecursive Rules:");
		
		for (Triple<Operator, int[], Boolean> triple : recursiveRuleAnalysis) {
			System.out.println(triple.getFirst().getPCGAndNode().getRule() + "[" + (triple.getThird() ? "LINEAR" : "NONLINEAR") + "]");
			System.out.println("Operator tree:\n" + triple.getFirst().toStringTree().substring(1));
			System.out.println("Analysis: " + interpretAnalysis(triple.getSecond(), true));
		}
		
		boolean isSirup = ((exitRuleAnalysis.size() == 1) && (recursiveRuleAnalysis.size() == 1));
		int[] analysis = recursiveRuleAnalysis.get(0).getSecond();
		boolean isLinear = recursiveRuleAnalysis.get(0).getThird();
		List<CompilerVariable> restrictingVariables = null;
		// we focus on linear sirups for now
		if (isLinear && isSirup && (analysis[analysis.length - 1] != 0)) {
			restrictingVariables = new LinkedList<>();
			Predicate exitRuleHead = exitRuleAnalysis.get(0).getFirst().getPCGAndNode().getRule().getHead();
			// first, second or reverse is indicated by last position in array - if 0, neither
			switch (analysis[analysis.length - 1]) {
				case 1:
					restrictingVariables.add((CompilerVariable)exitRuleHead.getArgument(0));
					break;
				case 2:
					restrictingVariables.add((CompilerVariable)exitRuleHead.getArgument(1));
					break;
				case 3:
					restrictingVariables.add((CompilerVariable)exitRuleHead.getArgument(0));
					restrictingVariables.add((CompilerVariable)exitRuleHead.getArgument(1));
					break;
			}
		}	
		return restrictingVariables;
	}
	
	private static String interpretAnalysis(int[] analysis, boolean isRecursive) {
		// 1st position - no constants
		// 2nd position - no evaluable expressions
		// 3rd position (optional for binary predicate w/ linear recursion):
		//   1 = first, 2 = second, 3 = reverse
		StringBuilder output = new StringBuilder();
		if ((analysis[0] == 0) && (analysis[1] == 0) && (analysis[2] == 0))
			output.append("pure rule");
		
		if (analysis[0] == 1)
			output.append("has constants");
		
		if (analysis[1] == 1) {
			if (output.length() > 0) output.append(" && ");
			output.append("has evaluable expressions");
		}
		
		if (analysis[2] == 1) {
			if (output.length() > 0) output.append(" && ");
			output.append("has repeating variables");
		}
		
		if (isRecursive) {
			switch (analysis[analysis.length - 1]) {
				case 1:
					output.append(" && first variable");
					break;
				case 2:
					output.append(" && second variable");
					break;
				case 3:
					output.append(" && reversed variables");
					break;
			}
			
		}
		
		return output.toString();
	}
	
	private List<Triple<Operator, int[], Boolean>> analyzeRecursiveRules(Operator operator, CliqueOperator cliqueOperator) {
		if (operator.getPCGAndNode() != null && operator.getPCGAndNode().getRule() != null) {
			LinkedList<Triple<Operator, int[], Boolean>> results = new LinkedList<>();
			results.add(analyzeRecursiveRule(operator, cliqueOperator));
			return results;			
		}
		
		List<Triple<Operator, int[], Boolean>> recursiveRuleAnalysis = new LinkedList<>();
		if (operator.getNumberOfChildren() > 0)
			for (Operator op : operator.getChildren())
				recursiveRuleAnalysis.addAll(analyzeRecursiveRules(op, cliqueOperator));
		
		return recursiveRuleAnalysis;
	}
	
	private static Triple<Operator, int[], Boolean> analyzeRecursiveRule(Operator operator, CliqueOperator cliqueOperator) {
		int[] pureAnalysis = checkIfPureRule(operator.getPCGAndNode().getRule());
			
		int[] analysis = new int[pureAnalysis.length + 1];
		System.arraycopy(pureAnalysis, 0, analysis, 0, pureAnalysis.length);
		
		boolean isNonLinear = operator.getPCGAndNode().isNonLinearRecursive(cliqueOperator.getClique());
		if (!isNonLinear) {
			Rule rule = operator.getPCGAndNode().getRule();
			Predicate head = rule.getHead();
			if (head.getArity() == 2) {
				Predicate recursiveLiteral = getRecursiveLiteral(rule);
				if (head.getArgument(0) == recursiveLiteral.getArgument(0))
					analysis[analysis.length - 1] = 1;
				else if (head.getArgument(1) == recursiveLiteral.getArgument(1))
					analysis[analysis.length - 1] = 2;
				else if ((head.getArgument(0) == recursiveLiteral.getArgument(1))
						&& (head.getArgument(1) == recursiveLiteral.getArgument(0)))
					analysis[analysis.length - 1] = 3;
			}
		}

		return new Triple<>(operator, analysis, !isNonLinear);
	}
	
	private static Predicate getRecursiveLiteral(Rule rule) {
		Predicate head = rule.getHead();
		for (Predicate literal : rule.getBody())
			if (literal.getPredicateName().equals(head.getPredicateName()))
				return literal;
		
		return null;
	}
	
	private List<Pair<Operator, int[]>> analyzeExitRules(Operator operator) {
		if (operator.getPCGAndNode() != null && operator.getPCGAndNode().getRule() != null) {
			LinkedList<Pair<Operator, int[]>> results = new LinkedList<>();
			results.add(new Pair<>(operator, 
					analyzeExitRule(operator.getPCGAndNode().getRule())));
			return results;
		}
		
		List<Pair<Operator, int[]>> exitRulesAnalysis = new LinkedList<>();
		if (operator.getNumberOfChildren() > 0)
			for (Operator op : operator.getChildren())
				exitRulesAnalysis.addAll(analyzeExitRules(op));
		
		return exitRulesAnalysis;
	}
			
	private static int[] analyzeExitRule(Rule rule) {
		// check exit rule for purity.  No: 
		//   1) constants
		//   2) evaluable expressions
		//   3) repeating variables
		return checkIfPureRule(rule);
	}
	
	private static int[] checkIfPureRule(Rule rule) {
		int[] analysis = new int[3]; 
		analysis[0] = hasConstants(rule) ? 1 : 0;
		analysis[1] = hasEvaluableExpression(rule) ? 1 : 0;	
		analysis[2] = isRepeating(rule) ? 1 : 0;		
		return analysis;
	}
	
	private static boolean isRepeating(Rule rule) {
		if (hasRepeatingVariables(rule.getHead().getArguments()))
			return true;
		
		for (Predicate literal : rule.getBody())
			if (hasRepeatingVariables(literal.getArguments()))
				return true;
		
		return false;
	}
	
	private static boolean hasRepeatingVariables(CompilerTypeList arguments) {
		Set<CompilerTypeBase> set = new HashSet<>();
		
		CompilerTypeBase obj;
		Iterator<CompilerTypeBase> iterator = arguments.iterator();
		while (iterator.hasNext()) {
			obj = iterator.next();
			if (!(obj instanceof CompilerVariable))
				continue;
			
			if (set.contains(obj))
				return true;
			set.add(obj);
		}
		return false;
	}
	
	private static boolean hasEvaluableExpression(Rule rule) {
		for (Predicate literal : rule.getBody()) {
			if (literal.getPredicateType() == PredicateType.BUILT_IN)
				if (((BuiltInPredicate)literal).getBuiltInPredicateType() == BuiltInPredicateType.BINARY)
					return true;
		}
		return false;
	}
	
	private static boolean hasConstants(Rule rule) {
		CompilerTypeList constants = new CompilerTypeList();
		Utilities.getConstants(rule.getHead(), constants);
		
		if (!constants.isEmpty())
			return true;

		Utilities.getConstants(rule.getBody(), constants);
		return !constants.isEmpty();
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
		if (operator.getPCGAndNode() != null && operator.getPCGAndNode().getRule() != null) {
			System.out.println(operator.getPCGAndNode().getRule());
		} else {
			if (operator.getOperatorType() == OperatorType.UNION) {
				for (Operator child : operator.getChildren())
					printRules(child);		
			}
		}
	}
	
	
}
