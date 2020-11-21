package edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.fraction.Fraction;
import org.apache.commons.math3.optimization.GoalType;
import org.apache.commons.math3.optimization.PointValuePair;
import org.apache.commons.math3.optimization.linear.LinearConstraint;
import org.apache.commons.math3.optimization.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optimization.linear.NoFeasibleSolutionException;
import org.apache.commons.math3.optimization.linear.Relationship;
import org.apache.commons.math3.optimization.linear.SimplexSolver;

import edu.ucla.cs.wis.bigdatalog.compiler.ProgramRules;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.BasicClique;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;

public class GeneralizedPivotingSolver {
	
	public static GeneralizedPivotSet getGeneralizedPivotSet(ProgramRules programRules) {
		
		List<Rule> recursiveRules = getRecursiveRules(programRules);
		
		GeneralizedPivotSet generalizedPivotSet = new GeneralizedPivotSet();
		
		CompilerTypeBase headArg, bodyArg;
		Set<CompilerTypeBase> usedArgs = new HashSet<>();
		List<Equation> equations = new ArrayList<>();
		
		Map<String, Integer> predicateArityMap = new LinkedHashMap<>();
		
		for (Rule rule : recursiveRules) {
			Predicate head = rule.getHead();
			predicateArityMap.put(head.getPredicateName(), head.getArity());
			// for each variable or constant in the head, identify the positions in which it occurs
			for (int i = 0; i < head.getArity(); i++) {
				headArg = head.getArgument(i);
				// we exempt aggregate variables from consideration
				if (!usedArgs.contains(headArg) && (headArg.isVariable() || headArg.isConstant()) && !isAggregateVariable(headArg, rule)) {
					int[] headArgs = new int[head.getArity()];
					for (int j = i; j < head.getArity(); j++)
						if ((head.getArgument(j) == headArg))
							headArgs[j] = 1;

					for (Predicate literal : rule.getBody()) {
						// only consider derived (intensional) body literals
						if (literal.getPredicateType() == PredicateType.DERIVED) { 
							int[] bodyArgs = new int[literal.getArity()];
							for (int k = 0; k < literal.getArity(); k++) {
								bodyArg = literal.getArgument(k);
								if (bodyArg.equals(headArg) && (headArg.getType() == bodyArg.getType()))
									bodyArgs[k] = 1;
							}
							
							Equation eq = new Equation(headArgs, bodyArgs, literal.getPredicateName());
							equations.add(eq);

							predicateArityMap.put(literal.getPredicateName(), literal.getArity());
						}
					}
					usedArgs.add(headArg);
				}
			}
		}
		
		if (equations.isEmpty())
			return generalizedPivotSet;
						
		// pred -> [0,1,2,3], pred2 -> [4,5,6], pred3 -> [7,8]
		Map<String, int[]> predicateToMatrixRowIndexMap = new LinkedHashMap<>();  // for lookup by predicate + predicate index position
	
		int counter = 0;
		
		Iterator<String> keyIter = predicateArityMap.keySet().iterator();

		// create a map between predicate|index and global index in matrix row		
		while (keyIter.hasNext()) {
			String key = keyIter.next();
			int value = predicateArityMap.get(key);

			int[] map = new int[value];
			predicateToMatrixRowIndexMap.put(key, map);
			for (int i = 0; i < value; i++)
				map[i] = i + counter;

			counter += value;
		}

		List<double[]> matrixRows = new ArrayList<>();
		
		double[] equation;
		for (Equation eq : equations) {
			equation = new double[counter];
			for (int i = 0; i < eq.left.length; i++)
				equation[i] = eq.left[i];
			
			for (int i = 0; i < eq.right.length; i++)
				equation[predicateToMatrixRowIndexMap.get(eq.rightPredicateName)[i]] -= eq.right[i];

			matrixRows.add(equation);
		}
		
		int[] gps = GeneralizedPivotingSolver.determineGeneralizedPivotSet(matrixRows);
		if (gps != null) {
			Iterator<Map.Entry<String, int[]>> iter = predicateToMatrixRowIndexMap.entrySet().iterator();
			
			while (iter.hasNext()) {
				// predicatename -> int[]
				Map.Entry<String, int[]> pred = iter.next(); 

				int[] positions = new int[predicateArityMap.get(pred.getKey())];
				
				int[] predMapping = pred.getValue();
				for (int i = 0; i < predMapping.length; i++)
					positions[i] = gps[predMapping[i]];
				
				generalizedPivotSet.add(pred.getKey(), positions);
			}
		}

		// APS 8/15/2015
		// finally, remove sets that belong to aggregate nodes that are produced during rewriting
		// exposing them to the caller does them no good
		// This last block of code is cleaner then integrating it with code above
		for (int i = generalizedPivotSet.set.size() - 1; i >=0; i--) {
			String predicateName = generalizedPivotSet.set.get(i).predicateName;
			//int arity = generalizedPivotSet.set.get(i).positions.length;
			if (predicateName.startsWith(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX))
				generalizedPivotSet.set.remove(i);
			// APS 8/17/2015 also remove gps if no exit rule to modify
			//else if (!hasExitRules(predicateName, arity, programRules, recursiveRules))
			//	generalizedPivotSet.set.remove(i);
		}
				
		return generalizedPivotSet;
	}
	
	// the equations provided to this function are of the form pi1, pi2... pin, and do not include the y slack variable
	// y must be added to the end of each equation
	public static int[] determineGeneralizedPivotSet(List<double[]> equations) {
		int numberOfEquationVariables = equations.get(0).length;
		Collection<LinearConstraint> constraints = new ArrayList<LinearConstraint>();

		for (double[] equation : equations)
			constraints.add(new LinearConstraint(equation, Relationship.EQ, 0));
				
		// add constraint for inequality that all sum of variables must exceed 0
		double[] sumOfAllGreaterThanZeroConstraint = new double[numberOfEquationVariables];
		Arrays.fill(sumOfAllGreaterThanZeroConstraint, 1);
		constraints.add(new LinearConstraint(sumOfAllGreaterThanZeroConstraint, Relationship.GEQ, 1));
		
		LinearObjectiveFunction f = new LinearObjectiveFunction(sumOfAllGreaterThanZeroConstraint, 0);
		PointValuePair results = null;
		
		try{
			results = new SimplexSolver().optimize(f, constraints, GoalType.MINIMIZE, true);
		} catch (NoFeasibleSolutionException nfsex) {
			return null;
		}
		
		int[] integers = convertToIntegers(results.getPoint());
		return Arrays.copyOf(integers, integers.length);
	}
	
	private static int[] convertToIntegers(double[] doubles) {
		int[] integers = new int[doubles.length];
		Fraction[] fractions = new Fraction[doubles.length];
				
		boolean hasFractions = false;
		for (int i = 0; i < doubles.length; i++) {
			if (!((doubles[i] == Math.floor(doubles[i])) && !Double.isInfinite(doubles[i])))
				hasFractions = true;
		}
		
		if (hasFractions) {
			for (int i = 0; i < doubles.length; i++)
				fractions[i] = new Fraction(doubles[i]);
			
			integers = convertToIntegers(fractions);
		} else {
			for (int i = 0; i < doubles.length; i++) 
				integers[i] = (int)doubles[i];
		}
		
		return integers;
	}
	
	private static int[] convertToIntegers(Fraction[] fractions) {
		List<Integer> divisors = new ArrayList<>();
		for (int i = 0; i < fractions.length; i++) {
			if (fractions[i] != null)
				divisors.add(fractions[i].getDenominator());
		}
		
		Iterator<Integer> iter = divisors.iterator();
		
		int lcm = iter.next();
		while (iter.hasNext())
			lcm = org.apache.commons.math3.util.ArithmeticUtils.lcm(lcm, iter.next());

		int[] integers = new int[fractions.length];
		for (int i = 0; i < fractions.length; i++)
			integers[i] = fractions[i].multiply(new Fraction(lcm)).intValue();

		return integers;
	}
	
	private static boolean isAggregateVariable(CompilerTypeBase arg, Rule rule) {
		if ((arg instanceof CompilerVariable) 
				&& ((CompilerVariable)arg).getVariableName().startsWith(AggregateRewriter.FS_AGGR_VALUE_PREFIX)) {
			return true;
		}
		
		// inspect the aggregate adornment to identify variables produced from aggregate functions
		Predicate head = rule.getHead();
		for (int i = 0; i < head.getArity(); i++)
			if (head.getArgument(i) == arg)
				if (head.getArgumentTypeAdornment().get(i) == ArgumentType.FSAGGREGATE)
					return true;
		
		for (Predicate literal : rule.getBody()) {
			for (int i = 0; i < literal.getArity(); i++)
				if (literal.getArgument(i) == arg)
					if (literal.getArgumentTypeAdornment().get(i) == ArgumentType.FSAGGREGATE)
						return true;
		}
		return false;
	}
		
	private static List<Rule> getRecursiveRules(ProgramRules programRules) {
		Set<String> recursivePredicates = new HashSet<>();
		
		List<Rule> recursiveRules = new ArrayList<>();
		for (BasicClique clique : programRules.getCliques())
			for (DerivedPredicate dp : clique.getDerivedPredicates())
				for (Rule rule : dp.getRules())
					recursivePredicates.add(rule.getHead().getPredicateName() + "|" + rule.getHead().getArity());
		
		for (BasicClique clique : programRules.getCliques())
			for (DerivedPredicate dp : clique.getDerivedPredicates())
				for (Rule rule : dp.getRules())
					for (Predicate literal : rule.getBody())
						if (recursivePredicates.contains(literal.getPredicateName() + "|" + literal.getArity()))
							recursiveRules.add(rule);

		return recursiveRules;
	}
	
	private static boolean hasExitRules(String predicateName, int arity, ProgramRules programRules, List<Rule> recursiveRules) {
		for (BasicClique clique : programRules.getCliques()) {
			DerivedPredicate dp = clique.getDerivedPredicate(predicateName, arity);
			for (Rule rule : dp.getRules())
				if (!recursiveRules.contains(rule))
					return true;			
		}
		
		return false;
	}
	
	private static void test1() {
		LinearObjectiveFunction f = new LinearObjectiveFunction(new double[] { 2, 2, 1 }, 0);
		Collection<LinearConstraint> constraints = new ArrayList<>();

		constraints.add(new LinearConstraint(new double[] { 1, 1, 0 }, Relationship.GEQ,  1));
		constraints.add(new LinearConstraint(new double[] { 1, 0, 1 }, Relationship.GEQ,  1));
		constraints.add(new LinearConstraint(new double[] { 0, 1, 0 }, Relationship.GEQ,  1));
		PointValuePair solution = new SimplexSolver().optimize(f, constraints, GoalType.MINIMIZE, true);
		System.out.println(Arrays.toString(solution.getPoint()));
		System.out.println(solution.getValue());
	}
	
	private static void test2() {
		LinearObjectiveFunction f = new LinearObjectiveFunction(new double[] { 1, 1, 1, 1, 1, 1, 1, 1 }, 0);
		Collection<LinearConstraint> constraints = new ArrayList<>();

		constraints.add(new LinearConstraint(new double[]{ 1, 1, 0, 0, -1, 0, 0, 0}, Relationship.EQ, 0));
		constraints.add(new LinearConstraint(new double[]{ 0, 1, 0, -1, 0, 0, 0, 0}, Relationship.EQ, 0));		
		constraints.add(new LinearConstraint(new double[]{ 0, 0, 1, 0, 0, 0, 0, 0}, Relationship.EQ, 0));
		constraints.add(new LinearConstraint(new double[]{ 0, 0, 1, 0, 0, 0, -1, 0}, Relationship.EQ, 0));
		//constraints.add(new LinearConstraint(new double[]{ 0, -1, 0, 1, 0, 0, 0, 0}, Relationship.EQ, 0));
		constraints.add(new LinearConstraint(new double[]{ 0, 0, 0, 1, 0, -1, 0, 0}, Relationship.EQ, 0));
		constraints.add(new LinearConstraint(new double[]{ 1, 1, 1, 1, 1, 1, 1, 1}, Relationship.GEQ, 1));		
		PointValuePair solution = new SimplexSolver().optimize(f, constraints, GoalType.MINIMIZE, true);
		System.out.println(Arrays.toString(solution.getPoint()));
		System.out.println(solution.getValue());
	}
	
	private static void test3() {
		List<double[]> equations = new ArrayList<>();		
		equations.add(new double[]{ 1, 1, 0, 0, 0,-1, 0, 0 });
		equations.add(new double[]{ 0, 1, 0, -1, 0, 0, 0, 0 });
		equations.add(new double[]{ 0, -1, 0, 1, 0, 0, 0, 0 });
		equations.add(new double[]{ 0, 0, 1, 0, 0, 0, 0, 0 });
		equations.add(new double[]{ 0, 0, 1, 0, 0, 0, 0, -1 });
		equations.add(new double[]{ 0, 0, 0, 1, 0, 0, -1, 0 });
		
		int[] solution = determineGeneralizedPivotSet(equations);
		System.out.println(Arrays.toString(solution));
	}
	
	private static void test4() {
		List<double[]> equations = new ArrayList<>();		
		equations.add(new double[]{ 1, 0 });
		
		int[] solution = determineGeneralizedPivotSet(equations);
		System.out.println(Arrays.toString(solution));
	}
	
	private static void test5() {
		List<double[]> equations = new ArrayList<>();		
		equations.add(new double[]{ 0, -1, 0, 1 });
		equations.add(new double[]{ 0, 0, -1, 1 });
		equations.add(new double[]{ 1, -1, -1, 0 });
		equations.add(new double[]{ 0, 1, 0, -1 });
		equations.add(new double[]{ 0, 0, 1, -1 });
		equations.add(new double[]{ -1, 1, 1, 0 });
		
		int[] solution = determineGeneralizedPivotSet(equations);
		System.out.println(Arrays.toString(solution));
	}
	
	public static void main(String[] args) {
		test1();
		test2();
		test3();
		test4();
		test5();
	}
}