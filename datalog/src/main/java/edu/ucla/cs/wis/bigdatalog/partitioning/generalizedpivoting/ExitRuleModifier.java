package edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting;

import edu.ucla.cs.wis.bigdatalog.interpreter.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison.ComparisonAllBoundNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class ExitRuleModifier {
	
	public static void addRestrictingPredicate(OrNode node, GeneralizedPivotSet gps, int index, int numberOfPartitions) {
		if (node instanceof RecursiveOrNode) {
			CliqueBaseNode clique = ((RecursiveOrNode)node).getClique();
			String predicateName = clique.getPredicateName().substring(0, clique.getPredicateName().lastIndexOf("_"));
			OrNode restrictingPredicate = doAddRestrictingPredicate(predicateName, clique.getArguments(), gps, index, numberOfPartitions);
			if (clique.getExitRulesOrNode().hasChildren()) {
				OrNode exitRulesOrNode = clique.getExitRulesOrNode();
				for (AndNode exitRulesAndNode : exitRulesOrNode.getChildren()) {
					int[] newBacktrackMap = new int[exitRulesAndNode.getNumberOfChildren() + 1];
					System.arraycopy(exitRulesAndNode.getBacktrackMap(), 0, newBacktrackMap, 0, exitRulesAndNode.getBacktrackMap().length);
					newBacktrackMap[newBacktrackMap.length - 1] = newBacktrackMap[newBacktrackMap.length - 2];					
					//System.out.println(Arrays.toString(exitRulesAndNode.getBacktrackMap()));
					//System.out.println(Arrays.toString(newBacktrackMap));				
					exitRulesAndNode.setBacktrackMap(newBacktrackMap);
					exitRulesAndNode.addChild(restrictingPredicate);
					restrictingPredicate.attachContext(exitRulesAndNode.getDeALSContext());
				}
			} else {
				OrNode oldExitRulesOrNode = clique.getExitRulesOrNode();
				OrNode newOrNode = new OrNode(clique.getPredicateName(), clique.getArguments(), clique.getBindingPattern(), new VariableList());
				AndNode newAndNode = new AndNode(clique.getPredicateName(), clique.getArguments(), clique.getBindingPattern());
				newAndNode.setRuleBacktrackPoint(0);
				int[] backtrackMap = new int[2];
				backtrackMap[0] = -1;
				backtrackMap[1] = 0;
				newAndNode.setBacktrackMap(backtrackMap);
				newOrNode.addChild(newAndNode);				
				newAndNode.addChild(oldExitRulesOrNode);
				newAndNode.addChild(restrictingPredicate);
				clique.addExitRulesOrNode(newOrNode);
				newOrNode.attachContext(clique.getDeALSContext());
			}
		}
	}

	private static OrNode doAddRestrictingPredicate(String predicateName, NodeArguments arguments, 
			GeneralizedPivotSet gps, int index, int numberofPartitions) {
		OrNode node = null;
		int[] positions = gps.get(predicateName);
		
		if ((positions != null) && (positions.length > 0)) {
			VariableList variableList = new VariableList();
			// gather variables we will use based on GPS
			for (int i = 0; i < arguments.size(); i++)
				if (positions[i] > 0)
					variableList.add((Variable)arguments.get(i));

			if (variableList.size() > 0) {		
				// x mod numberofPartitions == index
				Argument expr = variableList.get(0); 
				if (variableList.size() > 1) {		
					// x + y mod numberofPartitions == index
					for (int i = 1; i < variableList.size(); i++)
						expr = new BinaryExpression(ArithmeticOperation.ADDITION, expr, variableList.get(i), DataType.INT);					
				}

				expr = new BinaryExpression(ArithmeticOperation.MOD, expr, DbInteger.create(numberofPartitions), DataType.INT);
				
				Binding binding = new Binding(2);
				binding.setBinding(0, BindingType.BOUND);
				binding.setBinding(1, BindingType.BOUND);
				NodeArguments args = new NodeArguments();
				args.add(expr);
				args.add(DbInteger.create(index));
				node = new ComparisonAllBoundNode("=", args, new VariableList());
			}
		}
		return node;
	}	
}
