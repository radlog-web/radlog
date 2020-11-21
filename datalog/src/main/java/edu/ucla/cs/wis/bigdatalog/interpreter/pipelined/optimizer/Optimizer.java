package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.optimizer;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.api.CommandResult;
import edu.ucla.cs.wis.bigdatalog.api.CommandWrapper;
import edu.ucla.cs.wis.bigdatalog.api.QueryResults;
import edu.ucla.cs.wis.bigdatalog.api.TimerOption;
import edu.ucla.cs.wis.bigdatalog.common.FileUtilities;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.interpreter.PipelinedProgram;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.AndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ExecutionMode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.Node;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramGenerator;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.FSCliqueComparisonReachedNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.MonotonicAggregateComparisonReachedNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.StratifiedAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison.ComparisonNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.EMSNCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.FSCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.BaseRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.ReadOnlyRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;
import edu.ucla.cs.wis.bigdatalog.system.SystemCommandResult;

public class Optimizer {
	private static Logger logger = LoggerFactory.getLogger(Optimizer.class.getName());
	private static int predicateId;
	
	private DeALSContext deALSContext;

	public Optimizer(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	
	public void optimize(QueryForm queryForm, PipelinedProgram program) {
		this.deALSContext.getConfiguration().setProperty("deals.interpreter.optimize", "false");
		
		this.deALSContext.logInfo(logger, "[BEGIN Step 10 - Optimize Program for Query Form '{}']{}\n", queryForm.toString(), program.toString());
				
		// Step 1) remove the sealing aggregates 
		// this requires materializing the execution of the recursive node, which is done in Step 3;
		removeSealingAggregates(program, null, program.getRoot());
		
		// Step 2) compress variable assignments to minimize dereferencing
		ProgramGenerator.compressVariableAssignments(program.getRoot());
		
		this.deALSContext.logInfo(logger, "[BEGIN Step 10a - Removed Sealing Aggregates for Query Form '{}' BEGIN]{}", queryForm, program);
		this.deALSContext.logInfo(logger, "[END Step 10a - Removed Sealing Aggregates for Query Form '{}' END]\n", queryForm);
		
		// Step 3) compress tree by removing unnecessary nodes
		compress(program);
		
		this.deALSContext.logInfo(logger, "[BEGIN Step 10b - Compressed Program for Query Form '{}' BEGIN]{}", queryForm, program);
		this.deALSContext.logInfo(logger, "[END Step 10b - Compressed Program for Query Form '{}' END]\n", queryForm);
		
		// Step 4) set materialized execution mode for certain aggregates and recursive nodes based on DeAL rules 
		optimizeExecution(program);
				
		this.deALSContext.logInfo(logger, "[BEGIN Step 10c - After Optimizing Execution '{}' BEGIN]{}", queryForm, program);
		this.deALSContext.logInfo(logger, "[END Step 10c - After Optimizing Execution '{}' END]\n", queryForm);
			
		this.deALSContext.getConfiguration().setProperty("deals.interpreter.optimize", "true");		
	}
	
	private void optimizeExecution(PipelinedProgram program) {
		Module originalActiveModule = this.deALSContext.getModuleManager().getActiveModule();
		int originalLogLevelId = this.deALSContext.getLogLevel();
		this.deALSContext.setLogLevel(DeALSContext.DEFAULT_LOG_LEVEL);
		Map<Integer, Node<?>> nodes = new HashMap<>();
		Map<Node<?>, Node<?>> dependencies = new HashMap<>(); // child, parent
		Map<Integer, Argument> arguments = new HashMap<>(); // argumentid, argument
		
		if (this.loadOptimizerRules(program.getRoot(), nodes, dependencies, arguments) == ReturnStatus.SUCCESS) {
			// Step 1) materialize eligible recursive recursive and aggregate nodes
			QueryResults results = executeQuery("nodes_to_materialize(ID).");
			for (Tuple tuple : results.getResults()) {
				int id = ((DbInteger)tuple.columns[0]).getValue();
				((RelationNode)nodes.get(id)).setExecutionMode(ExecutionMode.Materialized);
			}
			
			// Step 2) replace recursive or aggregate nodes with node to read from earlier materialized result
			results = executeQuery("nodes_to_replace(ID, NodeType).");
			for (Tuple tuple : results.getResults()) {
				int id = ((DbInteger)tuple.columns[0]).getValue();
				String className = ((DbString)tuple.columns[1]).getValue();
				
				if (className.equals("RecursiveOrNode")) 
					continue;
				
				Node<?> node = nodes.get(id);
				if (node != null) {
					int i;
					ReadOnlyRelationNode replacementNode = new ReadOnlyRelationNode(node.getPredicateName(), 
							node.getArguments(), 
							node.getBindingPattern(),
							((OrNode)node).getFreeVariableList(), 
							className.equals("RecursiveOrNode"));
					
					AndNode parent = (AndNode) dependencies.get(node);
					for (i = 0; i < parent.getNumberOfChildren(); i++) {
						if (parent.getChild(i) == node)
							break;
					}
					
					if (i < parent.getNumberOfChildren())
						parent.replaceChild(i, replacementNode);
				}
			}
			 
			// Step 3) optimize base relations
			results  = executeQuery("bplustree_base_relations(ID).");
			for (Tuple tuple : results.getResults()) {
				int id = ((DbInteger)tuple.columns[0]).getValue();
				Node<?> node = nodes.get(id);
				if (node != null)
					((BaseRelationNode)node).setAsBPlusTreeRelation();
			}
			
			// Step 4) optimize monotonic comparison
			results = executeQuery("fs_cliques_with_comparison(ID, ComparisonNodeID, AggregateArgument).");
			for (Tuple tuple : results.getResults()) {
				int id = ((DbInteger)tuple.columns[0]).getValue();
				int comparisonNodeId = ((DbInteger)tuple.columns[1]).getValue();
				int aggrArgumentId = ((DbInteger)tuple.columns[2]).getValue();
				Node<?> node = nodes.get(id);
				Node<?> parentNode = dependencies.get(node);
				Node<?> comparisonNode = nodes.get(comparisonNodeId);
				Argument aggrArgument = arguments.get(aggrArgumentId);
								
				// first add tracker node after comparison operator			
				FSCliqueComparisonReachedNode tracker = this.addTrackerNode((AndNode)parentNode, (RecursiveOrNode)node, 
						(ComparisonNode)comparisonNode, aggrArgument); 
								
				// then set tracker to clique node
				((EMSNCliqueNode)((RecursiveOrNode)node).getClique()).setupForComparison(tracker);					
			}
			this.deALSContext.getModuleManager().removeModule("optimizer");
		}
		// reset module back to original default module
		this.deALSContext.getModuleManager().setActiveModule(originalActiveModule);
		this.deALSContext.setLogLevel(originalLogLevelId);
		
		//this.optimizeMonotonicAggregateComparison(program);
	}
	
	private FSCliqueComparisonReachedNode addTrackerNode(AndNode parentAndNode, RecursiveOrNode recursiveOrNode, 
			ComparisonNode comparisonNode, Argument aggrArgument) {
		NodeArguments keyArgs = new NodeArguments();
		for (int i = 0; i < recursiveOrNode.getArity(); i++)
			keyArgs.add(recursiveOrNode.getArgument(i));
			
		keyArgs.remove(aggrArgument); 
		
		Binding binding = new Binding(keyArgs.size(), BindingType.BOUND);
		FSCliqueComparisonReachedNode tracker = new FSCliqueComparisonReachedNode("tracker", keyArgs, binding, new VariableList());
		int i;
		for (i = 0; i < parentAndNode.getNumberOfChildren(); i++) {
			if (parentAndNode.getChild(i) == comparisonNode)
				break;
		}

		parentAndNode.insertChild(tracker, i+1);
		int[] backtrackMap = new int[parentAndNode.getBacktrackMap().length + 1];
		for (int j = 0; j < parentAndNode.getBacktrackMap().length; j++)
			backtrackMap[j] = parentAndNode.getBacktrackMap()[j];
		backtrackMap[backtrackMap.length - 1] = backtrackMap[backtrackMap.length - 2];
		if (parentAndNode.getRuleBacktrackPoint() == (parentAndNode.getBacktrackMap().length - 1))
			parentAndNode.setRuleBacktrackPoint(parentAndNode.getRuleBacktrackPoint()+1);
		parentAndNode.setBacktrackMap(backtrackMap);				
			
		return tracker;		
	}
	
	private QueryResults executeQuery(String query) {
		CommandWrapper commandWrapper = new CommandWrapper(this.deALSContext);
		commandWrapper.getOptions().setTimer(TimerOption.QUERY);
		CommandResult commandResult = commandWrapper.execute("query " + query);
		return commandResult.getQueryResults();
	}
	
	private ReturnStatus loadOptimizerRules(OrNode root, Map<Integer, Node<?>> nodes, Map<Node<?>, Node<?>> dependencies, Map<Integer, Argument> arguments) {
		String data = "begin module 'optimizer'.\n";
		data += FileUtilities.getFileContents(Optimizer.class.getResourceAsStream("optimizer_rules.deal"));
		data += "end module.";
		SystemCommandResult scr = this.deALSContext.loadDatabaseObjects(data);
		if (scr.getStatus() == ReturnStatus.SUCCESS) {
			Module module = this.deALSContext.getModuleManager().getModule("optimizer");
			this.deALSContext.getModuleManager().setActiveModule(module);
			
			StringBuilder factStr = new StringBuilder();
			generateFacts(root, 0, factStr, nodes, dependencies, arguments);
			
			//System.out.println(factStr.toString());
			
			scr = this.deALSContext.load(true, factStr.toString());
			if (scr.getStatus() == ReturnStatus.SUCCESS)
				deALSContext.compileAllQueryForms();			
		}
		
		return scr.getStatus();
	}
	
	private static int generateFacts(Node<?> node, int depth, StringBuilder factStr, Map<Integer, Node<?>> nodes, 
			Map<Node<?>, Node<?>> dependencies, Map<Integer, Argument> arguments) {
		int predId = generatePredicateFact(node, depth, factStr, arguments);
		nodes.put(predId, node);
		int childPredId;

		if (node instanceof RecursiveOrNode) {
			CliqueBaseNode clique = ((RecursiveOrNode)node).getClique();
			dependencies.put(clique, node);
			childPredId = generateFacts(clique, depth+1, factStr, nodes, dependencies, arguments);
			generateDependencyFact(predId, childPredId, factStr);
			
			OrNode exitRules = clique.getExitRulesOrNode();
			dependencies.put(exitRules, clique);
			int grandChildPredId = generateFacts(exitRules, depth+2, factStr, nodes, dependencies, arguments);
			generateDependencyFact(childPredId, grandChildPredId, factStr);			
			
			if (((RecursiveOrNode)node).getClique() instanceof CliqueNode) {
				OrNode recursiveRules = ((CliqueNode)((RecursiveOrNode)node).getClique()).getRecursiveRulesOrNode();
				dependencies.put(recursiveRules, clique);
				grandChildPredId = generateFacts(recursiveRules, depth+2, factStr, nodes, dependencies, arguments);
				generateDependencyFact(childPredId, grandChildPredId, factStr);
			}
		}
		
		for (edu.ucla.cs.wis.bigdatalog.interpreter.Node<?> child : node.getChildren()) {
			dependencies.put((Node<?>) child, node);
			childPredId = generateFacts((Node<?>) child, depth+1, factStr, nodes, dependencies, arguments);
			generateDependencyFact(predId, childPredId, factStr);	
		}
		
		return predId;
	}
	
	private static void generateDependencyFact(int predId, int childPredId, StringBuilder factStr) {
		factStr.append("predicateDependency(");
		factStr.append(predId);
		factStr.append(",");
		factStr.append(childPredId);
		factStr.append(").\n");	
	}
	
	private static int generatePredicateFact(Node<?> node, int depth, StringBuilder factStr, Map<Integer, Argument> arguments) {
		int predId = ++predicateId;
		factStr.append("predicate(");
		factStr.append(predId);
		factStr.append(",'");
		factStr.append(node.getPredicateName() + "',");
		factStr.append(depth);
		factStr.append(",'");
		factStr.append(node.getClass().getSimpleName());
		factStr.append("').\n");
		
		Argument arg;
		int argId;
		for (int i = 0; i < node.getArity(); i++) {
			arg = node.getArgument(i);
			argId = arg.hashCode();
			arguments.put(argId, arg);
			factStr.append(arg.toFact() + "\n");
			
			factStr.append("argumentPredicate(");
			factStr.append(predId);
			factStr.append(",");
			factStr.append(argId);
			factStr.append(").\n");

			factStr.append("binding(");
			factStr.append(argId);
			factStr.append(",");
			factStr.append(predId);
			factStr.append(",");
			factStr.append(i);
			factStr.append(",'");
			factStr.append(node.getBinding(i).toString());
			factStr.append("').\n");
		}

		return predId;
	}

	private static void compress(PipelinedProgram program) {
		program.setRoot(compress(program.getRoot()));
	}
	
	private static OrNode compress(OrNode orNode) {
		// compress orNode if 1 child andNode with 1 child orNode, all with same arity
		// an orNode.class can only have andNode children, but there can be different kinds of andNodes
		boolean isMatch = true;
		while ((isMatch && (orNode.getNumberOfChildren() == 1) && (orNode.getClass() == OrNode.class))
				&& (orNode.getChild(0).getNumberOfChildren() == 1) && orNode.getChild(0).getClass().equals(AndNode.class)
				&& (orNode.getArity() == orNode.getChild(0).getChild(0).getArity())) {
			
			for (int i = 0; i < orNode.getArity() && isMatch; i++) {
				if (orNode.getArgument(i) != orNode.getChild(0).getChild(0).getArgument(i))
					isMatch = false;
			}
			
			if (isMatch)
				orNode = orNode.getChild(0).getChild(0);
		}
		
		if (orNode instanceof RecursiveOrNode) {
			compress(((RecursiveOrNode)orNode).getClique().getExitRulesOrNode());
			if (((RecursiveOrNode)orNode).getClique() instanceof CliqueNode) 
				compress(((CliqueNode)((RecursiveOrNode)orNode).getClique()).getRecursiveRulesOrNode());
		} else {
			OrNode compressedNode = null;
			for (AndNode child : orNode.getChildren()) {
				for (int i = 0; i < child.getNumberOfChildren(); i++) {
					compressedNode = compress(child.getChild(i));
					if (compressedNode != child.getChild(i))
						child.replaceChild(i, compressedNode);				
				}
			}
		}
		
		return orNode;
	}
	
	private static PipelinedProgram removeSealingAggregates(PipelinedProgram program, AndNode parentNode, OrNode orNode) {
		/*Sealing aggregate rules have the form:
		 *   fsminshortestpathsLL(X, Z, min<D>) <- fpath2LL(X, Z, D).
		 * where fspath2LL is:
		 *   fpath2LL(X,Y,fsmin<D>) <- arcW(X, Y, D).
		 *   fpath2LL(X,Z,fsmin<D>) <- fpath2LL(X, Y, D1), arcW(Y, Z, D2), D = D1 + D2.
		 */
		// replaced aggregate with ornode in sealing aggregate rules
		// these are min and max stratified aggregates over a literal that is an fs aggregate predicate

		boolean isSealingAggregate = false;
		if (orNode.getClass() == StratifiedAggregateRelationNode.class) {
			// always only one child andNode under StratifiedAggregateRelationNode
			for (OrNode child : orNode.getChild(0).getChildren()) {
				if (child instanceof RecursiveOrNode) {
					if (((RecursiveOrNode)child).getClique() instanceof FSCliqueNode) {
						isSealingAggregate = true;					
						break;
					}
				}
			}
		}
		
		if (isSealingAggregate) {
			OrNode replacementOrNode = new OrNode(orNode.getPredicateName(), 
					orNode.getArguments(), 
					orNode.getBindingPattern(), 
					orNode.getFreeVariableList());
			replacementOrNode.addChild(orNode.getChild(0));
			
			StratifiedAggregateRelationNode aggregateNode = (StratifiedAggregateRelationNode)orNode;
			
			// for each aggregate, get its term and set it equal to the child's value in the same place
			// count from the right
			for (int i = aggregateNode.getArity() - 1; i >= (aggregateNode.getArity() - aggregateNode.getAggregateInfos().length); i--) {
				 Variable var = (Variable)aggregateNode.getArgument(i);
				 var.setValue(orNode.getChild(0).getArgument(i));
			}

			// root has no parent
			if (parentNode == null) {
				program.setRoot(replacementOrNode);
			} else {
				int index;
				for (index = 0; index < parentNode.getNumberOfChildren(); index++)
					if (parentNode.getChild(index) == orNode)
						break;
			
				parentNode.replaceChild(index, replacementOrNode);
			}
		} else {
			for (AndNode child : orNode.getChildren()) {
				for (OrNode grandChild : child.getChildren())
					removeSealingAggregates(program, child, grandChild);				
			}
		}
		
		return program;
	}
	
	private void optimizeMonotonicAggregateComparison(PipelinedProgram program) {
		optimizeMonotonicAggregateComparison(program.getRoot());
	}
	
	private void optimizeMonotonicAggregateComparison(OrNode orNode) {
		for (AndNode child : orNode.getChildren())
			optimizeMonotonicAggregateComparison(child);		
	}
	
	private void optimizeMonotonicAggregateComparison(AndNode andNode) {
		if (andNode.getNumberOfChildren() > 1) {			
			// 1) identify variable used in comparison predicate
			// 2) if we have mononotic aggregate, insert tracking predicate
			// 3) append tracking predicate after comparison
			Variable comparedVariable = identifyComparison(andNode);
			boolean found = false;
			if (comparedVariable != null) {
				for (Argument arg : andNode.getArguments().innerArguments) {
					if (arg == comparedVariable) {
						found = true;
						break;
					}
				}
				// if the variable used in the comparison is returned from andNode, do not optimize
				if (!found) {
					OrNode orNodeWithMonotonicAggregate = null;
					MonotonicAggregateComparisonReachedNode trackerNode = null;
					for (OrNode child : andNode.getChildren()) {
						if (child.getClass().equals(OrNode.class)) {
							trackerNode = adjustMonotonicAggregate(child, comparedVariable);
							orNodeWithMonotonicAggregate = child;
							break;
						}
					}
	
					if (trackerNode != null) {
						NodeArguments args = new NodeArguments();
						for (int i = 0; i < orNodeWithMonotonicAggregate.getArity(); i++)
							if (orNodeWithMonotonicAggregate.getArgument(i) != comparedVariable)
								args.add(orNodeWithMonotonicAggregate.getArgument(i));
						
						Binding binding = new Binding(args.size(), BindingType.BOUND);						
						MonotonicAggregateComparisonReachedNode tracker = new MonotonicAggregateComparisonReachedNode("tracker", args, binding, new VariableList(), false);
						tracker.setCompanion(trackerNode);
						andNode.addChild(tracker);
						int[] backtrackMap = new int[andNode.getBacktrackMap().length + 1];
						for (int i = 0; i < andNode.getBacktrackMap().length; i ++)
							backtrackMap[i] = andNode.getBacktrackMap()[i];
						backtrackMap[backtrackMap.length - 1] = backtrackMap[backtrackMap.length - 2];
						if (andNode.getRuleBacktrackPoint() == (andNode.getBacktrackMap().length - 1))
							andNode.setRuleBacktrackPoint(andNode.getRuleBacktrackPoint()+1);
						andNode.setBacktrackMap(backtrackMap);
					}
				}
			}
		}
		
		this.optimizeMonotonicAggregateComparison(andNode.getChild(0));
	}
	
	private MonotonicAggregateComparisonReachedNode adjustMonotonicAggregate(OrNode orNode, Variable comparedVariable) {
		if (orNode.getChild(0).getChild(0) instanceof FSAggregateRelationNode) {
			NodeArguments usedArgs = new NodeArguments();
			for (int i = 0; i < orNode.getArity(); i++)
				usedArgs.add(orNode.getArgument(i));
			
			usedArgs.remove(comparedVariable); 
			NodeArguments returnedArgs = usedArgs.copy();
			FSAggregateRelationNode aggrNode = (FSAggregateRelationNode)orNode.getChild(0).getChild(0);
			if (aggrNode.getArgument(aggrNode.getArity() - 1) == comparedVariable) {
				AndNode child = aggrNode.getChild(0);
				int i;
				OrNode grandChild;
				for (i = 0; i < child.getChildren().length; i++) {
					grandChild = child.getChild(i);
					for (int j = 0; j < grandChild.getArity(); j++) {
						if (usedArgs.contains(grandChild.getArgument(j)))
							usedArgs.remove(grandChild.getArgument(j));

						if (usedArgs.isEmpty())
							break;
					}
					
					if (usedArgs.isEmpty())
						break;
				}
				
				Binding binding = new Binding(returnedArgs.size(), BindingType.BOUND);
				MonotonicAggregateComparisonReachedNode tracker = new MonotonicAggregateComparisonReachedNode("tracker", returnedArgs, binding, new VariableList(), true);
				
				child.insertChild(tracker, i+1);
				int[] backtrackMap = new int[child.getBacktrackMap().length + 1];
				for (int j = 0; j < child.getBacktrackMap().length; j++)
					backtrackMap[j] = child.getBacktrackMap()[j];
				backtrackMap[backtrackMap.length - 1] = backtrackMap[backtrackMap.length - 2];
				if (child.getRuleBacktrackPoint() == (child.getBacktrackMap().length - 1))
					child.setRuleBacktrackPoint(child.getRuleBacktrackPoint()+1);
				child.setBacktrackMap(backtrackMap);				
					
				return tracker;
			}
		}
		return null;
	}
	
	private Variable identifyComparison(AndNode andNode) {
		for (int i = 0; i < andNode.getNumberOfChildren(); i++) {
			if (i > 0 && andNode.getChild(i) instanceof ComparisonNode)
				if (((ComparisonNode)andNode.getChild(i)).getArgument(0) instanceof Variable)
					return (Variable) ((ComparisonNode)andNode.getChild(i)).getArgument(0);
		}
		return null;
	}
	
	private Variable identifyMonotonicAggregate(OrNode orNode) {
		VariableList variables = new VariableList();
		orNode.getVariables(variables);
		
		Variable aggrVar = null;
		OrNode grandChild = orNode.getChild(0).getChild(0);
		if (grandChild instanceof FSAggregateRelationNode) {
			if (grandChild.getArgument(grandChild.getArity() - 1) instanceof Variable)
				aggrVar = (Variable)grandChild.getArgument(grandChild.getArity() - 1);
		}
		return aggrVar;
	}
}
