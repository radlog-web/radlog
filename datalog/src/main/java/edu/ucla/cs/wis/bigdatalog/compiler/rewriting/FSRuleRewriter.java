package edu.ucla.cs.wis.bigdatalog.compiler.rewriting;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class FSRuleRewriter {
	private static Logger logger = LoggerFactory.getLogger(FSRuleRewriter.class.getName());

	private DeALSContext deALSContext;
	public FSRuleRewriter(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	
	// APS added 5/13/2014 to consolidate with addition of running fs pushdown
	public void optimize(PCGOrNode orNode) {
		//pushdownFSSelectionPredicatesOrNode(orNode);
				
		// APS 11/27/2013
		// apply optimization to short ciruit predicates evaluating FS values 
		// the only way to know if the node can be optimized is to look at the siblings and parents as well
		//if (this.deALSContext.getConfiguration().compareProperty("deals.compiler.rewriter.reversemagic", "true"))
		//this.injectReverseMagicPredicateForFSBodyComparison(orNode);
		
		// insert a predicate that will guide the search through the tuples being aggregated
		// to do this we need to do the following:
		// 1) find a rule body that has an FS Value that is not being passed to the rule head
		//   we do this as:
		//	- find the predicate with argument type 'FSAggregate' that is not being used in the parent
		// 2) find the literals being executed by the aggregate
		// 3) inject a copy of the predicate(s) being executed by the aggregate that push variables to the FS aggregate predicate that produced the FSValue
		this.injectReverseMagicPredicateOrNode(orNode, null);
	}
			
	private static void pushdownFSSelectionPredicatesOrNode(PCGOrNode orNode) {
		for (PCGOrNodeChild child : orNode.getChildren())
			if (child instanceof PCGAndNode)
				pushdownFSSelectionPredicatesAndNode((PCGAndNode)child);
	}
		
	private static void pushdownFSSelectionPredicatesAndNode(PCGAndNode andNode) {
		FSAggregate fsAggr = null;
		PCGOrNode orNodeToPushdown = null;
		PCGOrNode orNodeToPushInto = null;
		for (PCGOrNode orNode : andNode.getChildren()) {
			if (fsAggr == null) {
				for (int i = 0; i < orNode.getArity(); i++) {
					if (orNode.getArgument(i).isFSAggregate()) {
						fsAggr = (FSAggregate) orNode.getArgument(i);
						orNodeToPushInto = orNode;
						break;
					}
				}
			} else {
				for (int i = 0; i < orNode.getArity(); i++) {
					if (orNode.getArgument(i).equals(fsAggr)) {
						if (orNode.getBuiltInPredicateType() == BuiltInPredicateType.BINARY) {
							BuiltInPredicate bip = (BuiltInPredicate)orNode.getPredicate();
							if (bip.isNonEqualityComparison()) {
								orNodeToPushdown = orNode;
								break;
							}
						}
					}
				}
				if (orNodeToPushdown != null)
					break;
			}		
		}

		if (fsAggr != null && orNodeToPushInto != null && orNodeToPushdown != null)
			pushdown(andNode, fsAggr, orNodeToPushInto, orNodeToPushdown);
		
		for (PCGOrNode child : andNode.getChildren())
			pushdownFSSelectionPredicatesOrNode(child);
	}
	
	private static void pushdown(PCGAndNode andNode, FSAggregate fsAggr, PCGOrNode orNodeToPushInto, PCGOrNode orNodeToPushdown) {
		// 1) get variable to replace in node being pushed
		// 2) swap out variable
		// 3) push node down as last child in aggregate 'Outer rule'
		// 4) remove node from original place
		CompilerVariable varToSwap = null;
		for (PCGOrNodeChild child : orNodeToPushInto.getChildren()) {
			if (orNodeToPushInto.getPredicateName().equals(((PCGAndNode)child).getPredicateName())) {
				int i;
				for (i = 0; i < orNodeToPushInto.getArity(); i++) {
					if (orNodeToPushInto.getArgument(i).equals(fsAggr))
						break;
				}
				
				if (i < orNodeToPushInto.getArity())
					varToSwap = (CompilerVariable) ((PCGAndNode)child).getArgument(i);
			}
			if (varToSwap != null)
				break;
		}
		
		if (varToSwap != null) {
			for (int i = 0; i < orNodeToPushdown.getArity(); i++) {
				if (orNodeToPushdown.getArgument(i).equals(fsAggr)) {
					orNodeToPushdown.getArguments().set(i, varToSwap);
					break;
				}
			}
			
			((PCGAndNode)orNodeToPushInto.getChild(0)).addChild(orNodeToPushdown);		

			andNode.removeChild(orNodeToPushdown);
		}
	}
		
	private void injectReverseMagicPredicateOrNode(PCGOrNode orNode, PCGAndNode parent) {
		// check if parent uses fs values in this node
		if (parent != null) {
			CompilerVariableList freeVariables = new CompilerVariableList();
			CompilerVariableList parentVariables = new CompilerVariableList();
			Utilities.getVariables(parent.getArguments(), parentVariables);

			boolean canOptimize = false;
			for (int i = 0; i < orNode.getArity(); i++) {
				if (orNode.getPredicate().getArgument(i).isVariable() && 
						(orNode.getPredicate().getArgumentTypeAdornment().get(i) == ArgumentType.FSAGGREGATE)) {
					CompilerVariable fsArgument = (CompilerVariable)orNode.getPredicate().getArgument(i);

					// this is the simple case, where the variable is used inside the body for >= or <= comparison purposes
					if (!parentVariables.contains(fsArgument) && fsValueUsedOnlyForComparison(fsArgument, orNode, parent)) {
						//!fsValueUsedForArithmetic(fsArgument, orNode, parent)) {
						// we might be able to optimize this since there is a variable not being passed to the head
						this.deALSContext.logInfo(logger, "Attempting to apply reverse magic predicate rewriting {} for variable {}", orNode.getPredicate(), fsArgument);

						// APS 3/23/2015 - just focus on all FREE for now
						if (orNode.getBindingPattern().allFree()) {
							// we only can optimize free variables
							for (int j = 0; j < orNode.getArity(); j++) {
								if ((orNode.getPredicate().getArgumentTypeAdornment().get(j) == ArgumentType.VARIABLE) 
										&& (orNode.getBinding(j) == BindingType.FREE)) {
									freeVariables.add((CompilerVariable)orNode.getArgument(j));
								}
							}
							
							if (!freeVariables.isEmpty()) {
								canOptimize = true;
								break;
							}
						}
					}
				}
			}

			// get the body of the fs aggregate whom's variable is being compared
			if (canOptimize) {
				CompilerVariableList variables = new CompilerVariableList();
				variables.appendList(freeVariables);
				
				if (orNode.getChild(0) instanceof Clique) {
				} else {
					List<PCGOrNode> predicates = getBodyOfFSAggregate(orNode, orNode.getPredicateName(), variables);
					pruneUnneededPredicates(predicates, freeVariables.copyList());
					// should be null under normal circumstances
					if (predicates != null) {
						PCGOrNode topLevelOrNode = new PCGOrNode(Rewriter.REVERSE_MAGIC_PREDICATE_NAME_PREFIX + predicates.get(0).getPredicateName(), freeVariables.toCompilerTypeList());
						topLevelOrNode.setBindingPattern(0, BindingType.FREE);
						PCGAndNode dummyAndNode = new PCGAndNode(Rewriter.REVERSE_MAGIC_PREDICATE_NAME_PREFIX + predicates.get(0).getPredicateName(), freeVariables.toCompilerTypeList());
						dummyAndNode.setBindingPattern(0, BindingType.FREE);
						// insert starting at head of children list
						for (PCGOrNode predicate : predicates)
							dummyAndNode.addChild(predicate);
		
						topLevelOrNode.addChild(dummyAndNode);
						parent.insertChild(0, topLevelOrNode);
	
						this.deALSContext.logInfo(logger, "Applied reverse magic predicate rewriting for variables {} before {}", freeVariables.toString(), orNode.getPredicate());
					}
				}
				return;
			}
		}
		
		for (PCGOrNodeChild child : orNode.getChildren()) {
			if (child instanceof PCGAndNode)
				injectReverseMagicPredicateAndNode((PCGAndNode)child);			
		}
	}	
	
	private void injectReverseMagicPredicateAndNode(PCGAndNode andNode) {
		// since we're modifying this list, grab a copy before iterating
		List<PCGOrNode> children = new ArrayList<>();
		
		for (PCGOrNode child : andNode.getChildren())
			children.add(child);
				
		for (PCGOrNode child : children)
			injectReverseMagicPredicateOrNode(child, andNode);
	}
	
	private static List<PCGOrNode> getBodyOfFSAggregate(PCGOrNode orNode, String fsAggregatePredicateName, CompilerVariableList variableList) {
		List<PCGOrNode> list = null;
		
		PCGAndNode bodyAndNode = (PCGAndNode)((PCGAndNode)orNode.getChild(0)).getChild(0).getChild(0);

		list = new ArrayList<>();

		for (PCGOrNode child : bodyAndNode.getChildren())
			list.add(child.copy(variableList));

		/*String predicateName = orNode.getPredicate().getPredicateName(); 
		if (predicateName.startsWith(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX))
				//&& predicateName.substring(0, predicateName.lastIndexOf("_")).endsWith(fsAggregatePredicateName) 
				//&& (orNode.getPredicate().getFSAggregateType() != FSAggregateType.NONE)) 
			{
			for (PCGOrNodeChild child : orNode.getChildren()) {
				if (child instanceof PCGAndNode) {
					list = getBodyOfFSAggregate((PCGAndNode)child, fsAggregatePredicateName, variableList);
					if (list != null)
						return list;
				}
			}
		}
		
		for (PCGOrNodeChild child : orNode.getChildren()) {
			if (child instanceof PCGAndNode) {
				list = getBodyOfFSAggregate((PCGAndNode)child, fsAggregatePredicateName, variableList);
				if (list != null)
					return list;
			}
		}*/
		return list;
	}
	
	private static List<PCGOrNode> getBodyOfFSAggregate(PCGAndNode andNode, String fsAggregatePredicateName, CompilerVariableList variableList) {
		List<PCGOrNode> list = null;
		String predicateName = andNode.getPredicate().getPredicateName();
		
		// the body of the fs aggregate rule is what we want
		// it is 2 levels deep though - skip over the andNode to its children 
		if (predicateName.equals(fsAggregatePredicateName)) {
			for (PCGOrNode fsAggregateChild : andNode.getChildren()) {
				if (fsAggregateChild.getPredicate().isBuiltIn()) {
					BuiltInPredicate bip = (BuiltInPredicate)fsAggregateChild.getPredicate();
					if (bip.getBuiltInPredicateType() == BuiltInPredicateType.AGGREGATE_FS) {
						//list = getBodyOfFSAggregate(fsAggregateChild, fsAggregatePredicateName, variableList);
						list = new ArrayList<>();

						for (PCGOrNode child : andNode.getChildren())
							list.add(child.copy(variableList));

						break;
					}
				}
			}
						
			return list;
		} else if ((FSAggregateType.isFSAggregateType(predicateName)
				&& predicateName.substring(0, predicateName.lastIndexOf("_")).endsWith(fsAggregatePredicateName) 
				&& andNode.getPredicate().getFSAggregateType() != FSAggregateType.NONE)) {
			list = new ArrayList<>();

			for (PCGOrNode child : andNode.getChildren())
				list.add(child.copy(variableList));

			return list;		
		}
		
		for (PCGOrNode child : andNode.getChildren()) {
			list = getBodyOfFSAggregate(child, fsAggregatePredicateName, variableList);
			if (list != null)
				return list;
		}
		return null;
	}
	/*
	private static boolean fsValueUsedForArithmetic(Variable fsArgument, PCGOrNode orNode, PCGAndNode parent) {
		int i;
		for (i = 0; i < parent.getNumberOfChildren(); i++)
			if (parent.getChild(i) == orNode)
				break;
				
		// any siblings after the orNode are possible candidates to perform arithmetic on the fsvalue
		for (; i < parent.getNumberOfChildren(); i++) {
			PCGOrNode child = parent.getChild(i);
			if (child.isBuiltInPredicateType(BuiltInPredicateType.BINARY)) {
				for (CompilerTypeBase argument : child.getPredicate().getArguments()) {
					if (argument.isFunctor() && fsValueUsedForArithmetic(fsArgument, (CompilerFunctor)argument))
						return true;
				}
			}
		}
		
		return false;
	}
	
	private static boolean fsValueUsedForArithmetic(Variable fsArgument, CompilerFunctor equation) {
		boolean status = false;
		for (CompilerTypeBase argument : equation.getArguments()) {
			if (argument.isVariable() && argument.equals(fsArgument))
				return (ArithmeticOperation.getArithmeticOperation(equation.getFunctorName()) != ArithmeticOperation.NONE);
			else if (argument.isFunctor()) {
				status = fsValueUsedForArithmetic(fsArgument, (CompilerFunctor)argument);
				if (status)
					break;
			}
		}
		
		return status;
	}
	*/
	private static boolean fsValueUsedOnlyForComparison(CompilerVariable fsArgument, PCGOrNode orNode, PCGAndNode parent) {
		int i;
		for (i = 0; i < parent.getNumberOfChildren(); i++)
			if (parent.getChild(i) == orNode)
				break;
				
		boolean status = false;

		// first find the fsvalue is used in comparison
		for (; i < parent.getNumberOfChildren(); i++) {
			PCGOrNode child = parent.getChild(i);
			if (child.isBuiltInPredicateType(BuiltInPredicateType.BINARY)) {
				ComparisonOperation co = ComparisonOperation.getOperation(child.getPredicateName());
				if ((co != ComparisonOperation.NONE) && (co != ComparisonOperation.EQUALITY) && (co != ComparisonOperation.INEQUALITY)) {
					CompilerVariableList variables = new CompilerVariableList();
					Utilities.getVariables(child.getArguments(), variables);
					if (variables.contains(fsArgument)) {
						status = true;
						i++;
						break;
					}
				}
			}
		}
		
		// if first check successful, check if another predicate uses it - which would invalidate the fsvalue
		if (status) {
			for (; i < parent.getNumberOfChildren(); i++) {
				PCGOrNode child = parent.getChild(i);
				CompilerVariableList variables = new CompilerVariableList();
				Utilities.getVariables(child.getArguments(), variables);
				if (variables.contains(fsArgument)) {
					status = false;
					break;
				}
			}
		}
		
		return status;
	}
	
	private static void pruneUnneededPredicates(List<PCGOrNode> predicates, CompilerVariableList freeVariables) {
		// we will go through the predicates and account for binding of all variables via left-right analysis
		// once all variables are accounted for, we remove any predicates we haven't examined yet
		CompilerVariableList predicateVariables = new CompilerVariableList();
		int i;
		for (i = 0; i < predicates.size(); i++) {
			Utilities.getVariables(predicates.get(i).getArguments(), predicateVariables);
			for (int j = freeVariables.size() - 1; j >= 0; j--) {
				if (predicateVariables.contains(freeVariables.get(j)))
					freeVariables.remove(j);
			}
			
			if (freeVariables.isEmpty()) {
				i++;
				break;
			}
		}

		if (i < predicates.size()) {
			for (int j = predicates.size() - 1; j >= i; j--)
				predicates.remove(j);			
		}
	}
}
