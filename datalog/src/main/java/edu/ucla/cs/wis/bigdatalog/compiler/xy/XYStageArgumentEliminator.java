package edu.ucla.cs.wis.bigdatalog.compiler.xy;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

/* This class removes the XY stage argument and re-attaches it to the predicate as an adornment
 * The predicate is also marked as OLD or NEW depending on the type of rule */
public class XYStageArgumentEliminator {
	private static Logger logger = LoggerFactory.getLogger(XYStageArgumentEliminator.class.getName());

	public static String XY_STAGE_NODE_NAME_PREFIX = "xyStage_";

	private DeALSContext deALSContext;
	protected boolean isInsideXYClique;
	protected CompilerVariableList stageVariableList;

	public XYStageArgumentEliminator(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
		this.isInsideXYClique = false;
		this.stageVariableList = new CompilerVariableList();
	}

	public PCGOrNode eliminateStageVariables(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering eliminateStageVariables");
		
		PCGOrNode retvalOrNode = orNode; 
		
		if (orNode.containsXYClique())
			retvalOrNode = this.eliminateStageVariableInOrNode(orNode, false);
			
		this.deALSContext.logTrace(logger, "Exiting eliminateStageVariables");

		return retvalOrNode;
	}

	private void setXYPredicateType(PCGOrNode orNode, boolean inXRule) {
		this.deALSContext.logTrace(logger, "Entering setXYPredicateType for {} inXRule = {}", orNode.toStringAsPredicate(), inXRule);
				
		// In Y rule, only I+1 is new-xy-predicate
		// In X rule, all nodes are new-xy-predicate
		// Outside clique, only I+1 is new-xy-predicate
		if ((orNode.getArgument(0).getType() == CompilerType.ARITHMETIC_EXPRESSION) || inXRule)
			orNode.getPredicate().setXYPredicateType(XYPredicateType.NEW);
		else
			orNode.getPredicate().setXYPredicateType(XYPredicateType.OLD);		
		
		this.deALSContext.logTrace(logger, "Exiting setXYPredicateType for {}", orNode.toStringAsPredicate());
	}

	private PCGOrNode eliminateStageVariableInOrNode(PCGOrNode orNode, boolean inXRule) {
		this.deALSContext.logTrace(logger, "Entering eliminateStageVariableInOrNode for {} inXRule = {}", orNode.toStringAsPredicate(), inXRule);
		
		CliqueBase clique;
		PCGOrNode retvalOrNode = orNode;

		if (orNode.isRecursive()) {
			clique = orNode.getBaseClique();
			
			//if (clique.getType() != CompilerType.XY_CLIQUE) {
			if (!(clique instanceof XYClique)) {
				this.eliminateStageVariableInClique((Clique) clique);
			} else {
				// set xy predicate type in orNode
				this.setXYPredicateType(orNode, inXRule);

				Utilities.getVariables(orNode.getArguments().get(0), this.stageVariableList);

				if (!this.isInsideXYClique)
					retvalOrNode = this.insertStageVariableRule(orNode);
				else
					this.eliminateFirstArgumentInNode(orNode);

				this.eliminateStageVariableInXYClique((XYClique) clique);
			}
		} else {
			if (orNode.getPredicate().isDerived() || orNode.getPredicate().isBuiltIn()) {
				for (PCGOrNodeChild andNode : orNode.getChildren())
					this.extendEliminateAndNode((PCGAndNode) andNode, inXRule);

				// stage variable won't appear in non-recursive predicate
				// except for builtin predicate ifthenelse and accessRead/Write
				if (orNode.getPredicate().isBuiltIn())
					this.eliminateStageVariableInBuiltInOrNode(orNode);
			}
		}
		
		this.deALSContext.logTrace(logger, "Entering eliminateStageVariableInOrNode with {}", retvalOrNode.toString());
		
		return retvalOrNode;
	}

	private PCGOrNode insertStageVariableRule(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering insertStageVariableRule for {}", orNode.toStringAsPredicate());			
				
		String predicateName = orNode.getPredicateName();
		CompilerVariableList variableList = new CompilerVariableList();

		// 1. create new pcg or node;
		CompilerTypeList orNodeArguments = orNode.getArguments().copy();
		PCGOrNode xyOrNode = new PCGOrNode(predicateName, orNodeArguments);
		xyOrNode.setBindingPattern(orNode.getBindingPattern());

		// APS said on 3/10/2013 - this looks like what the version on cheetah is doing
		if (orNode.getPredicate().isNegative()) {
			xyOrNode.getPredicate().setAsNegative();
			orNode.getPredicate().setAsPositive();
		}

		// 2. create pcg and node;
		CompilerTypeList andNodeArguments = new CompilerTypeList();
		CompilerVariable tmpVariable;
		for (int i = 0; i < orNodeArguments.size(); i++) {
			tmpVariable = new CompilerVariable("tmp." + predicateName + "." + i);
			tmpVariable.setDataType(orNodeArguments.get(i).getDataType());
			andNodeArguments.add(tmpVariable.copy(variableList));
		}

		PCGAndNode xyAndNode = new PCGAndNode(predicateName, andNodeArguments);
		xyAndNode.setBindingPattern(orNode.getBindingPattern());
		xyOrNode.addChild(xyAndNode);

		// 3. create builtin stage variable node;
		// In xy_stage_node, if stage variable is of the form I+1, we set node as
		// new XY predicate and change stage variable to I. This has 2 advantages:
		// 1) functor structure can be deleted whatever;
		// 2) arg type of xy_stage_node is always integer.
		CompilerTypeList stageNodeArguments = new CompilerTypeList();
		stageNodeArguments.addUnique(andNodeArguments.get(0));

		PCGOrNode xyStageOrNode = new PCGOrNode(XY_STAGE_NODE_NAME_PREFIX + predicateName, stageNodeArguments, 
				BuiltInPredicateType.XY_STAGE);
		xyStageOrNode.setBindingPattern(0, orNode.getBinding(0));
		xyStageOrNode.setBaseClique(orNode.getBaseClique());

		xyAndNode.addChild(orNode);
		xyAndNode.addChild(xyStageOrNode);

		// Since we are outside clique, always set xy literal as NEW. In the case there are
		// both "I" literal and "I+1" literal in a single rule, xy-node-type will be adjusted
		// in extendEliminatePcgAndNode()
		orNode.getPredicate().setXYPredicateType(XYPredicateType.NEW);
		
		// set stage node as new if it's I+1 type.
		if ((orNode.getPredicate().getArgument(0).getType() == CompilerType.ARITHMETIC_EXPRESSION) 
				&& !xyOrNode.getPredicate().isNegative())
			xyStageOrNode.getPredicate().setXYPredicateType(XYPredicateType.NEW);
		else
			xyStageOrNode.getPredicate().setXYPredicateType(XYPredicateType.OLD);

		// 4. set or node
		CompilerTypeList newOrNodeArguments = new CompilerTypeList();
		for (int i = 1; i < andNodeArguments.size(); i++)
			newOrNodeArguments.add(andNodeArguments.get(i));

		orNode.resetArgumentsForXYNode(newOrNodeArguments);

		this.deALSContext.logTrace(logger, "Exiting insertStageVariableRule for {}", orNode.toStringAsPredicate());

		return xyOrNode;
	}

	private void eliminateFirstArgumentInNode(PCGNode<?> node) {
		this.deALSContext.logTrace(logger, "Entering eliminateFirstArgumentInNode for {}", node.toString());
		
		CompilerTypeList oldArgumentList = node.getArguments();
		CompilerTypeList newArgumentList = new CompilerTypeList();

		for (int i = 1; i < oldArgumentList.size(); i++)
			newArgumentList.add(oldArgumentList.get(i));

		node.resetArgumentsForXYNode(newArgumentList);
		
		this.deALSContext.logTrace(logger, "Exiting eliminateFirstArgumentInNode for {}", node.toString());
	}

	private void eliminateAllStageArgumentsInNode(PCGNode<?> node) {
		this.deALSContext.logTrace(logger, "Entering eliminateAllStageArgumentsInNode for {}", node.toString());
		
		CompilerTypeList argumentList = node.getArguments();
		CompilerTypeList newArgumentList = new CompilerTypeList();
		CompilerVariableList variableList = new CompilerVariableList();
		CompilerVariableList intersectList = new CompilerVariableList();

		for (CompilerTypeBase arg : argumentList) {
			variableList.clear();
			Utilities.getVariables(arg, variableList);

			intersectList = Utilities.getIntersectingVariables(variableList, this.stageVariableList);

			if (intersectList.size() == 0)
				newArgumentList.add(arg);
		}
		
		node.resetArgumentsForXYNode(newArgumentList);
		
		this.deALSContext.logTrace(logger, "Exiting eliminateAllStageArgumentsInNode for {}", node.toString());
	}

	private void eliminateStageVariableInBuiltInOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering eliminateStageVariableInBuiltInOrNode for {}", orNode.toString());
		
		// Builtin pcgOrNode, unlike xy recursiveOrNode, can have 0, 1, or more stage variables in its argument list. 
		// All arguments that appear in stageVariableList, instead of only the 0th argument, need to be eliminated.

		List<BuiltInPredicateType> builtInPredicateTypes = new ArrayList<>();
		builtInPredicateTypes.add(BuiltInPredicateType.IFTHEN);
		builtInPredicateTypes.add(BuiltInPredicateType.IFTHENELSE);
		builtInPredicateTypes.add(BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE);
		builtInPredicateTypes.add(BuiltInPredicateType.WRITE_USER_DEFINED_AGGREGATE);
		builtInPredicateTypes.add(BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS);
		builtInPredicateTypes.add(BuiltInPredicateType.WRITE_USER_DEFINED_AGGREGATE_FS);
		builtInPredicateTypes.add(BuiltInPredicateType.CHOICE);

		if (orNode.isAnyOf(builtInPredicateTypes)) {
			this.eliminateAllStageArgumentsInNode(orNode);

			for (PCGOrNodeChild orNodeChild : orNode.getChildren())
				this.eliminateAllStageArgumentsInNode((PCGAndNode) orNodeChild);
		}
		
		this.deALSContext.logTrace(logger, "Exiting eliminateStageVariableInBuiltInOrNode for {}", orNode.toString());
	}

	private void extendEliminateAndNode(PCGAndNode andNode, boolean inXRule) {		
		this.deALSContext.logTrace(logger, "Entering extendEliminateAndNode for {} inXRule = {}", andNode.toString(), inXRule);
		
		PCGOrNode newOrNode;
		boolean allLiteralsStageI = true;
		List<Integer> INodeLiteralsPositions = new ArrayList<>();

		// for a rule outside xy clique it may have the following form:
		// 1. <- p(I,X), q(I,X). (all xy literals are of stage I)
		// 2. <- p(I,X), q(I+1,X). (some xy literals are of stage I+1)
		// the literals in the first rule should be regarded as new_xy_node
		// only the "I+1" literal in the second rule should be regarded as new_xy_node

		int count = 0;
		for (PCGOrNode orNode : andNode.getChildren()) {
			newOrNode = this.eliminateStageVariableInOrNode(orNode, inXRule);
			if (newOrNode != orNode) {
				andNode.setChild(count, newOrNode);
				if (newOrNode.getArgument(0).getType() == CompilerType.ARITHMETIC_EXPRESSION)
					allLiteralsStageI = false;
				else
					INodeLiteralsPositions.add(count);
			}
			count++;
		}

		// During insertNode() all xy literals outside xy clique are set as NEW_NODE. 
		// If there are "I+1" nodes, then "I" nodes will be set as OLD_NODE here.
		if (!allLiteralsStageI) {
			PCGOrNode orNode;
			PCGAndNode insertedAndNode;
			for (Integer position : INodeLiteralsPositions) {
				orNode = andNode.getChild(position);
				insertedAndNode = (PCGAndNode) orNode.getChild(0);
				insertedAndNode.getChild(0).getPredicate().setXYPredicateType(XYPredicateType.OLD);
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting extendEliminateAndNode for {} inXRule = {}", andNode.toString(), inXRule);
	}

	private void eliminateStageVariableInClique(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering eliminateStageVariableInClique for clique {}", clique.getCliqueId());
		
		if (!clique.stageVariableEliminated()) {
			clique.setStageVariableEliminatedAs(true);

			for (CliquePredicate cliquePredicate : clique.getCliquePredicates()) {
				for (PCGAndNode andNode : cliquePredicate.getExitRules())
					this.extendEliminateAndNode(andNode, false);

				for (PCGAndNode andNode : cliquePredicate.getRecursiveRules())
					this.extendEliminateAndNode(andNode, false);
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting eliminateStageVariableInClique for clique {}", clique.getCliqueId());
	}

	private void eliminateStageVariableInXYClique(XYClique xyClique) {
		this.deALSContext.logTrace(logger, "Entering eliminateStageVariableInXYClique for clique {}", xyClique.getCliqueId());
		
		if (!xyClique.stageVariableEliminated()) {
			xyClique.setStageVariableEliminatedAs(true);
		
			this.isInsideXYClique = true;
		
			for (XYCliquePredicate xyCliquePredicate : xyClique.getCliquePredicates()) {
				for (PCGAndNode andNode : xyCliquePredicate.getExitRules()) {
					this.eliminateFirstArgumentInNode(andNode);
					this.extendEliminateAndNode(andNode, false);
				}
		
				for (PCGAndNode andNode : xyCliquePredicate.getXRules()) {
					this.eliminateFirstArgumentInNode(andNode);
					this.extendEliminateAndNode(andNode, true);
				}
		
				for (PCGAndNode andNode : xyCliquePredicate.getYRules()) {
					this.eliminateFirstArgumentInNode(andNode);
					this.extendEliminateAndNode(andNode, false);
				}
		
				for (PCGAndNode andNode : xyCliquePredicate.getCopyRules()) {
					this.eliminateFirstArgumentInNode(andNode);
					this.extendEliminateAndNode(andNode, false);
				}
		
				for (PCGAndNode andNode : xyCliquePredicate.getDeleteRules()) {
					this.eliminateFirstArgumentInNode(andNode);
					this.extendEliminateAndNode(andNode, false);
				}
			}
		
			this.isInsideXYClique = false;
		}
		this.deALSContext.logTrace(logger, "Exiting eliminateStageVariableInXYClique for clique {}", xyClique.getCliqueId());
	}
}
