package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Quad;
import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.Aggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSCountNodeType;
import edu.ucla.cs.wis.bigdatalog.compiler.compilation.CompilationResult;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.Rewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeInferrer;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYClique;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYCliquePredicate;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.exception.ProgramGeneratorException;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.Node;
import edu.ucla.cs.wis.bigdatalog.interpreter.PipelinedProgram;
import edu.ucla.cs.wis.bigdatalog.interpreter.ProgramGenerationResult;
import edu.ucla.cs.wis.bigdatalog.interpreter.Utilities;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeStack;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Unifier;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableMappings;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.Expression;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateStoreType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.StratifiedAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.DateFunctionNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.FalseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.FunctorBXXNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.FunctorFBBNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.IfThenElseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.LimitAndNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.SubStringNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.TrueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison.ComparisonAllBoundNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison.ComparisonNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison.EqualityMixedBindingNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.comparison.LikeNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.AppendBBBNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.AppendBBFNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.AppendBFBNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.AppendFBBNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.CardinalityNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.GetNthMemberNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.MemberBBNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.MemberFBNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list.SubsetNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.choice.ChoicePredicate;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.choice.ChoicePredicate.XYStageArgumentType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.choice.ChoiceRule;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.optimizer.Optimizer;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.LinearRecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.NonLinearRecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveLiteral;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.RecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.FSCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.fs.MutualFSCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueBaseNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueRuleType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueType;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.IClique;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.IMutualClique;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.MutualCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.ReadOnlyRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.MaterializedPredicate;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.MaterializedRule;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.BaseRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.SortRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.TopKNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.MutualXYCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.XYCliqueNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.XYRecursiveOrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.XYStageVariableNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.*;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.AggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.SingleMultiNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSAggregateRelationNodeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.fs.FSCountDoubleAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSCountMixedAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSCountSingleKeyValueAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSManyAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSMaxManyFunction;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSSingleAggregateRelationNode;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class ProgramGenerator {
	private static Logger logger = LoggerFactory.getLogger(ProgramGenerator.class.getName());

	private static String UNIQUE_VARIABLE_NAME_PREFIX = "Var_";

	protected 	DeALSContext			deALSContext;
	protected static int 				variableCount;
	protected NodeStack<OrNode> 		globalOrNodeStack;
	protected NodeStack<Clique> 		globalCreatingCliqueStack;
	protected NodeStack<PCGOrNode> 	globalCreatingFSAggregateStack;
	protected VariableList 			backtrackVariableList;
	protected VariableList 			backtrackOrNodeVariableList;
	protected Map<String, Integer>		uniqueRelationNameMapping; 
	
	// Predicatename, int[fsmax,fsmin,fscnt,fsmany]
	protected Map<String, int[]> 	predicateFSAggregateTypeMappings; 
	protected PCGOrNode 			queryFormRootOrNode;
		
	public ProgramGenerator(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
		this.backtrackVariableList = new VariableList();
		this.backtrackOrNodeVariableList = new VariableList(); 
	}

	private PipelinedProgram doGenerateProgram(CompilationResult compilationResult) {
		this.deALSContext.logTrace(logger, "Entering generateProgram for {}", compilationResult.getCompiledProgram());

		this.queryFormRootOrNode = compilationResult.getCompiledProgram();
		
		VariableMappings variableMappings = new VariableMappings();
		VariableList boundVariableList = new VariableList();
		NodeList<IClique> localSharableCliqueList = new NodeList<>();
		NodeList<ChoicePredicate> localChoiceLiteralList = new NodeList<>();		
		OrNode rootOrNode;

		variableCount = 1;
		this.globalOrNodeStack = new NodeStack<>(); // for generating magic exit rule
		this.globalCreatingCliqueStack = new NodeStack<>(); // APS 12/22/2013 to track cliques for MaterializedAggregates
		this.globalCreatingFSAggregateStack = new NodeStack<>(); // APS 12/22/2013 to track cliques for MaterializedAggregates
		this.uniqueRelationNameMapping = new HashMap<>(); // APS 10/6/2014 to track relation names given to nodes

		rootOrNode = this.generateOrNode(this.queryFormRootOrNode, variableMappings, boundVariableList, localSharableCliqueList,
				localChoiceLiteralList, null, null, null, null, null);

		if (rootOrNode != null) {
			NodeList<OrNode> complexObjectEqualityList = new NodeList<>();

			// this will flag an error if there is any free complex objects in orNode
			if (this.extractFreeFunctorFromOrNode(rootOrNode, complexObjectEqualityList)) {
				this.deALSContext.logError(logger, "Query form should not have free complex objects in arguments");
				throw new ProgramGeneratorException("Query form should not have free complex objects in arguments");
			}

			rootOrNode = this.extendOrNode(this.queryFormRootOrNode, rootOrNode, localSharableCliqueList);

			complexObjectEqualityList.clear();
		
		} else {
			String message = "No execution objects generated for " + this.queryFormRootOrNode;
			this.deALSContext.logError(logger, message);
			throw new ProgramGeneratorException(message);
		}

		this.globalOrNodeStack.clear();
		localSharableCliqueList.clear();
		localChoiceLiteralList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateProgram with {}", rootOrNode);

		return new PipelinedProgram(rootOrNode, compilationResult.getProgramRules());
	}

	private OrNode generateOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList,
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> localChoiceLiteralList,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateOrNode for {}", pcgOrNode);

		OrNode orNode = null;

		switch (pcgOrNode.getPredicateType()) {
			case BUILT_IN: {
				orNode = this.generateBuiltInOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList,
						localChoiceLiteralList, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);
				break;
			}
			
			case BASE: {
				orNode = this.generateBaseOrNode(pcgOrNode, variableMappings, boundVariableList);
				break;
			}
	
			case DERIVED: {
				if (pcgOrNode.isRecursive())
					orNode = this.generateRecursiveOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList);
				else
					orNode = this.createNonRecursiveOrNode(pcgOrNode, variableMappings, boundVariableList);				
				break;
			}			
		}

		if (orNode != null) {
			// Propagating new/old xy literal mark from pcg node
			orNode.setXYPredicateType(pcgOrNode.getPredicate().getXYPredicateType());
			
			//Negation is allowed within XY clique. Generate extra node for
			//negation. Non-recursive negation node is generated in generateOrNode().			
			if (pcgOrNode.getPredicate().isNegative())
				orNode = new NegationOrNode(orNode);
		}

		this.deALSContext.logTrace(logger, "Exiting generateOrNode with {}", orNode);

		return orNode;
	}

	private OrNode extendOrNode(PCGOrNode pcgOrNode, OrNode orNode, NodeList<IClique> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering extendOrNode for {}", pcgOrNode);

		OrNode extendedOrNode = null;

		if (orNode != null) {
			switch (pcgOrNode.getPredicateType()) {
				case DERIVED: {
					if (pcgOrNode.isRecursive())
						extendedOrNode = orNode;
					else
						extendedOrNode = this.extendNonRecursiveOrNode(pcgOrNode, orNode, localSharableCliqueList);
					break;
				}	
				
				case BUILT_IN:
				case BASE:
				default: {
					extendedOrNode = orNode;				
					break;
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting extendOrNode with {}", extendedOrNode);

		return extendedOrNode;
	}

	private AndNode generateAndNode(PCGAndNode pcgAndNode, VariableMappings variableMappings, VariableList boundVariableList,
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> externalChoiceLiteralList, OrNode orNode,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList, 
			boolean isAggregateAndNode) {

		this.deALSContext.logTrace(logger, "Entering generateAndNode for {}", pcgAndNode);
				
		NodeList<OrNode> frontEqualityList = new NodeList<>();
		NodeList<OrNode> backEqualityList = new NodeList<>();
		NodeList<ChoicePredicate> localChoiceLiteralList = new NodeList<>();

		// If the choice predicate list is needed externally, do not create a local choice list and instead use 
		// the external list. We do not need to create a choice andNode either as we will be in the context of a choiceNode.
		if (externalChoiceLiteralList != null)
			localChoiceLiteralList = externalChoiceLiteralList;

		AndNode andNode = null;
		if ((pcgAndNode.hasChoiceLiteral() && externalChoiceLiteralList == null)) {
			// We generate special choice andNode here. If the choice list is
			// needed externally, we do not need to create a special choice andNode
			andNode = this.createChoiceAndNode(pcgAndNode, variableMappings, boundVariableList);
		} else {
			andNode = this.createAndNode(pcgAndNode, variableMappings, boundVariableList);
		}

		if ((andNode != null)
				&& this.unifyAndOrNodes(andNode, orNode, frontEqualityList, backEqualityList)) {
			andNode = this.generateAndNodeChildren(andNode, pcgAndNode, variableMappings, boundVariableList, localSharableCliqueList,
					localChoiceLiteralList, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
					localRecursiveLiteralList);

			if (andNode != null) {
				this.addEqualityOrNodes(andNode, frontEqualityList, backEqualityList);
				if (andNode instanceof ChoiceRule) {
					// Thus, if there is any choice literal, we add it to the special andNode.
					// Put all choice literals in local list into choice andNode
					for (ChoicePredicate literal : localChoiceLiteralList)
						((ChoiceRule) andNode).addChoicePredicate(literal);
				}
			}
		} else {
			andNode = null;
		}

		frontEqualityList.clear();
		backEqualityList.clear();

		if (andNode != null) {
			this.expandSugaredArguments(andNode);

			BacktrackType backtrackType = BacktrackType.ALL_BACKTRACK;

			if (pcgAndNode.noBacktrack() || andNode.containXYNode())
				backtrackType = BacktrackType.NO_BACKTRACK;

			this.generateBacktrackMap(andNode, backtrackType, isAggregateAndNode);
		}

		// Only if the the choice list is not needed externally that we
		// create the local choice list. So, we clean up the local choice list
		if (externalChoiceLiteralList == null)
			localChoiceLiteralList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateAndNode with status = {}", andNode);

		return andNode;
	}
	
	private AndNode createChoiceAndNode(PCGAndNode pcgAndNode, VariableMappings variableMappings, VariableList boundVariableList) {
		String predicateName = ProgramGenerator.generateUniquePredicateName(pcgAndNode.getPredicateName(), pcgAndNode.getBindingPattern());
		NodeArguments arguments = generateArguments(pcgAndNode.getArguments(), variableMappings);
		Binding binding = pcgAndNode.getExecutionBindingPattern().copy();

		Utilities.getBoundVariables(arguments, binding, boundVariableList);

		return new ChoiceRule(predicateName, arguments, binding);
	}

	private AndNode createAndNode(PCGAndNode pcgAndNode, VariableMappings variableMappings, VariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering createCommonAndNode for {}", pcgAndNode);

		AndNode andNode;
		Binding binding;

		// APS changed on 3/9/2013 to include xy stage binding
		String predicateName = ProgramGenerator.generateUniquePredicateName(pcgAndNode.getPredicateName(), 
				pcgAndNode.getBindingPattern(), pcgAndNode.getXYStageVariableBinding());
		NodeArguments arguments = generateArguments(pcgAndNode.getArguments(), variableMappings);

		// APS changed on 3/9/2013 to include xy stage binding
		if (pcgAndNode.hasXYStageVariableBinding()) {
			Binding executionBinding = pcgAndNode.getExecutionBindingPattern().copy();
			binding = new Binding(executionBinding.getArity() + 1);
			for (int i = 0; i < executionBinding.getArity(); i++)
				binding.setBinding(i, executionBinding.getBinding(i));

			binding.setBinding(binding.getArity() - 1, pcgAndNode.getXYStageVariableBinding());
		} else {
			binding = pcgAndNode.getExecutionBindingPattern().copy();
		}

		Utilities.getBoundVariables(arguments, binding, boundVariableList);

		andNode = this.createLimitNode(pcgAndNode, variableMappings, predicateName, arguments, binding);
		if (andNode == null)
			andNode = new AndNode(predicateName, arguments, binding);

		this.deALSContext.logTrace(logger, "Exiting createCommonAndNode with {}", andNode);

		return andNode;
	}	
	
	private AndNode createLimitNode(PCGAndNode pcgAndNode, VariableMappings variableMappings, String predicateName, 
			NodeArguments arguments, Binding binding) {
		AndNode andNode = null;
		PCGOrNode limitNode = null;
		
		for (int i = 0; i < pcgAndNode.getNumberOfChildren(); i++) {
			if ((pcgAndNode.getChild(i).getPredicate() instanceof BuiltInPredicate) 
					&& pcgAndNode.getChild(i).getPredicateName().equals(BuiltInPredicate.LIMIT_PREDICATE_NAME)) {
				limitNode = pcgAndNode.getChild(i);
				pcgAndNode.removeChild(i);
				break;
			}			
		}

		if (limitNode != null) {
			// if we have a limit node, and the limit argument is a variable in the head, since there will not be a child
			// to bind the variable in the body (we removed the limit node above), we must mark the variable's binding as bound 
			Argument limitArgument = Utilities.convertToArgument(limitNode.getArgument(0), variableMappings, this.deALSContext.getDatabase().getTypeManager());
			
			boolean found = false;
			for (int i = 0; i < pcgAndNode.getNumberOfChildren() && !found; i++) {
				if (pcgAndNode.getChild(i).getArguments().contains(limitNode.getArgument(0)))
					found = true;				
			}
			
			for (int i = 0; i < arguments.size() && !found; i++) {
				if (limitArgument == arguments.get(i)) {
					binding.setBinding(i, BindingType.BOUND);
					found = true;
				}
			}
			
			andNode = new LimitAndNode(predicateName, arguments, binding, limitArgument);		
		}
		
		return andNode;
	}
	
	private AndNode generateAndNodeChildren(AndNode andNode, PCGAndNode pcgAndNode, VariableMappings variableMappings,
			VariableList boundVariableList, NodeList<IClique> localSharableCliqueList,
			NodeList<ChoicePredicate> localChoiceLiteralList, CliqueBase clique, NodeStack<IClique> localCliqueStack,
			NodeList<IMutualClique> localSharableMutualCliqueList, NodeList<IMutualClique> localMutualCliqueList,
			NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateAndNodeChildren for {}", pcgAndNode);

		OrNode orNode;
		NodeList<OrNode> complexObjectEqualityList = new NodeList<>();
		OrNode originalOrNode = null; // APS 4/1/2014 to hold original node in
		// case we need to swap it out

		for (PCGOrNode pcgOrNode : pcgAndNode.getChildren()) {
			if (pcgOrNode.isRecursiveLiteral(clique)) {
				// APS changed 3/14/2013 as part of fs aggregates
				if (clique instanceof XYClique)
					orNode = this.generateXYCliqueOrNode(pcgOrNode, clique, variableMappings, boundVariableList, localCliqueStack,
							localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
				else
					orNode = this.generateCliqueOrNode(pcgOrNode, (Clique) clique, variableMappings, boundVariableList, localCliqueStack, 
							localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
			} else {
				orNode = this.generateOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList, localChoiceLiteralList, 
						clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
			}

			if (orNode != null) {
				// if (orNode.getType() == NodeType.XY_STAGE_VARIABLE_NODE)
				if (orNode instanceof XYStageVariableNode)
					((XYStageVariableNode) orNode).setXYNode(andNode);

				// perform free complex objects extraction, but don't extract on read access node -- HW
				// APS 3/25/2013 @DATALOGFS - added check for read fs pred
				if (!((pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE 
						|| pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS)
						|| pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.AGGREGATE_FS)
						&& this.extractFreeFunctorFromOrNode(orNode, complexObjectEqualityList)) {
					// this will update the bound variable list and add the orNode to andNode
					this.addOrNodesToAndNode(orNode, complexObjectEqualityList, andNode, boundVariableList);
				} else {
					Utilities.getVariables(orNode.getArguments(), boundVariableList);
					andNode.addChild(orNode);
					
					if (pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.AGGREGATE) {
						NodeList<OrNode> equalityPredicateList = new NodeList<>();
 					
						if (this.extractConstantFiltersFromAggregateNode(orNode, equalityPredicateList)) {
							for (OrNode equalityNode : equalityPredicateList) {
								Utilities.getVariables(equalityNode.getArguments(), boundVariableList);
								andNode.addChild(equalityNode);
							}
						}
					}
				}

				complexObjectEqualityList.clear();
				originalOrNode = orNode;
				orNode = this.extendOrNode(pcgOrNode, orNode, localSharableCliqueList);
			}

			if (orNode != null) {
				if (orNode != originalOrNode) {
					for (int i = 0; i < andNode.getNumberOfChildren(); i++) {
						if (andNode.getChild(i) == originalOrNode) {
							andNode.replaceChild(i, orNode);
							break;
						}
					}
				}
			} else {
				andNode = null;
				break;
			}
		}

		complexObjectEqualityList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateAndNodeChildren with {}", andNode);

		return andNode;
	}

	private OrNode generateBaseOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering generateBaseOrNode for {}", pcgOrNode);

		OrNode orNode = null;

		if (pcgOrNode.getPredicate().isBase()) {
			BasePredicate basePredicate = pcgOrNode.getBasePredicate();

			if (basePredicate != null) {
				String predicateName = pcgOrNode.getPredicateName();
				Binding binding = pcgOrNode.getExecutionBindingPattern().copy();
				NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
				VariableList freeVariableList = new VariableList();

				Utilities.getVariables(arguments, freeVariableList);
				Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

				orNode = new BaseRelationNode(predicateName, arguments, binding, freeVariableList);
			} else {
				this.deALSContext.logError(logger, "Missing schema relation in base pcg or node");
				throw new ProgramGeneratorException("Missing schema relation in base pcg or node");
			}
		} else {
			this.deALSContext.logError(logger, "Can not generate base or temporary base relation");
			throw new ProgramGeneratorException("Can not generate base or temporary base relation");
		}

		this.deALSContext.logTrace(logger, "Exiting generateBaseOrNode with {}", orNode);

		return orNode;
	}

	private OrNode generateBuiltInOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList,
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> localChoiceLiteralList,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateBuiltInOrNode for {}", pcgOrNode);

		OrNode orNode = null;

		switch (pcgOrNode.getBuiltInPredicateType()) {
			case TRUE:
				orNode = new TrueNode();
				break;
				
			case FALSE:
				orNode = new FalseNode();
				break;
	
			case BINARY:
				orNode = this.generateBinaryOrNode(pcgOrNode, variableMappings, boundVariableList);
				break;
	
			case CHOICE: {
				Binding binding = pcgOrNode.getExecutionBindingPattern().copy();
				NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
	
				orNode = new ChoicePredicate(pcgOrNode.getPredicateName(), arguments, binding);
				// APS 6/12/2015 - HACK 
				// xy stage variable removal removes the 1st argument from choice, this way we know what it was
				if (pcgOrNode.originalArguments != null) {
					// if we reach this far, we have a removed stage argument
					CompilerTypeList originalArguments = pcgOrNode.originalArguments;
					CompilerFunctor func = (CompilerFunctor)originalArguments.get(0);
					if (func.getArgument(0).isFunctor())
						((ChoicePredicate)orNode).setXYStageArgumentType(XYStageArgumentType.NEW);
					else 
						((ChoicePredicate)orNode).setXYStageArgumentType(XYStageArgumentType.OLD);
				}

				localChoiceLiteralList.add((ChoicePredicate) orNode);
				break;
			}
			case IFTHEN:
			case IFTHENELSE:
				orNode = this.generateIfThenElseOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList,
						localChoiceLiteralList, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);			
				break;
			case GENERIC:
				orNode = this.generateBuiltInGenericOrNode(pcgOrNode, variableMappings, boundVariableList);
				break;			
			case AGGREGATE:
				orNode = this.generateAggregateOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList,
						null, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);
				break;
			case READ_USER_DEFINED_AGGREGATE:
			case WRITE_USER_DEFINED_AGGREGATE:
				orNode = this.generateUDAFrameworkAggregateOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList,
						null, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);
	
				break;
			case READ_USER_DEFINED_AGGREGATE_FS:
			case WRITE_USER_DEFINED_AGGREGATE_FS:
				orNode = this.generateUDAFrameworkFSAggregateOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList,
						null, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);
				break;
			case AGGREGATE_FS:
				orNode = this.generateFSAggregateOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList,
						localChoiceLiteralList, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);
				break;
			case SINGLE_MULTI_USER_DEFINED_AGGREGATE: {
				String predicateName = pcgOrNode.getPredicateName();
				Binding binding = pcgOrNode.getExecutionBindingPattern().copy();
				NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
				VariableList freeVariableList = new VariableList();
	
				Utilities.getVariables(arguments, freeVariableList);
				Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);
	
				orNode = new SingleMultiNode(predicateName, arguments, binding, freeVariableList);
				break;
			}			
			case XY_STAGE: {
				Binding binding = pcgOrNode.getExecutionBindingPattern().copy();
				NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
				String predicateName = pcgOrNode.getPredicateName();
				VariableList freeVariableList = new VariableList();
	
				Utilities.getVariables(arguments, freeVariableList);
				Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);
	
				orNode = new XYStageVariableNode(predicateName, arguments, binding, freeVariableList);
				// new(I) is actually simplified version of (I+1)
				orNode.setXYPredicateType(pcgOrNode.getPredicate().getXYPredicateType());
				break;
			}			
			case FS_MAX_MANY: {
				Binding binding = pcgOrNode.getExecutionBindingPattern().copy();
				NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
				VariableList freeVariableList = new VariableList();
				Utilities.getVariables(arguments, freeVariableList);
				Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);
	
				orNode = new FSMaxManyFunction(arguments, binding, freeVariableList);
				break;
			}
			
			case SORT: {
				String predicateName = pcgOrNode.getPredicateName();				
				Binding binding = pcgOrNode.getExecutionBindingPattern().copy();
				
				NodeArguments arguments = new NodeArguments(pcgOrNode.getArity() - 1);
				for (int i = 0; i < pcgOrNode.getArity() - 1; i++)
					arguments.set(i,  Utilities.convertToArgument(pcgOrNode.getArgument(i), variableMappings, 
							this.deALSContext.getDatabase().getTypeManager()));

				VariableList freeVariableList = new VariableList();
				InterpreterList sortConditions 
					= Utilities.convertToInterpreterList((CompilerList) pcgOrNode.getArgument(pcgOrNode.getArity() - 1)
						, variableMappings, this.deALSContext.getDatabase().getTypeManager());
				
				arguments.add(sortConditions);

				orNode = new SortRelationNode(predicateName, arguments, binding, freeVariableList);
				AndNode andNode;
				for (PCGOrNodeChild pcgAndNode : pcgOrNode.getChildren()) {
					if ((andNode = this.generateAndNode((PCGAndNode)pcgAndNode, new VariableMappings(), new VariableList(), 
							localSharableCliqueList, null, orNode, null, null, null, null, null, false)) != null)
						orNode.addChild(andNode);
				}
				break;
			}
			
			case TOPK: {
				String predicateName = pcgOrNode.getPredicateName();				
				Binding binding = pcgOrNode.getExecutionBindingPattern().copy();
				
				NodeArguments arguments = new NodeArguments(pcgOrNode.getArity() - 2);
				for (int i = 0; i < pcgOrNode.getArity() - 2; i++)
					arguments.set(i,  Utilities.convertToArgument(pcgOrNode.getArgument(i), variableMappings, 
							this.deALSContext.getDatabase().getTypeManager()));

				VariableList freeVariableList = new VariableList();
				InterpreterList sortConditions 
					= Utilities.convertToInterpreterList((CompilerList) pcgOrNode.getArgument(pcgOrNode.getArity() - 2)
						, variableMappings, this.deALSContext.getDatabase().getTypeManager());
				
				//arguments.add(sortConditions);
				
				//arguments.add(Utilities.convertToArgument(pcgOrNode.getArgument(pcgOrNode.getArity()-1), variableMappings, this.deALSContext.getDatabase().getTypeManager()));

				orNode = new TopKNode(predicateName, arguments, binding, freeVariableList);
				
				Argument limitArgument = Utilities.convertToArgument(pcgOrNode.getArgument(pcgOrNode.getArity()-1), 
						variableMappings, this.deALSContext.getDatabase().getTypeManager());
				((TopKNode)orNode).setConditions(sortConditions, limitArgument);								
				
				AndNode andNode;
				for (PCGOrNodeChild pcgAndNode : pcgOrNode.getChildren()) {
					if ((andNode = this.generateAndNode((PCGAndNode)pcgAndNode, new VariableMappings(), new VariableList(), 
							localSharableCliqueList, null, orNode, null, null, null, null, null, false)) != null)
						orNode.addChild(andNode);
				}
				break;
			}
			case SINGLE:
			case MULTI: 
				this.deALSContext.logError(logger, "We can not generate " + pcgOrNode.getBuiltInPredicateType().name() + " built-in predicates.");
				throw new ProgramGeneratorException("We can not generate " + pcgOrNode.getBuiltInPredicateType().name() + " built-in predicates.");	
			case UNKNOWN:
			default: {
				this.deALSContext.logError(logger, "Unknown built-in type in program generator");
				throw new ProgramGeneratorException("Unknown built-in type in program generator");
			}
		}

		this.deALSContext.logTrace(logger, "Exiting generateBuiltInOrNode with {}", orNode);

		return orNode;
	}

	private OrNode generateIfThenElseOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList,
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> localChoiceLiteralList,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateIfThenElseOrNode for {}", pcgOrNode);

		int numberOfChildren = pcgOrNode.getNumberOfChildren();
		OrNode orNode = new IfThenElseNode();
		AndNode[] iteAndNodes = new AndNode[numberOfChildren];
		VariableList[] iteBoundVariableLists = new VariableList[numberOfChildren];
		boolean intersectsBoundVariableList = false;
		
		if (numberOfChildren > 2) {		
			Quad<Boolean, VariableList, VariableList, VariableList> retvalQuad = Utilities.assignIfThenElseBoundVariableList(pcgOrNode,
						boundVariableList, boundVariableList.copyList(), boundVariableList.copyList(), boundVariableList.copyList());
			
			intersectsBoundVariableList = retvalQuad.getFirst();
			iteBoundVariableLists[0] = retvalQuad.getSecond();
			iteBoundVariableLists[1] = retvalQuad.getThird();
			iteBoundVariableLists[2] = retvalQuad.getFourth();			
		} else {
			Pair<VariableList, VariableList> retvalPair = Utilities.assignIfThenBoundVariableList(pcgOrNode, boundVariableList,
				boundVariableList.copyList(), boundVariableList.copyList());
			
			iteBoundVariableLists[0] = retvalPair.getFirst();
			iteBoundVariableLists[1] = retvalPair.getSecond();
		}

		for (int i = 0; i < numberOfChildren; i++) {
			iteAndNodes[i] = this.generateAndNode((PCGAndNode) pcgOrNode.getChild(i), variableMappings, iteBoundVariableLists[i],
					localSharableCliqueList, localChoiceLiteralList, null, clique, localCliqueStack, localSharableMutualCliqueList,
					localMutualCliqueList, localRecursiveLiteralList, false);
		}

		if (intersectsBoundVariableList) {
			boundVariableList.clear();
			Utilities.getIntersectingVariables(iteBoundVariableLists[1], iteBoundVariableLists[2], boundVariableList);
		}

		for (int i = 0; i < numberOfChildren; i++) {
			if (iteAndNodes[i] == null) {
				this.deALSContext.logError(logger, "Failed to generate ifthenelse literals");
				throw new ProgramGeneratorException("Failed to generate ifthenelse literals");
			}

			orNode.addChild(iteAndNodes[i]);
		}

		this.deALSContext.logTrace(logger, "Exiting generateIfThenElseOrNode with {}", orNode);

		return orNode;
	}
	
	private OrNode generateUDAFrameworkAggregateOrNode(PCGOrNode pcgOrNode,  VariableMappings variableMappings, VariableList boundVariableList,	
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> localChoiceLiteralList,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateUDAFrameworkAggregateOrNode for {}", pcgOrNode);

		OrNode 			orNode 					= null;
		PCGAndNode 		datasourcePCGAndNode 	= null;
		AndNode 		datasourceAndNode;
		VariableList 	freeVariableList 		= new VariableList();
		String			predicateName 			= pcgOrNode.getPredicateName();
		Binding 		binding;

		if (pcgOrNode.hasXYStageVariableBinding()) {
			Binding executionBinding = pcgOrNode.getExecutionBindingPattern().copy();
			binding = new Binding(executionBinding.getArity() + 1);
			for (int i = 0; i < executionBinding.getArity(); i++)
				binding.setBinding(i, executionBinding.getBinding(i));

			binding.setBinding(binding.getArity() - 1, pcgOrNode.getXYStageVariableBinding());
		} else {
			binding = pcgOrNode.getExecutionBindingPattern().copy();
		}

		NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		boolean isRead = (pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE);
		AggregateStoreType aggregateStoreType = 
				AggregateStoreType.getAggregateStoreType(this.deALSContext.getConfiguration().getProperty("deals.database.tuplestores.aggregate.type"));
		orNode = new AggregateRelationNode(predicateName, arguments, binding, freeVariableList, isRead, aggregateStoreType);

		if (pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE) {
			// APS added 3/18/2013 - use same binding pattern as pcgOrNode to find clique predicate 
			// since we're looking for the child andNode for the aggregate table orNode
			if (clique instanceof XYClique) {
				Pair<XYCliquePredicate, Integer> retvalPair = ((XYClique) clique).getCliquePredicate(predicateName, arguments.size(),
								pcgOrNode.getBindingPattern());
				if (retvalPair.getFirst() != null)
					datasourcePCGAndNode = retvalPair.getFirst().getYRule(0);
			}

			// if it wasn't recursive, we just get the 1st child
			if (datasourcePCGAndNode == null)
				datasourcePCGAndNode = (PCGAndNode) pcgOrNode.getChild(0);

			// read aggregate, we need to handle datasourceAndNode
			VariableList boundVariableList1 = boundVariableList.copyList();
			datasourceAndNode = this.generateAndNode(datasourcePCGAndNode, variableMappings, boundVariableList1, 
					localSharableCliqueList, localChoiceLiteralList, null, clique, localCliqueStack, localSharableMutualCliqueList, 
					localMutualCliqueList, localRecursiveLiteralList, true);

			if (datasourceAndNode != null) {
				orNode.addChild(datasourceAndNode);
			} else {
				this.deALSContext.logError(logger, "Failed to generate read aggregate literals");
				throw new ProgramGeneratorException("Failed to generate read aggregate literals");
			}
		}


		this.deALSContext.logTrace(logger, "Exiting generateUDAFrameworkAggregateOrNode with {}", orNode);

		return orNode;
	}

	private OrNode generateAggregateOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList,
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> localChoiceLiteralList,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		VariableList freeVariableList = new VariableList();
		String predicateName = pcgOrNode.getPredicateName();
		Binding binding;

		if (pcgOrNode.hasXYStageVariableBinding()) {
			Binding executionBinding = pcgOrNode.getExecutionBindingPattern().copy();
			binding = new Binding(executionBinding.getArity() + 1);
			for (int i = 0; i < executionBinding.getArity(); i++)
				binding.setBinding(i, executionBinding.getBinding(i));

			binding.setBinding(binding.getArity() - 1, pcgOrNode.getXYStageVariableBinding());
		} else {
			binding = pcgOrNode.getExecutionBindingPattern().copy();
		}

		NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		int numberOfAggregates = 0;
		for (int i = 0; i < pcgOrNode.getArity(); i++) {
			if (pcgOrNode.getArgument(i).isAnyAggregate())
				numberOfAggregates++;
		}

		AggregateInfo[] aggregateInfos = new AggregateInfo[numberOfAggregates];
		numberOfAggregates = 0;
		for (int i = 0; i < pcgOrNode.getArity(); i++) {
			if (pcgOrNode.getArgument(i).isAnyAggregate()) {
				Aggregate aggr = (Aggregate) pcgOrNode.getArgument(i);
				Variable var = variableMappings.getVariableByName(AggregateRewriter.AGGR_VALUE_PREFIX + ++numberOfAggregates);
				// must add to free variable list, so aggregate relation node can free it easily
				if (pcgOrNode.getBinding(i) == BindingType.FREE)
					freeVariableList.add(var);
				arguments.set(i, var);
				
				if (aggr instanceof BuiltInAggregate) {
					BuiltInAggregate bia = (BuiltInAggregate) aggr;
					aggregateInfos[numberOfAggregates - 1] = new AggregateInfo(bia.getBuiltInAggregateType(), bia.getReturnDataType());
					//if (aggr.getAggregateTerm().isVariable())
					//	aggregateInfos[numberOfAggregates - 1].sourceArgument = variableMappings.getVariable((CompilerVariable)aggr.getAggregateTerm());
					//else if (aggr.getAggregateTerm().isConstant())
					aggregateInfos[numberOfAggregates - 1].sourceArgument = Utilities.convertToArgument(aggr.getAggregateTerm(), 
							variableMappings, this.deALSContext.getDatabase().getTypeManager());
				}
			}
		}
		AggregateStoreType aggregateStoreType = 
				AggregateStoreType.getAggregateStoreType(this.deALSContext.getConfiguration().getProperty("deals.database.tuplestores.aggregate.type"));
		OrNode orNode = new StratifiedAggregateRelationNode(predicateName, arguments, binding, freeVariableList, aggregateInfos, aggregateStoreType);		
		PCGAndNode datasourcePCGAndNode = null;

		if (clique instanceof XYClique) {
			// APS added 3/18/2013 - use same binding as pcgOrNode to find
			// clique predicate since we're looking for the child and node for the aggregate table or node
			Pair<XYCliquePredicate, Integer> retvalPair = ((XYClique) clique).getCliquePredicate(predicateName, arguments.size(), 
																									pcgOrNode.getBindingPattern());
			if (retvalPair.getFirst() != null)
				datasourcePCGAndNode = retvalPair.getFirst().getYRule(0);
		}

		// if it wasn't recursive, we just get the 1st child
		if (datasourcePCGAndNode == null)
			datasourcePCGAndNode = (PCGAndNode) pcgOrNode.getChild(0);

		VariableList boundVariableList1 = boundVariableList.copyList();
		AndNode datasourceAndNode = this.generateAndNode(datasourcePCGAndNode, variableMappings, boundVariableList1,
				localSharableCliqueList, localChoiceLiteralList, null, clique, localCliqueStack, 
				localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList, true);

		if (datasourceAndNode != null) {
			orNode.addChild(datasourceAndNode);
		} else {
			this.deALSContext.logError(logger, "Failed to generate read aggregate literals");
			throw new ProgramGeneratorException("Failed to generate read aggregate literals");
		}

		this.deALSContext.logTrace(logger, "Exiting generateAggregateOrNode with {}", orNode);

		return orNode;
	}

	private OrNode generateUDAFrameworkFSAggregateOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList,
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> localChoiceLiteralList,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateUDAFrameworkFSAggregateOrNode for {}", pcgOrNode);

		this.globalCreatingFSAggregateStack.push(pcgOrNode);

		FSAggregateRelationNodeBase 	orNode 					= null;
		PCGAndNode 						datasourcePCGAndNode 	= null;
		AndNode 						datasourceAndNode;
		VariableList 					freeVariableList 		= new VariableList();
		String 							predicateName 			= pcgOrNode.getPredicateName();
		Binding 						binding;

		if (this.predicateFSAggregateTypeMappings == null)
			this.gatherFSAggregateMappings();
		
		if (pcgOrNode.hasXYStageVariableBinding()) {
			Binding executionBinding = pcgOrNode.getExecutionBindingPattern().copy();
			binding = new Binding(executionBinding.getArity() + 1);
			for (int i = 0; i < executionBinding.getArity(); i++)
				binding.setBinding(i, executionBinding.getBinding(i));

			binding.setBinding(binding.getArity() - 1, pcgOrNode.getXYStageVariableBinding());
		} else {
			// we ignore the 1st argument (read|write)
			binding = new Binding(pcgOrNode.getExecutionBindingPattern().getArity() - 1);
			for (int i = 1; i < pcgOrNode.getExecutionBindingPattern().getArity(); i++)
				binding.setBinding(i - 1, pcgOrNode.getExecutionBindingPattern().getBinding(i));
		}

		CompilerTypeList args = new CompilerTypeList();
		for (int i = 1; i < pcgOrNode.getArguments().size(); i++)
			args.add(pcgOrNode.getArgument(i));		
		
		NodeArguments arguments = generateArguments(args, variableMappings);

		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		BuiltInPredicateType type = pcgOrNode.getBuiltInPredicateType();
		boolean isReadAggregate = pcgOrNode.getArgument(0).toString().equals("read");
		int numberOfFSAggregates = 0;

		// TODO - this would be better with argument type adornment
		CompilerTypeBase arg;
		for (int i = 0; i < pcgOrNode.getPredicate().getArity(); i++) {
			arg = pcgOrNode.getPredicate().getArgument(i);
			if (arg.isVariable()) {
				CompilerVariable varg = (CompilerVariable) arg;
				if (varg.getVariableName().startsWith(AggregateRewriter.OLD_FS_VALUE_PREFIX))
					numberOfFSAggregates++;
				else if (varg.getVariableName().startsWith(AggregateRewriter.FS_AGGREGATE_VALUE_PREFIX))
					numberOfFSAggregates++;				
			}
		}

		AggregateStoreType aggregateStoreType = AggregateStoreType.getAggregateStoreType(this.deALSContext.getConfiguration().getProperty("deals.database.tuplestores.aggregate.type"));
		
		// if we have multiple fs aggregates, use the "many aggregate" node
		if (numberOfFSAggregates > 1) {
			orNode = new FSManyAggregateRelationNode(predicateName, arguments, binding, freeVariableList, isReadAggregate, numberOfFSAggregates, aggregateStoreType);
		} else {
			if ((pcgOrNode.getPredicate().getFSAggregateType() == FSAggregateType.FSMAX) || 
					(pcgOrNode.getPredicate().getFSAggregateType() == FSAggregateType.FSMIN)) {
				orNode = new FSSingleAggregateRelationNode(predicateName, arguments, binding, freeVariableList, isReadAggregate, aggregateStoreType, pcgOrNode.getPredicate().getFSAggregateType());
			} else {
				if (this.isMixedAggregatePredicate(predicateName)) {
					switch (FSCountNodeType.getNodeType(this.deALSContext.getConfiguration().getProperty("deals.interpreter.fscntconfiguration").toLowerCase())) {
						case DOUBLE:
							orNode = new edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSCountDoubleAggregateRelationNode(predicateName, arguments, binding,
									freeVariableList, isReadAggregate, false, aggregateStoreType);
							break;
						case DOUBLEDELTA:
							orNode = new edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSCountDoubleAggregateRelationNode(predicateName, arguments, binding,
									freeVariableList, isReadAggregate, true, aggregateStoreType);
							break;
						case KEYVALUESTORE:
							orNode = new FSCountSingleKeyValueAggregateRelationNode(predicateName, arguments, binding,
									freeVariableList, isReadAggregate, aggregateStoreType);
							break;
					}
				} else {
					// does not matter what is specified in config file - mixed
					// aggregate case used mixed aggregate node
					orNode = new FSCountMixedAggregateRelationNode(predicateName, arguments, binding,
							freeVariableList, isReadAggregate, aggregateStoreType);
				}
			}
		}

		// APS 2/5/2014 - change to look for local clique - aggregate node needs
		// to pass fscliquenode address of tuple - eager monotonic
		if (localCliqueStack != null && !localCliqueStack.isEmpty()) {
			if (localCliqueStack.get(localCliqueStack.size() - 1) instanceof FSCliqueNode)
				orNode.setClique((FSCliqueNode) localCliqueStack.get(localCliqueStack.size() - 1));
		}

		if (type == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS) {
			// TODO - need to find a way to get the PCGAndNode from the clique to send to next call
			if (clique != null) {
				// use same binding as orNode to find clique predicate since
				// we're looking for the child andnode for the aggregate table orNode
				CliquePredicate cp = ((Clique) clique).getCliquePredicate(predicateName, pcgOrNode.getArity(), pcgOrNode.getBindingPattern());
				if (cp != null)
					datasourcePCGAndNode = cp.getRecursiveRule(0);
			}

			// if it wasn't recursive, we just get the 1st child
			if (datasourcePCGAndNode == null)
				datasourcePCGAndNode = (PCGAndNode) pcgOrNode.getChild(0);

			if (datasourcePCGAndNode.getArgument(0).toString().equals("read")
					|| datasourcePCGAndNode.getArgument(0).toString().equals("write"))
				datasourcePCGAndNode.removeArgument(0);
			
			// read aggregate, we need to handle datasourceAndNode
			VariableList boundVariableList1 = boundVariableList.copyList();
			datasourceAndNode = this.generateAndNode(datasourcePCGAndNode, variableMappings, boundVariableList1, 
					localSharableCliqueList, localChoiceLiteralList, null, clique, localCliqueStack,
					localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList, true);

			if (datasourceAndNode != null) {
				orNode.addChild(datasourceAndNode);
			} else {
				this.deALSContext.logError(logger, "Failed to generate fs read aggregate literals");
				throw new ProgramGeneratorException("Failed to generate fs read aggregate literals");
			}
		}

		this.globalCreatingFSAggregateStack.pop();

		this.deALSContext.logTrace(logger, "Exiting generateUDAFrameworkFSAggregateOrNode with {}", orNode);

		return orNode;
	}

	private OrNode generateFSAggregateOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList,
			NodeList<IClique> localSharableCliqueList, NodeList<ChoicePredicate> localChoiceLiteralList,
			CliqueBase clique, NodeStack<IClique> localCliqueStack, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateFSAggregateOrNode for {}", pcgOrNode);

		this.globalCreatingFSAggregateStack.push(pcgOrNode);

		OrNode 			orNode					= null;
		PCGAndNode 		datasourcePCGAndNode 	= null;
		AndNode 		datasourceAndNode;
		VariableList	freeVariableList 		= new VariableList();
		String			predicateName			= pcgOrNode.getPredicateName();
		Binding 		binding;

		if (this.predicateFSAggregateTypeMappings == null)
			this.gatherFSAggregateMappings();
		
		boolean isMixedAggregatePredicate = this.isMixedAggregatePredicate2(predicateName);

		if (pcgOrNode.hasXYStageVariableBinding()) {
			Binding executionBinding = pcgOrNode.getExecutionBindingPattern().copy();
			binding = new Binding(executionBinding.getArity() + 1);
			for (int i = 0; i < executionBinding.getArity(); i++)
				binding.setBinding(i, executionBinding.getBinding(i));

			binding.setBinding(binding.getArity() - 1, pcgOrNode.getXYStageVariableBinding());
		} else {
			binding = pcgOrNode.getExecutionBindingPattern().copy();	
		}

		NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);

		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);
		
		boolean isMinOrMax = false;
		int numberOfAggregates = 0;
		for (int i = 0 ; i < pcgOrNode.getArity(); i++)
			if (pcgOrNode.getArgument(i).isFSAggregate())
				numberOfAggregates++;
				
		AggregateInfo[] fsAggregateInfos = new AggregateInfo[numberOfAggregates];
		for (int i = 0 ; i < pcgOrNode.getArity(); i++) {
			if (pcgOrNode.getArgument(i).isFSAggregate()) {
				FSAggregate fsa = (FSAggregate)pcgOrNode.getArgument(i);
				// must add to free variable list, so aggregate relation node can free it easily
				Argument aggTerm = Utilities.convertToArgument(fsa.getAggregateTerm(), variableMappings, this.deALSContext.getDatabase().getTypeManager());

				if (pcgOrNode.getBinding(i) == BindingType.FREE)
					Utilities.getVariables(aggTerm, freeVariableList);
				
				arguments.set(i, aggTerm);
				fsAggregateInfos[0] = new AggregateInfo(fsa.getFSAggregateType(), fsa.getReturnDataType());
				fsAggregateInfos[0].sourceArgument = aggTerm;
				if (((fsa.getFSAggregateType() == FSAggregateType.FSMAX) 
						|| (fsa.getFSAggregateType() == FSAggregateType.FSMIN)))
					isMinOrMax = true;
				break;
			}
		}

		FSCliqueNode fsClique = null;
		// get evaluation type from clique - if no clique, use semi-naive
		if (localCliqueStack != null && !localCliqueStack.isEmpty())
			if (localCliqueStack.get(localCliqueStack.size() - 1) instanceof FSCliqueNode)
				fsClique = (FSCliqueNode) localCliqueStack.get(localCliqueStack.size() - 1);			
				
		// TODO - support multiple aggregates in one predicate 
		if (numberOfAggregates > 1)
			throw new InterpreterException("Multi-aggregate predicates not yet supported!");
		
		AggregateStoreType aggregateStoreType = AggregateStoreType.getAggregateStoreType(this.deALSContext.getConfiguration().getProperty("deals.database.tuplestores.aggregate.type"));
		
		if (isMinOrMax) {
			// ssc uses MO only for recursive rule
			orNode = new FSMinMaxAggregateRelationNode(predicateName, arguments, binding, freeVariableList, 
					fsAggregateInfos, aggregateStoreType);
		} else {			
			FSCountNodeType fscntNodeType = FSCountNodeType.getNodeType(this.deALSContext.getConfiguration().getProperty("deals.interpreter.fscntconfiguration").toLowerCase());
						
			if (isMixedAggregatePredicate)
				fscntNodeType = FSCountNodeType.DOUBLEDELTA;

			switch (fscntNodeType) {
				case DOUBLE:
					orNode = new FSCountDoubleAggregateRelationNode(predicateName, arguments, binding, freeVariableList, 
							fsAggregateInfos, aggregateStoreType, false, isMixedAggregatePredicate);
					break;
				case DOUBLEDELTA:
					orNode = new FSCountDoubleAggregateRelationNode(predicateName, arguments, binding, freeVariableList, 
							fsAggregateInfos, aggregateStoreType, true, isMixedAggregatePredicate);
					break;
				case KEYVALUESTORE:
					orNode = new FSCountKeyValueAggregateRelationNode(predicateName, arguments, binding, freeVariableList, 
							fsAggregateInfos, aggregateStoreType);					
					break;
			}
		}
		
		if (fsClique != null)		
			((FSAggregateRelationNode)orNode).setClique(fsClique);

		if (clique != null) {
			// use same binding as orNode to find clique predicate since we're looking for the child andnode for the aggregate table orNode
			CliquePredicate cp = ((Clique)clique).getCliquePredicate(predicateName, pcgOrNode.getArity(), pcgOrNode.getBindingPattern());
			if (cp != null)
				datasourcePCGAndNode = cp.getRecursiveRule(0);
		}

		// if no datasource node set yet, use first child
		if (datasourcePCGAndNode == null)
			datasourcePCGAndNode = (PCGAndNode)pcgOrNode.getChild(0);

		VariableList boundVariableList1 = boundVariableList.copyList();
		datasourceAndNode = this.generateAndNode(datasourcePCGAndNode, variableMappings, boundVariableList1,
					localSharableCliqueList, localChoiceLiteralList, null, clique, localCliqueStack, localSharableMutualCliqueList,
					localMutualCliqueList, localRecursiveLiteralList, true);

		if (datasourceAndNode != null) {
			orNode.addChild(datasourceAndNode);
		} else {
			this.deALSContext.logError(logger, "Failed to generate fs read aggregate literals");
			throw new ProgramGeneratorException("Failed to generate fs read aggregate literals");
		}

		this.globalCreatingFSAggregateStack.pop();

		this.deALSContext.logTrace(logger, "Exiting generateFSAggregateOrNode with {}", orNode);

		return orNode;
	}

	private void incrementPredicateFSAggregateCounters(Predicate predicate) {
		int[] counter = new int[4];
		if (this.predicateFSAggregateTypeMappings.containsKey(predicate.getPredicateName()))
			counter = this.predicateFSAggregateTypeMappings.get(predicate.getPredicateName());

		/*if (DeALSContext.getConfiguration().compareProperty("deals.aggregates.useudaframework", "true")) {
			switch (predicate.getFSAggregateType()) {
			case FSMAX:
				counter[0]++;
				break;
			case FSMIN:
				counter[1]++;
				break;
			case FSCNT:
			case FSSUM:
				counter[2]++;
				break;
			case FSMANY:
				counter[3]++;
				break;
			}
		} else {*/
			for (int i = 0; i < predicate.getArity(); i++) {
				if (predicate.getArgument(i).isFSAggregate()) {
					switch (((FSAggregate)predicate.getArgument(i)).getFSAggregateType()) {
						case FSMAX:
							counter[0]++;
							break;
						case FSMIN:
							counter[1]++;
							break;
						case FSCNT:
						case FSSUM:
							counter[2]++;
							break;
						case FSMANY:
							counter[3]++;
							break;
						}		
				}
			}
		//}		
		this.predicateFSAggregateTypeMappings.put(predicate.getPredicateName(), counter);
	}

	private void gatherFSAggregateMappings() {
		this.predicateFSAggregateTypeMappings = new HashMap<>();
		
		this.doGatherFSAggregateMappings(this.queryFormRootOrNode, new ArrayList<String>());
	}
	
	private void doGatherFSAggregateMappings(PCGOrNode orNode, List<String> countedCliqueIds) {
		this.incrementPredicateFSAggregateCounters(orNode.getPredicate());

		for (PCGOrNodeChild child : orNode.getChildren())
			this.doGatherFSAggregateMappings(child, countedCliqueIds);
	}

	private void doGatherFSAggregateMappings(PCGOrNodeChild orNodeChild, List<String> countedCliqueIds) {
		if (orNodeChild instanceof PCGAndNode) {
			this.incrementPredicateFSAggregateCounters(((PCGAndNode) orNodeChild).getPredicate());

			for (PCGOrNode child : ((PCGAndNode) orNodeChild).getChildren())
				this.doGatherFSAggregateMappings(child, countedCliqueIds);

		} else if (orNodeChild instanceof Clique) {
			Clique clique = (Clique) orNodeChild;
			if (!countedCliqueIds.contains(clique.getCliqueId())) {
				countedCliqueIds.add(clique.getCliqueId());

				for (CliquePredicate cp : clique.getCliquePredicates()) {
					for (PCGAndNode exitRule : cp.getExitRules())
						this.doGatherFSAggregateMappings(exitRule, countedCliqueIds);

					for (PCGAndNode recursiveRule : cp.getRecursiveRules())
						this.doGatherFSAggregateMappings(recursiveRule, countedCliqueIds);
				}
			}
		}
	}

	private boolean isMixedAggregatePredicate(String aggregatePredicateName) {
		// we have to lookup the parent predicate of the aggregate
		// we do this by removing the prefix from the aggregate rewriting and the suffix of '_[rulenumber]'
		if (FSAggregateType.isFSAggregateType(aggregatePredicateName)) {
			String predicateName = aggregatePredicateName.substring(FSAggregateType.getFSAggregateType(aggregatePredicateName).name().length());
			predicateName = predicateName.substring(0, predicateName.lastIndexOf("_"));

			if (this.predicateFSAggregateTypeMappings.containsKey(predicateName)) {
				int[] counters = this.predicateFSAggregateTypeMappings.get(predicateName);
				int count = 0;
				for (int i = 0; i < counters.length; i++)
					if (counters[i] > 0)
						count++;
				
				return (count > 1);
			}
		}
		
		return false;
	}
	
	private boolean isMixedAggregatePredicate2(String aggregatePredicateName) {
		// we have to lookup the parent predicate of the aggregate
		// we do this by removing the prefix from the aggregate rewriting and the suffix of '_[rulenumber]'
		String predicateName = aggregatePredicateName.substring(0, aggregatePredicateName.lastIndexOf("_"));
		
		// combine counters from all predicates starting with 'predicateName'
		int[] counters = new int[4];
		for (Map.Entry<String, int[]> entry : this.predicateFSAggregateTypeMappings.entrySet()) {
			if (entry.getKey().startsWith(predicateName)) {
				for (int i = 0; i < 4; i++)
					counters[i] += entry.getValue()[i];
			}
		}
		
		int count = 0;
		for (int i = 0; i < counters.length; i++)
			if (counters[i] > 0)
				count++;

		return (count > 1);
	}
	
	private OrNode generateBinaryOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, VariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering generateBinaryOrNode for {}", pcgOrNode);

		String 			predicateName 	= pcgOrNode.getPredicateName();
		Binding 		binding 		= pcgOrNode.getExecutionBindingPattern().copy();
		NodeArguments 	arguments 		= generateArguments(pcgOrNode.getArguments(), variableMappings);
		VariableList 	freeVariableList = new VariableList();
		OrNode 			orNode 			= null;

		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		if (arguments.size() == 2) {
			if (predicateName.equals(BuiltInPredicate.EQUALITY_PREDICATE_NAME)) {
				if ((binding.getBinding(0) == BindingType.BOUND)
						&& (binding.getBinding(1) == BindingType.BOUND)) {
					orNode = new ComparisonAllBoundNode(predicateName, arguments, freeVariableList);
				} else if ((binding.getBinding(0) == BindingType.FREE)
						|| (binding.getBinding(1) == BindingType.FREE)) {
					orNode = new EqualityMixedBindingNode(arguments, binding, freeVariableList);
				} else {
					this.deALSContext.logError(logger, "Unsafe equality with free-free binding");
					throw new ProgramGeneratorException("Unsafe equality with free-free binding");
				}
			} else if (predicateName.equals(BuiltInPredicate.LIKE_PREDICATE_NAME)) {
				orNode = new LikeNode(arguments, binding, freeVariableList, true);
			} else if (predicateName.equals(BuiltInPredicate.NOT_LIKE_PREDICATE_NAME)) {
				orNode = new LikeNode(arguments, binding, freeVariableList, false);
			} else {
				orNode = new ComparisonAllBoundNode(predicateName, arguments, freeVariableList);
			}
		} else {
			this.deALSContext.logError(logger, "Binary built-in relation has wrong number of arguments");
			throw new ProgramGeneratorException("Binary built-in relation has wrong number of arguments");
		}

		this.deALSContext.logTrace(logger, "Exiting generateBinaryOrNode with {}", orNode);

		return orNode;
	}

	private OrNode generateBuiltInGenericOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, 
			VariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering generateBuiltInGenericOrNode for {}", pcgOrNode);

		String 			predicateName 		= pcgOrNode.getPredicateName();
		Binding 		binding 			= pcgOrNode.getExecutionBindingPattern().copy();
		int 			arity 				= pcgOrNode.getArity();
		NodeArguments 	arguments 			= generateArguments(pcgOrNode.getArguments(), variableMappings);
		VariableList 	freeVariableList 	= new VariableList();
		OrNode 			orNode 				= null;

		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		if (predicateName.equals(BuiltInPredicate.APPEND_PREDICATE_NAME) && arity == 3) {
			if (binding.getBinding(0) == BindingType.BOUND
					&& binding.getBinding(1) == BindingType.BOUND
					&& binding.getBinding(2) == BindingType.FREE)
				orNode = new AppendBBFNode(arguments, binding, freeVariableList);
			else if (binding.getBinding(0) == BindingType.BOUND
					&& binding.getBinding(1) == BindingType.FREE
					&& binding.getBinding(2) == BindingType.BOUND)
				orNode = new AppendBFBNode(arguments, binding, freeVariableList);
			else if (binding.getBinding(0) == BindingType.FREE
					&& binding.getBinding(1) == BindingType.BOUND
					&& binding.getBinding(2) == BindingType.BOUND)
				orNode = new AppendFBBNode(arguments, binding, freeVariableList);
			else if (binding.getBinding(0) == BindingType.BOUND
					&& binding.getBinding(1) == BindingType.BOUND
					&& binding.getBinding(2) == BindingType.BOUND)
				orNode = new AppendBBBNode(arguments, binding, freeVariableList);
			else
				throw new ProgramGeneratorException("Unsafe binding in append node generation");
		} else if (predicateName.equals(BuiltInPredicate.MEMBER_PREDICATE_NAME) && arity == 2) {
			if (binding.getBinding(0) == BindingType.BOUND
					&& binding.getBinding(1) == BindingType.BOUND)
				orNode = new MemberBBNode(arguments, binding, freeVariableList);
			else if (binding.getBinding(0) == BindingType.FREE
					&& binding.getBinding(1) == BindingType.BOUND)
				orNode = new MemberFBNode(arguments, binding, freeVariableList);
			else
				throw new ProgramGeneratorException("Unsafe binding in member node generation");
		} else if (predicateName.equals(BuiltInPredicate.CARDINALITY_PREDICATE_NAME)
				&& arity == 2) {
			orNode = new CardinalityNode(arguments, binding, freeVariableList);
		} else if (predicateName.equals(BuiltInPredicate.GET_NTH_MEMBER_PREDICATE_NAME)
				&& arity == 3) {
			if (binding.getBinding(0) == BindingType.BOUND
					&& binding.getBinding(1) == BindingType.BOUND)
				orNode = new GetNthMemberNode(arguments, binding, freeVariableList);
			else
				throw new ProgramGeneratorException("Unsafe binding in nth member node generation");
		} else if (predicateName.equals(BuiltInPredicate.FUNCTOR_PREDICATE_NAME) && arity == 3) {
			if (binding.getBinding(0) == BindingType.BOUND) {
				if ((binding.getBinding(1) == BindingType.BOUND && binding.getBinding(2) == BindingType.BOUND)
						|| (binding.getBinding(1) == BindingType.BOUND && binding.getBinding(2) == BindingType.FREE)
						|| (binding.getBinding(1) == BindingType.FREE && binding.getBinding(2) == BindingType.BOUND)
						|| (binding.getBinding(1) == BindingType.FREE && binding.getBinding(2) == BindingType.FREE))
					orNode = new FunctorBXXNode(arguments, binding, freeVariableList);
				else
					throw new ProgramGeneratorException("Unsafe binding in functor predicate node generation");
			} else if (binding.getBinding(0) == BindingType.FREE) {
				if (binding.getBinding(1) == BindingType.BOUND && binding.getBinding(2) == BindingType.BOUND)
					orNode = new FunctorFBBNode(arguments, binding, freeVariableList);
				else
					this.deALSContext.logError(logger, "Unsafe binding in functor predicate node generation");
				throw new ProgramGeneratorException("Unsafe binding in functor predicate node generation");
			} else {
				this.deALSContext.logError(logger, "Unsafe binding in functor predicate node generation");
				throw new ProgramGeneratorException("Unsafe binding in functor predicate node generation");
			}
			// } else if(predicateName.equals(BuiltInPredicate.SORT_PREDICATE_NAME) &&
			// arity == 2) {
			// throw new ProgramGenerationException("cannot generate predicate - unknown built-in predicate");
		} else if (predicateName.equals(BuiltInPredicate.SUBSET_PREDICATE_NAME) && arity == 2) {
			orNode = new SubsetNode(arguments, binding, freeVariableList);
		} else if (predicateName.equals(BuiltInPredicate.GET_DATE_PREDICATE_NAME) && arity == 1) {
			orNode = new DateFunctionNode(predicateName, arguments, binding, freeVariableList);
		} else if (predicateName.equals(BuiltInPredicate.DATE_PART_PREDICATE_NAME) && arity == 3) {
			if ((binding.getBinding(0) == BindingType.BOUND) 
					&& (binding.getBinding(1) == BindingType.BOUND) 
					&& (binding.getBinding(2) == BindingType.FREE)) {
				orNode = new DateFunctionNode(predicateName, arguments, binding, freeVariableList);
			} else {
				this.deALSContext.logError(logger, "Unsafe binding in datepart predicate node generation");
				throw new ProgramGeneratorException("Unsafe binding in datepart predicate node generation");
			}
		} else if (predicateName.equals(BuiltInPredicate.DATE_ADD_PREDICATE_NAME) && arity == 4) {
			if ((binding.getBinding(0) == BindingType.BOUND) 
					&& (binding.getBinding(1) == BindingType.BOUND)
					&& (binding.getBinding(2) == BindingType.BOUND)
					&& (binding.getBinding(3) == BindingType.FREE)) {
				orNode = new DateFunctionNode(predicateName, arguments, binding, freeVariableList);
			} else {
				this.deALSContext.logError(logger, "Unsafe binding in dateadd predicate node generation");
				throw new ProgramGeneratorException("Unsafe binding in dateadd predicate node generation");
			}
		} else if (predicateName.equals(BuiltInPredicate.DATE_DIFF_PREDICATE_NAME) && arity == 4) {
			if ((binding.getBinding(0) == BindingType.BOUND) 
					&& (binding.getBinding(1) == BindingType.BOUND)
					&& (binding.getBinding(2) == BindingType.BOUND)
					&& (binding.getBinding(3) == BindingType.FREE)) {
				orNode = new DateFunctionNode(predicateName, arguments, binding, freeVariableList);
			} else {
				this.deALSContext.logError(logger, "Unsafe binding in datediff predicate node generation");
				throw new ProgramGeneratorException("Unsafe binding in datediff predicate node generation");
			}
		 } else if (predicateName.equals(BuiltInPredicate.SUB_STRING_PREDICATE_NAME) && arity == 4) {
			 if ((binding.getBinding(0) == BindingType.BOUND) 
					 && (binding.getBinding(1) == BindingType.BOUND)
					 && (binding.getBinding(2) == BindingType.BOUND)
					 && (binding.getBinding(3) == BindingType.FREE)) {
				 orNode = new SubStringNode(predicateName, arguments, binding, freeVariableList);
			 } else {
				 logger.error("Unsafe binding in substring predicate node generation");
                 throw new ProgramGeneratorException("Unsafe binding in substring predicate node generation");
            }
		} else {
			this.deALSContext.logError(logger, "Can not generate predicate - unknown built-in predicate");
			throw new ProgramGeneratorException("Can not generate predicate - unknown built-in predicate");
		}

		this.deALSContext.logTrace(logger, "Exiting generateBuiltInGenericOrNode with {}", orNode);

		return orNode;
	}

	// Notes on Backtracking Data Structures:
	// 1. variableList stores a list of variables used in the rule
	// 2. variableRangeMap stores the largest index number of the variable in variableList
	// variableRangeMap has (# of Literals + 1) elements
	// variableRangeMap[0] denotes the bound variables in head predicate
	// 3. variableIndexMap stores the orNode index where the variable gets its binding
	// 4. backtrackMap stores the backtracking point (i.e. or node index) of each or node
	//
	// We also used two member variables, this.backtrackVariableList and this.backtrackOrNodeVariableList
	// They are constructed once when this class is constructed so that they do not need
	// to be constructed every time we invoke the following method.
	private void generateBacktrackMap(AndNode andNode, BacktrackType backtrackType, boolean isAggregateAndNode) {
		this.deALSContext.logTrace(logger, "Entering generateBacktrackMap for backtrackType name = {}", backtrackType.name());

		int backtrackMap[] = new int[andNode.getNumberOfChildren()];
		int headBacktrackIndex = 0;

		switch (backtrackType) {
			case NO_BACKTRACK:
				this.generateNormalBacktrackArray(backtrackMap);
				headBacktrackIndex = andNode.getNumberOfChildren() - 1;
				break;
	
			case ALL_BACKTRACK:
				headBacktrackIndex = this.generateAllBacktrackMap(andNode, backtrackMap);
				break;
	
			case LITERALS_BACKTRACK:
				headBacktrackIndex = this.generateLiteralsBacktrackMap(andNode, backtrackMap);
				break;
	
			case HEAD_BACKTRACK:
				headBacktrackIndex = this.generateHeadBacktrackMap(andNode, backtrackMap);
				break;
		}

		// APS 11/17/2013 - hack for fs aggregates
		if (headBacktrackIndex > 0) {
			if (andNode.getNumberOfChildren() == 2) {
				if ((andNode.getChild(0) instanceof AggregateRelationNode)
						&& (andNode.getChild(1) instanceof IfThenElseNode)) {
					headBacktrackIndex = 0;
					IfThenElseNode iteNode = (IfThenElseNode) andNode.getChild(1);
					for (int i = 0; i < iteNode.getNumberOfChildren(); i++)
						this.removeBacktrackingInformation(iteNode.getChild(i));						
				}
			}
		} else if (isAggregateAndNode 
				&& (andNode.getPredicateName().startsWith(AggregateRewriter.STRATIFIED_AGGREGATE_NODE_NAME_PREFIX))) {
			// stratified aggregates get special attention
			// TODO - add in rules for special attention for fs aggregates too
			if (headBacktrackIndex == -1) {		
				headBacktrackIndex = 0;
			} else if ((headBacktrackIndex == 0) && (andNode.getNumberOfChildren() > 1)) {
				// backtrack analysis fails to recognize child rules 
				// could produce additional more tuples in aggregate (multi-set) situations
				for (int i = andNode.getNumberOfChildren() - 1; i > 0; i--) {
					if (andNode.getChild(i) instanceof ComparisonNode)
						continue;
					
					if (!andNode.getChild(i).getBindingPattern().allBound()) {
						headBacktrackIndex = i;
						break;
					}
				}
			}			
		} 

		andNode.setBacktrackMap(backtrackMap);
		andNode.setRuleBacktrackPoint(headBacktrackIndex);

		if (this.deALSContext.isDebugEnabled()) {
			this.deALSContext.logDebug(logger, "{}", andNode);
			this.deALSContext.logDebug(logger, "\tbacktrackType = {}", backtrackType.name());
			this.deALSContext.logDebug(logger, "\theadBacktrackIndex = {}", headBacktrackIndex);

			for (int i = 0; i < andNode.getNumberOfChildren(); i++)
				this.deALSContext.logDebug(logger, "\tbacktrackMap[" + i + "] = " + backtrackMap[i]);
		}

		this.deALSContext.logTrace(logger, "Exiting generateBacktrackMap for backtrackType name = {}", backtrackType.name());
	}

	private void removeBacktrackingInformation(AndNode andNode) {
		int[] backtrackMap = new int[andNode.getBacktrackMap().length];
		for (int i = 0; i < backtrackMap.length; i++)
			backtrackMap[i] = -1;
		
		andNode.setBacktrackMap(backtrackMap);
		andNode.setRuleBacktrackPoint(-1);

		for (int i = 0; i < andNode.getNumberOfChildren(); i++)
			this.removeBacktrackingInformation(andNode.getChild(i));
	}

	private void removeBacktrackingInformation(OrNode orNode) {
		for (int i = 0; i < orNode.getNumberOfChildren(); i++)
			removeBacktrackingInformation(orNode.getChild(i));
	}

	private int generateAllBacktrackMap(AndNode andNode, int[] backtrackMap) {
		this.deALSContext.logTrace(logger, "Entering generateAllBacktrackMap for {}", andNode);

		this.deALSContext.logDebug(logger, "{}\n", andNode.toStringTree());

		int numberOfLiterals = andNode.getNumberOfChildren();
		int[] variableRangeMap = new int[numberOfLiterals + 1];

		this.backtrackVariableList.clear();

		// STEP 1: create the map for variable range
		this.generateVariableRangeMap(andNode, variableRangeMap);

		int numberOfVariables = this.backtrackVariableList.size();
		int[] variableIndexMap = new int[numberOfVariables];

		// STEP 2: create the map for variable indexes
		this.generateVariableIndexMap(variableRangeMap, variableIndexMap);

		// STEP 3: create the map for backtracking
		this.generateBacktrackPointForLiterals(andNode, variableIndexMap, backtrackMap);

		// STEP 4: get the headBacktrackIndex point
		int headBacktrackIndex = this.generateBacktrackPointForHead(andNode, variableIndexMap);

		this.backtrackVariableList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateAllBacktrackMap with headBacktrackIndex = {}", headBacktrackIndex);

		return headBacktrackIndex;
	}

	private int generateLiteralsBacktrackMap(AndNode andNode, int[] backtrackMap) {
		this.deALSContext.logTrace(logger, "Entering generateLiteralsBacktrackMap for {}", andNode);

		this.deALSContext.logDebug(logger, "{}", andNode.toStringTree());

		int numberOfLiterals = andNode.getNumberOfChildren();
		int[] variableRangeMap = new int[numberOfLiterals + 1];

		this.backtrackVariableList.clear();

		// step 1: we create the map for variableRangeMap
		this.generateVariableRangeMap(andNode, variableRangeMap);

		int numberOfVariables = this.backtrackVariableList.size();
		int[] variableIndexMap = new int[numberOfVariables];

		// step 2: we create the map for variableIndexMap
		this.generateVariableIndexMap(variableRangeMap, variableIndexMap);

		// step 3: we create the map for backtrack array
		this.generateBacktrackPointForLiterals(andNode, variableIndexMap, backtrackMap);

		// step 4: no intelligent backtracking for head
		int headBacktrackIndex = numberOfLiterals - 1;

		this.backtrackVariableList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateLiteralsBacktrackMap with headBacktrackIndex = {}", headBacktrackIndex);

		return headBacktrackIndex;
	}

	private int generateHeadBacktrackMap(AndNode andNode, int[] backtrackMap) {
		this.deALSContext.logTrace(logger, "Entering generateHeadBacktrackMap for {}", andNode);

		this.deALSContext.logDebug(logger, "{}", andNode.toStringTree());

		int numberOfLiterals = andNode.getNumberOfChildren();
		int[] variableRangeMap = new int[numberOfLiterals + 1];

		this.backtrackVariableList.clear();

		// step 1: we create the map for var_range_array
		this.generateVariableRangeMap(andNode, variableRangeMap);

		int numberOfVariables = this.backtrackVariableList.size();
		int[] variableIndexMap = new int[numberOfVariables];

		// step 2: we create the map for variableIndexMap
		this.generateVariableIndexMap(variableRangeMap, variableIndexMap);

		// step 3: we create the map for backtrack array
		this.generateNormalBacktrackArray(backtrackMap);

		// step 4: we create the map for head_backtrack_index
		int headBacktrackIndex = this.generateBacktrackPointForHead(andNode, variableIndexMap);

		this.backtrackVariableList.clear();

		this.deALSContext.logTrace(logger,  "Exiting generateHeadBacktrackMap with headBacktrackIndex = {}", headBacktrackIndex);

		return headBacktrackIndex;
	}

	private void generateVariableRangeMap(AndNode andNode, int[] variableRangeMap) {
		this.deALSContext.logTrace(logger, "Entering generateVariableRangeMap for {}", andNode);

		// For each ornode, we remember the largest index of the variables occurring in each ornode
		// The slot 0 is special, it represents the rule above for those bound variables in head
		// Info for ornode n will be stored in variableRangeMap[n+1]
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(), 
				this.backtrackVariableList);
		variableRangeMap[0] = this.backtrackVariableList.size() - 1;

		for (int i = 0; i < andNode.getNumberOfChildren(); i++) {
			andNode.getChild(i).getVariables(this.backtrackVariableList);
			variableRangeMap[i + 1] = this.backtrackVariableList.size() - 1;
		}

		if (this.deALSContext.isDebugEnabled()) {
			StringBuilder retval = new StringBuilder();
			retval.append("this.backtrackVariableList = ");
			for (int i = 0; i < this.backtrackVariableList.size(); i++) {
				if (i > 0)
					retval.append(" | ");
				retval.append(this.backtrackVariableList.get(i).toString());
			}
			this.deALSContext.logDebug(logger, "{}", retval);
		}

		this.deALSContext.logTrace(logger, "Exiting generateVariableRangeMap");
	}

	private void generateVariableIndexMap(int[] variableRangeMap, int[] variableIndexMap) {
		this.deALSContext.logTrace(logger, "Entering generateVariableIndexMap");

		if (this.deALSContext.isDebugEnabled()) {
			// for (int i = 0; i < numberOfLiterals + 1; i++)
			for (int i = 0; i < variableRangeMap.length; i++)
				this.deALSContext.logDebug(logger, "\tvariableRangeMap[" + i + "] = " + variableRangeMap[i]);
		}

		// For each variable, we find the or node that binds its value
		for (int i = 0; i < variableIndexMap.length; i++) {
			// for (int j = 0; j < numberOfLiterals + 1; j++) {
			for (int j = 0; j < variableRangeMap.length; j++) {
				if (i <= variableRangeMap[j]) {
					// i.e. variable[i] get its binding from or_node[j-1]
					// or_node[-1] means the variables gets its binding from the head
					variableIndexMap[i] = j - 1;
					break;
				}
			}
		}

		if (this.deALSContext.isDebugEnabled()) {
			for (int i = 0; i < variableIndexMap.length; i++)
				this.deALSContext.logDebug(logger, "\tvariableIndexMap[" + i + "] = " + variableIndexMap[i]);
		}

		this.deALSContext.logTrace(logger, "Exiting generateVariableIndexMap");
	}

	private int generateBacktrackPointForHead(AndNode andNode, int[] variableIndexMap) {
		this.deALSContext.logTrace(logger, "Entering generateBacktrackPointForHead for {}", andNode);

		// We need to check only those bound variables in head
		this.backtrackOrNodeVariableList.clear();
		Utilities.getFreeVariables(andNode.getArguments(), andNode.getBindingPattern(), this.backtrackOrNodeVariableList);

		if (this.deALSContext.isDebugEnabled()) {
			StringBuilder retval = new StringBuilder();
			retval.append("\nBEGIN: To find headBacktrackIndex: ");
			if (this.backtrackOrNodeVariableList.size() == 0) {
				retval.append("this.backtrackOrNodeVariableList = null");
			} else {
				retval.append("this.backtrackOrNodeVariableList = ");
				for (int i = 0; i < this.backtrackOrNodeVariableList.size(); i++) {
					if (i > 0)
						retval.append(" | ");
					retval.append(this.backtrackOrNodeVariableList.get(i).toString());
				}
			}
			retval.append("\nEND: To find headBacktrackIndex.");
			this.deALSContext.logDebug(logger, "{}", retval);
		}

		int headBacktrackIndex = this.setBacktrackIndex(andNode.getNumberOfChildren(), variableIndexMap);
		
		// APS 7/13/2014
		// no reason to backtrack into ComparisonNode if it is the last literal in the body go back 1 more before
		if ((headBacktrackIndex > 0) && (andNode.getChild(headBacktrackIndex) instanceof ComparisonNode))
			headBacktrackIndex--;

		this.backtrackOrNodeVariableList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateBacktrackPointForHead with headBacktrackIndex = {}", headBacktrackIndex);

		return headBacktrackIndex;
	}

	private void generateBacktrackPointForLiterals(AndNode andNode, int[] variableIndexMap, int[] backtrackMap) {
		this.deALSContext.logTrace(logger, "Entering generateBacktrackPointForLiterals for {}", andNode);

		OrNode orNode;
		// For each or node and the head, we find the closest backtrack or node to the left
		this.backtrackOrNodeVariableList.clear();

		for (int i = 0; i < andNode.getNumberOfChildren(); i++) {
			orNode = andNode.getChild(i);
			// if ((orNode = andNode.getLiteral(i)) != null) {
			// We need to check only those variables in or node because some of the
			// free variables may be partially bound and thus, relevant in determining
			// the closest backtracking point
			this.backtrackOrNodeVariableList.clear();
			orNode.getVariables(this.backtrackOrNodeVariableList);

			if (this.deALSContext.isDebugEnabled()) {
				StringBuilder retval = new StringBuilder();
				retval.append("\nBEGIN: To find backtrackIndex: ");
				if (this.backtrackOrNodeVariableList.size() == 0) {
					retval.append("this.backtrackOrNodeVariableList is empty");
				} else {
					retval.append("this.backtrackOrNodeVariableList = ");
					for (int j = 0; j < this.backtrackOrNodeVariableList.size(); j++) {
						if (j > 0)
							retval.append(" | ");
						retval.append(this.backtrackOrNodeVariableList.get(j).toString());
					}
				}
				retval.append("\nEND: To find backtrackIndex.");
				this.deALSContext.logDebug(logger, "{}", retval);
			}

			backtrackMap[i] = this.setBacktrackIndex(i, variableIndexMap);

			// APS 5/12/2014 - so comparison predicates can use status.FAIL and ENTRY_FAIL correctly
			// special case for assignment literals - just go back 1 spot
			if (orNode instanceof EqualityMixedBindingNode)
				if (((EqualityMixedBindingNode) orNode).getPredicateName().equals(BuiltInPredicate.EQUALITY_PREDICATE_NAME))
					backtrackMap[i] = i - 1;
		}

		this.backtrackOrNodeVariableList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateBacktrackPointForLiterals");
	}

	@SuppressWarnings("static-method")
	private void generateNormalBacktrackArray(int[] backtrackMap) {
		// for (int i = 0; i < andNode.getNumberOfChildren(); i++)
		for (int i = 0; i < backtrackMap.length; i++)
			backtrackMap[i] = i - 1;
	}

	private int setBacktrackIndex(int nodeIndex, int[] variableIndexMap) {
		this.deALSContext.logTrace(logger, "Entering setBacktrackIndex for nodeIndex = {}", nodeIndex);

		int backtrackIndex = -1;
		int variableIndex = 0;

		// For each variable in the or node, we find the or position of the node that binds its value
		for (int i = 0; i < this.backtrackOrNodeVariableList.size(); i++) {
			this.deALSContext.logDebug(logger, "this.backtrackOrNodeVariableList.get({}) = {}", i, this.backtrackOrNodeVariableList.get(i));

			if ((variableIndex = this.backtrackVariableList.getPosition(this.backtrackOrNodeVariableList.get(i))) > -1) {
				this.deALSContext.logDebug(logger, "variableIndexMap[{}] = {}", variableIndex, variableIndexMap[variableIndex]);
				this.deALSContext.logDebug(logger, "\nBEFORE: backtrackIndex = {}", backtrackIndex);

				// Only variables that were bound before current ornode are relevant
				// Only those ornodes closest to the left of current ornode are relevant
				if ((variableIndexMap[variableIndex] < nodeIndex)
						&& (variableIndexMap[variableIndex] > backtrackIndex))
					backtrackIndex = variableIndexMap[variableIndex];

				this.deALSContext.logDebug(logger, "AFTER: backtrackIndex = {}", backtrackIndex);

			} else if (!this.backtrackOrNodeVariableList.get(i).isAnonymous()) {
				// This could also mean that free variables in the head are unsafe
				// Anonymous variables are not flagged because they are used explicitly in the rewriting
				String message = "missing variable '" + this.backtrackOrNodeVariableList.get(i).toString() + "' in setBacktrackIndex()";
				this.deALSContext.logError(logger, message);
				throw new ProgramGeneratorException(message);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting setBacktrackIndex for nodeIndex = {} to {}", nodeIndex, backtrackIndex);

		return backtrackIndex;
	}

	private OrNode createNonRecursiveOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, 
			VariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering createNonRecursiveOrNode for {}", pcgOrNode);

		OrNode 			orNode;
		String 			predicateName 		= ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), 
												pcgOrNode.getBindingPattern());
		NodeArguments 	arguments 			= generateArguments(pcgOrNode.getArguments(), variableMappings);
		Binding 		binding 			= pcgOrNode.getExecutionBindingPattern().copy();
		VariableList 	freeVariableList 	= new VariableList();

		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		boolean materializedPredicate 
			= pcgOrNode.getPredicateName().startsWith(Rewriter.LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)
				|| pcgOrNode.getPredicateName().startsWith(Rewriter.NON_LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)
				|| pcgOrNode.getPredicateName().startsWith(Rewriter.REVERSE_MAGIC_PREDICATE_NAME_PREFIX);
			
		// We generate Materialized Or Node for Linear Supplementary Node
		if (materializedPredicate)
			orNode = new MaterializedPredicate(predicateName, arguments, binding, freeVariableList);
		else
			orNode = new OrNode(predicateName, arguments, binding, freeVariableList);
		
		this.deALSContext.logTrace(logger, "Exiting createNonRecursiveOrNode with {}", orNode);

		return orNode;
	}

	private OrNode extendNonRecursiveOrNode(PCGOrNode pcgOrNode, OrNode orNode, NodeList<IClique> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering extendNonRecursiveOrNode for {}", pcgOrNode);

		OrNode extendedOrNode = orNode;
		OrNode negatedOrNode = null;

		// step 1: check if we need to add a magic exit rule.
		// Sometimes, magic predicate may not be a clique, so check and generate magic exit rule if necessary
		this.generateMagicExitRule(pcgOrNode, extendedOrNode);

		if (pcgOrNode.getPredicate().isNegative()) {
			negatedOrNode = extendedOrNode;
			extendedOrNode = ((NegationOrNode) negatedOrNode).getLiteralToBeNegated();
		}

		if (extendedOrNode != null) {
			this.globalOrNodeStack.push(extendedOrNode);

			if (pcgOrNode.getPredicateName().startsWith(Rewriter.LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)
					|| pcgOrNode.getPredicateName().startsWith(Rewriter.NON_LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)
							|| pcgOrNode.getPredicateName().startsWith(Rewriter.REVERSE_MAGIC_PREDICATE_NAME_PREFIX)) /* APS 11/29/2013*/{
				if (!this.extendNonRecursiveMaterializedOrNode(pcgOrNode, (MaterializedPredicate) extendedOrNode,
						localSharableCliqueList))
					extendedOrNode = null;

				this.globalOrNodeStack.pop();
			} else {
				AndNode andNode;
				for (PCGOrNodeChild pcgAndNode : pcgOrNode.getChildren()) {
					VariableMappings variableMappings = new VariableMappings();
					VariableList boundVariableList = new VariableList();
					if ((andNode = this.generateAndNode((PCGAndNode) pcgAndNode, variableMappings, boundVariableList, 
							localSharableCliqueList, null, extendedOrNode, null, null, null, null, null, false)) != null)
						extendedOrNode.addChild(andNode);
				}
				this.globalOrNodeStack.pop();

				// step 2: check if there is no rules, we delete this orNode
				if (extendedOrNode.getNumberOfChildren() <= 0)
					extendedOrNode = null;
			}

			if ((extendedOrNode != null) && pcgOrNode.getPredicate().isNegative())
				extendedOrNode = negatedOrNode;
		}

		this.deALSContext.logTrace(logger, "Exiting extendNonRecursiveOrNode with {}", extendedOrNode);

		return extendedOrNode;
	}

	private boolean extendNonRecursiveMaterializedOrNode(PCGOrNode pcgOrNode, MaterializedPredicate materializedPredicate, 
			NodeList<IClique> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering extendNonRecursiveMaterializedOrNode for {}", pcgOrNode);

		boolean status = false;

		// Can not handle if it does not have exactly one rule
		if (pcgOrNode.getNumberOfChildren() != 1) {
			this.deALSContext.logError(logger, "Materialized orNode should have exactly one rule");
			throw new ProgramGeneratorException("Materialized orNode should have exactly one rule");
		}

		PCGAndNode pcgAndNode = (PCGAndNode) pcgOrNode.getChild(0);

		// BEGIN Generate ANDNode
		VariableMappings variableMappings = new VariableMappings();
		VariableList boundVariableList = new VariableList();
		NodeList<ChoicePredicate> localChoiceLiteralList = new NodeList<>();

		// BEGIN Generate MaterializedRule
		String predicateName = ProgramGenerator.generateUniquePredicateName(pcgAndNode.getPredicateName(), pcgAndNode.getBindingPattern());
		NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
		Binding binding = pcgAndNode.getExecutionBindingPattern().copy();

		Utilities.getBoundVariables(arguments, binding, boundVariableList);

		String materializedRelationName = this.generateUniqueRelationName(predicateName);
		MaterializedRule materializedRule = new MaterializedRule(predicateName, arguments, binding, materializedRelationName);

		materializedRule = (MaterializedRule) this.generateAndNodeChildren(materializedRule, pcgAndNode,
						variableMappings, boundVariableList, localSharableCliqueList, localChoiceLiteralList, null,
						null, null, null, null);
		// END MaterializedRule

		localChoiceLiteralList.clear();

		if (materializedRule != null) {
			materializedPredicate.setMaterializedRule(materializedRule);
			this.generateBacktrackMap(materializedRule, BacktrackType.ALL_BACKTRACK, false);
			status = true;
		}
		// END ANDNode

		this.deALSContext.logTrace(logger, "Exiting extendNonRecursiveMaterializedOrNode with status = {}", status);

		return status;
	}

	private RecursiveOrNode generateRecursiveOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, 
			VariableList boundVariableList, NodeList<IClique> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering generateRecursiveOrNode for {}", pcgOrNode);

		String 			predicateName 		= pcgOrNode.getPredicateName();
		Binding 		predicateBinding 	= pcgOrNode.getBindingPattern();
		int 			arity 				= pcgOrNode.getArity();
		CliqueBaseNode 	clique 				= null;
		CliqueBaseNode 	mutualClique 		= null;
		RecursiveOrNode recursiveOrNode 	= null;
		CliqueBase 		baseClique 			= pcgOrNode.getBaseClique();
		boolean 		status 				= true;

		// Clique Sharing Rules:
		// - all cliques or mutual cliques within unsharable clique are not sharable outside this unsharable clique
		// - all cliques or mutual cliques within unsharable clique are sharable within that unsharable clique
		// - all cliques or mutual cliques within sharable clique are sharable outside this sharable clique
		// - all tp1, tp2, linear or non-linear magic, gen.magic cliques are not sharable
		// - all semi-naive or naive clique are sharable
		if ((recursiveOrNode = this.createRecursiveOrNode(pcgOrNode, variableMappings, boundVariableList)) != null) {
			Triple<Boolean, IClique, IClique> retvalTriple;
			if (pcgOrNode.isSharableClique()
					&& ((retvalTriple = this.findSharableClique(predicateName, arity, predicateBinding, localSharableCliqueList)) != null)
							&& retvalTriple.getFirst()) {

				clique = (CliqueBaseNode) retvalTriple.getSecond();
				mutualClique = (CliqueBaseNode) retvalTriple.getThird();
				this.deALSContext.logInfo(logger, "******  Sharing Clique *****");
				this.deALSContext.logInfo(logger, "{}", pcgOrNode.toString());
				this.deALSContext.logInfo(logger, "With Parent Clique : {}", clique.toStringNode());
				this.deALSContext.logInfo(logger, "With Actual Clique : {}", mutualClique.toStringNode());
				this.deALSContext.logInfo(logger, "****************************");
			} else {
				this.deALSContext.logDebug(logger, "*** Pushing = {} to GlobalOrNodeStack ***", recursiveOrNode.toStringNode());

				this.globalOrNodeStack.push(recursiveOrNode);

				switch (baseClique.getType()) {
					case CLIQUE:
						clique = this.generateClique(pcgOrNode, (Clique) baseClique, localSharableCliqueList);
						mutualClique = clique;
						break;
					case XY_CLIQUE:
						clique = this.generateXYClique(pcgOrNode, (XYClique) baseClique, localSharableCliqueList);
						mutualClique = clique;
						break;
					default:
						status = false;
						break;
				}

				OrNode popped = this.globalOrNodeStack.pop();
				this.deALSContext.logDebug(logger, "*** Popping = {} ***", popped);

				if (pcgOrNode.isSharableClique())
					localSharableCliqueList.add(clique);
			}

			if (status) {
				recursiveOrNode.setParentClique(clique);
				recursiveOrNode.setClique(mutualClique);

				if (clique instanceof FSCliqueNode)
					recursiveOrNode.setEvaluationType(((FSCliqueNode) clique).getEvaluationType());

				recursiveOrNode.setRecursiveRelationName(mutualClique.getRecursiveRelationName());
			} else {
				recursiveOrNode = null;
			}
		}

		this.deALSContext.logTrace(logger, "Exiting generateRecursiveOrNode with status = {}", status);

		return recursiveOrNode;
	}

	private CliqueBaseNode generateClique(PCGOrNode pcgOrNode, Clique clique, NodeList<IClique> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering generateClique for {}", pcgOrNode);

		// APS 12/22/2013
		this.globalCreatingCliqueStack.push(clique);

		CliqueBaseNode 	cliqueNode 		= null;
		String 			predicateName 	= pcgOrNode.getPredicateName();
		int 			arity 			= pcgOrNode.getArity();
		Binding 		searchBinding 	= pcgOrNode.getBindingPattern();
		CliquePredicate cliquePredicate;

		if ((cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding)) == null) {
			// Since cliquePredicate with that binding does not exist, we know there is a catch-all
			// binding pattern i.e. all-free in performing a semi-naive fixpoint
			// In the future, we can more intelligent by choosing the clique predicate with the most
			// restricted binding that satisfy this binding
			searchBinding = new Binding(arity, BindingType.FREE);
			cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding);
		}

		if (cliquePredicate != null) {
			// for generating recursiveLiteral - if a clique or mtualClique is
			// put on this stack, then recursiveLiteral is generated.
			NodeStack<IClique> localCliqueStack = new NodeStack<>();
			// for sharing clique
			NodeList<IClique> newLocalSharableCliqueList = new NodeList<>();
			// for sharing mutualClique
			NodeList<IMutualClique> localSharableMutualCliqueList = new NodeList<>();
			// if a mutualClique is put on this stack, then the mutualClique can be shared
			NodeList<IMutualClique> localMutualCliqueList = new NodeList<>();
			// for storing in clique
			NodeList<RecursiveLiteral> localRecursiveLiteralList = new NodeList<>();

			// Determine whether it is linear or non-linear, we do it once and use the result later
			clique.determineRecursiveType();

			// Make all external cliques sharable to inside this clique.
			// And we also make those internal cliques sharable to outside this clique
			if (pcgOrNode.isSharableClique())
				newLocalSharableCliqueList = localSharableCliqueList;

			if (pcgOrNode.getBaseClique().getType() == CompilerType.XY_CLIQUE) {
				cliqueNode = this.createClique(pcgOrNode, searchBinding, CliqueType.XY_CLIQUE);
				localCliqueStack.push(cliqueNode);
				this.generateXYCliqueOrNodes(pcgOrNode, (XYClique) pcgOrNode.getBaseClique(),
						(XYCliqueNode) cliqueNode, localCliqueStack, newLocalSharableCliqueList,
						localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);
			} else {
				cliqueNode = this.createClique(pcgOrNode, searchBinding, CliqueType.CLIQUE);
				localCliqueStack.push(cliqueNode);
				this.generateCliqueExitAndRecursiveOrNodes(pcgOrNode, clique, (CliqueNode) cliqueNode, 
						localCliqueStack, newLocalSharableCliqueList, localSharableMutualCliqueList, 
						localMutualCliqueList, localRecursiveLiteralList);
			}

			localCliqueStack.pop();

			if (this.deALSContext.isDebugEnabled()) {
				this.deALSContext.logDebug(logger, "localRecursiveLiteralList = ");
				for (int i = 0; i < localRecursiveLiteralList.size(); i++)
					this.deALSContext.logDebug(logger, "i = " + i + ": " + localRecursiveLiteralList.get(i).toStringNode());
			}

			cliqueNode.setRecursiveLiteralList(localRecursiveLiteralList);
			cliqueNode.setMutualCliqueList(localMutualCliqueList);

			// we clear this list if it is not sharable
			if (!pcgOrNode.isSharableClique())
				newLocalSharableCliqueList.clear();

			localSharableMutualCliqueList.clear();
			localCliqueStack.clear();
		}

		// APS 12/22/2013
		this.globalCreatingCliqueStack.pop();

		this.deALSContext.logTrace(logger, "Exiting generateClique with {}", cliqueNode.toStringNode());

		return cliqueNode;
	}

	private Pair<OrNode, OrNode> generateCliqueExitAndRecursiveOrNodes(PCGOrNode pcgOrNode, Clique clique, CliqueNode cliqueNode,
			NodeStack<IClique> localCliqueStack, NodeList<IClique> localSharableCliqueList, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateCliqueExitAndRecursiveOrNodes for {}", pcgOrNode);

		CliquePredicate cliquePredicate;
		String 			predicateName 	= pcgOrNode.getPredicateName();
		int 			arity			= pcgOrNode.getArity();
		Binding 		searchBinding 	= pcgOrNode.getBindingPattern();
		OrNode 			exitRulesOrNode 		= null;
		OrNode 			recursiveRulesOrNode = null;

		if ((cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding)) == null) {
			// Since cliquePredicate with that binding does not exist, we know there is a catch-all
			// binding pattern i.e. all-free in performing a semi-naive fixpoint
			// In the future, we can more intelligent by choosing the clique predicate with the most
			// restricted binding that satisfy this binding
			searchBinding = new Binding(arity, BindingType.FREE);
			cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding);
		}

		if (cliquePredicate != null) {
			exitRulesOrNode = this.createRecursiveDummyOrNode(pcgOrNode, searchBinding);
					
			if ((exitRulesOrNode != null) && this.unifyWithClique(exitRulesOrNode, cliqueNode))
				cliqueNode.addExitRulesOrNode(exitRulesOrNode);
		
			this.generateMagicExitRule(pcgOrNode, exitRulesOrNode);
	
			AndNode andNode;
			for (PCGAndNode pcgAndNode : cliquePredicate.getExitRules()) {
				andNode = this.generateCliqueAndNode(pcgAndNode, clique, exitRulesOrNode, localCliqueStack, 
							localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, 
							localRecursiveLiteralList);
	
				if (andNode != null)
					exitRulesOrNode.addChild(andNode);
			}
			
			// APS 7/7/2014 - compress tree if process added unnecessary or/and parent nodes
			if ((exitRulesOrNode.getNumberOfChildren() == 1) 
				&& (exitRulesOrNode.getChild(0).getNumberOfChildren() == 1)) {
				exitRulesOrNode = exitRulesOrNode.getChild(0).getChild(0);
				cliqueNode.addExitRulesOrNode(exitRulesOrNode);
			}

			recursiveRulesOrNode = this.createRecursiveDummyOrNode(pcgOrNode, searchBinding);
			
			if ((recursiveRulesOrNode != null)
					&& this.unifyWithClique(recursiveRulesOrNode, cliqueNode))
				cliqueNode.addRecursiveRulesOrNode(recursiveRulesOrNode);
			
			for (PCGAndNode pcgAndNode : cliquePredicate.getRecursiveRules()) {
				andNode = this.generateCliqueAndNode(pcgAndNode, clique, recursiveRulesOrNode, 
						localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList,
						localMutualCliqueList, localRecursiveLiteralList);

				if (andNode != null)
					recursiveRulesOrNode.addChild(andNode);
			}

			// APS 7/7/2014 - compress tree if process added unnecessary or/and parent nodes
			if ((recursiveRulesOrNode.getNumberOfChildren() == 1) 
					&& (recursiveRulesOrNode.getChild(0).getNumberOfChildren() == 1)) {
				recursiveRulesOrNode = recursiveRulesOrNode.getChild(0).getChild(0);
				cliqueNode.addRecursiveRulesOrNode(recursiveRulesOrNode);
			}
		} else {
			this.deALSContext.logError(logger, "Clique predicate can not be found in clique");
			throw new ProgramGeneratorException("Clique predicate can not be found in clique");
		}

		this.deALSContext.logTrace(logger, "Exiting generateCliqueExitAndRecursiveOrNodes with {} and {}",
					exitRulesOrNode, recursiveRulesOrNode);

		return new Pair<>(exitRulesOrNode, recursiveRulesOrNode);
	}

	private AndNode generateCliqueAndNode(PCGAndNode pcgAndNode, CliqueBase clique, OrNode exitOrRecursiveOrNode,
			NodeStack<IClique> localCliqueStack, NodeList<IClique> localSharableCliqueList,
			NodeList<IMutualClique> localSharableMutualCliqueList, NodeList<IMutualClique> localMutualCliqueList,
			NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateCliqueAndNode for {}", pcgAndNode);

		AndNode 					andNode;
		VariableMappings 			variableMappings 		= new VariableMappings();
		VariableList 				boundVariableList 		= new VariableList();
		NodeList<ChoicePredicate> 	localChoiceLiteralList 	= new NodeList<>();
		NodeList<OrNode> 			frontEqualityList 		= new NodeList<>();
		NodeList<OrNode> 			backEqualityList 		= new NodeList<>();

		// All arguments in a recursive rule head are assumed to be free
		if (pcgAndNode.hasChoiceLiteral())
			andNode = this.createChoiceAndNode(pcgAndNode, variableMappings, boundVariableList);
		else
			andNode = this.createAndNode(pcgAndNode, variableMappings, boundVariableList);

		if ((andNode != null)
				&& this.unifyAndOrNodes(andNode, exitOrRecursiveOrNode, frontEqualityList, backEqualityList)) {
			andNode = this.generateCliqueAndNodeChildren(pcgAndNode, clique, variableMappings, boundVariableList, 
					andNode, localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList,
					localMutualCliqueList, localRecursiveLiteralList, localChoiceLiteralList);

			this.addEqualityOrNodes(andNode, frontEqualityList, backEqualityList);

			this.expandSugaredArguments(andNode);
			
			// We have to suppress intelligent backtracking for xy clique rules.
			// Since the stage variable in xy rules are eliminated, we can not do
			// intelligent backtracking based on variable range. --HW
			if (clique.getType() == CompilerType.XY_CLIQUE)
				this.generateBacktrackMap(andNode, BacktrackType.NO_BACKTRACK, false);
			else
				this.generateBacktrackMap(andNode, BacktrackType.ALL_BACKTRACK, false);
		} else {
			andNode = null;
		}

		if (andNode instanceof ChoiceRule) {
			// Put all choice literals in local list into the special and node, if there is any
			for (ChoicePredicate choiceLiteral : localChoiceLiteralList)
				((ChoiceRule) andNode).addChoicePredicate(choiceLiteral);
		}

		frontEqualityList.clear();
		backEqualityList.clear();
		localChoiceLiteralList.clear();

		if (andNode != null)
			this.deALSContext.logTrace(logger, "Exiting generateCliqueAndNode with {}", andNode);
		else
			this.deALSContext.logTrace(logger, "Exiting generateCliqueAndNode with null");

		return andNode;
	}

	private AndNode generateCliqueAndNodeChildren(PCGAndNode pcgAndNode, CliqueBase clique, VariableMappings variableMappings,
			VariableList boundVariableList, AndNode andNode, NodeStack<IClique> localCliqueStack, 
			NodeList<IClique> localSharableCliqueList, NodeList<IMutualClique> localSharableMutualCliqueList, 
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList, 
			NodeList<ChoicePredicate> localChoiceLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateCliqueAndNodeChildren for {}", pcgAndNode);

		OrNode 				orNode 						= null;
		NodeList<OrNode> 	complexObjectEqualityList 	= new NodeList<>();
		boolean 			all 						= true;
		// APS 4/1/2014 to hold original node in case we need to swap it out
		OrNode 				originalOrNode 				= null; 

		for (PCGOrNode pcgOrNode : pcgAndNode.getChildren()) {
			if ((pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.UNKNOWN)
					&& pcgOrNode.isRecursiveLiteral(clique)) {
				if (clique.getType() == CompilerType.CLIQUE) {
					orNode = this.generateCliqueOrNode(pcgOrNode, (Clique) clique, variableMappings, boundVariableList,
							localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList,
							localMutualCliqueList, localRecursiveLiteralList);
				} else if (clique.getType() == CompilerType.XY_CLIQUE) {
					orNode = this.generateXYCliqueOrNode(pcgOrNode, clique, variableMappings, boundVariableList, localCliqueStack,
							localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, 
							localRecursiveLiteralList);
				} else {
					this.deALSContext.logError(logger, "Type of clique trying to be generated is not allowed");
					throw new ProgramGeneratorException("Type of clique trying to be generated is not allowed");
				}
			} else {
				// if pcgOrNode is built-in (ifthen, ifthenelse), then it's possible
				// that the sub-clauses are recursive with XY clique -- HW
				orNode = this.generateOrNode(pcgOrNode, variableMappings, boundVariableList, localSharableCliqueList,
						localChoiceLiteralList, clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);
			}

			if (orNode != null) {
				if (orNode instanceof XYStageVariableNode)
					((XYStageVariableNode) orNode).setXYNode(andNode);

				// APS 3/25/2013 - added check for read fs pred
				if (!(pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE 
						|| pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS
						|| pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.AGGREGATE_FS)
						 && this.extractFreeFunctorFromOrNode(orNode, complexObjectEqualityList)) {
					// this will update the bound variable list and add the orNode to andNode
					this.addOrNodesToAndNode(orNode, complexObjectEqualityList, andNode, boundVariableList);
				} else {
					Utilities.getVariables(orNode.getArguments(), boundVariableList);
					andNode.addChild(orNode);
				}

				complexObjectEqualityList.clear();
				originalOrNode = orNode;
				// if not recursive, we need to extend with more andNodes
				if (!pcgOrNode.isRecursiveLiteral(clique))
					orNode = this.extendOrNode(pcgOrNode, orNode, localSharableCliqueList);
			}

			if (orNode != null) {
				if (originalOrNode != orNode) {
					 for (int i = 0; i < andNode.getNumberOfChildren(); i++) {
						 if (andNode.getChild(i) == originalOrNode) {
							 andNode.replaceChild(i, orNode);
							 break;
						 }
					 }
				 }
			} else {
				all = false;
				break;
			}
		}

		if (!all)
			andNode = null;

		complexObjectEqualityList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateCliqueAndNodeChildren with {}", andNode);

		return andNode;
	}

	private OrNode generateCliqueOrNode(PCGOrNode pcgOrNode, Clique clique, VariableMappings variableMappings, 
			VariableList boundVariableList, NodeStack<IClique> localCliqueStack, NodeList<IClique> localSharableCliqueList, 
			NodeList<IMutualClique> localSharableMutualCliqueList, NodeList<IMutualClique> localMutualCliqueList, 
			NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateCliqueOrNode for {}", pcgOrNode);

		CliqueNode 		generatedClique 	= null;
		OrNode 			orNode 				= null;
		String 			predicateName 		= pcgOrNode.getPredicateName();
		int 			arity 				= pcgOrNode.getArity();
		Binding 		predicateBinding 	= pcgOrNode.getBindingPattern();
		CliquePredicate cliquePredicate;

		if ((cliquePredicate = clique.getCliquePredicate(predicateName, arity, predicateBinding)) == null) {
			// Since cliquePredicate with that binding does not exist, we know
			// there is a catch-all binding pattern i.e. all-free in performing a semi-naive fixpoint
			// In the future, we can more intelligent by choosing the clique
			// predicate with the most restricted binding that satisfy this binding
			Binding allFreeBinding = new Binding(arity, BindingType.FREE);
			cliquePredicate = clique.getCliquePredicate(predicateName, arity, allFreeBinding);

			if (cliquePredicate == null) {
				this.deALSContext.logError(logger, "Clique predicate cannot be found in clique");
				throw new ProgramGeneratorException("Clique predicate cannot be found in clique");
			}

			generatedClique = (CliqueNode) this.findFromNodeList(predicateName, arity, allFreeBinding, localCliqueStack);
		} else {
			generatedClique = (CliqueNode) this.findFromNodeList(predicateName, arity, predicateBinding, localCliqueStack);
		}

		if (generatedClique != null) {
			orNode = this.createRecursiveLiteral(pcgOrNode, variableMappings, boundVariableList, clique, 
					generatedClique, localRecursiveLiteralList);
		} else {
			orNode = this.generateMutualRecursiveOrNode(pcgOrNode, clique, variableMappings, boundVariableList, 
					localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, 
					localMutualCliqueList, localRecursiveLiteralList);
		}

		this.deALSContext.logTrace(logger, "Exiting generateCliqueOrNode with {}", orNode);

		return orNode;
	}

	private OrNode generateMutualRecursiveOrNode(PCGOrNode pcgOrNode, CliqueBase clique, VariableMappings variableMappings,
			VariableList boundVariableList, NodeStack<IClique> localCliqueStack,
			NodeList<IClique> localSharableCliqueList, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateMutualRecursiveOrNode for {}", pcgOrNode);

		String 			predicateName 	= pcgOrNode.getPredicateName();
		RecursiveOrNode recursiveOrNode = this.createRecursiveOrNode(pcgOrNode, variableMappings, boundVariableList);

		Triple<Boolean, IClique, IClique> retvalTriple = this.findSharableMutualClique(predicateName, pcgOrNode.getArity(),
						pcgOrNode.getBindingPattern(), localSharableMutualCliqueList);

		IMutualClique mutualClique = (IMutualClique) retvalTriple.getSecond();
		IMutualClique actualMutualClique = (IMutualClique) retvalTriple.getThird();

		if (retvalTriple.getFirst()) {
			this.deALSContext.logDebug(logger, "******  Sharing Mutual Clique *****");
			this.deALSContext.logDebug(logger, "{}", pcgOrNode);
			this.deALSContext.logDebug(logger, "With Parent Clique : {}", mutualClique.toStringNode());
			this.deALSContext.logDebug(logger, "With Actual Clique : {}", actualMutualClique.toStringNode());
			this.deALSContext.logDebug(logger, "*************************************");			
		} else {
			// for sharing mutualClique
			NodeList<IMutualClique> newLocalSharableMutualCliqueList = new NodeList<>();

			// if a mutualClique is put on this stack, then the mutualClique can be shared
			NodeList<IMutualClique> newLocalMutualCliqueList = new NodeList<>();

			switch (clique.getType()) {
				case CLIQUE:
					mutualClique = this.generateMutualClique(pcgOrNode, (Clique) clique, localCliqueStack,
							localSharableCliqueList, newLocalSharableMutualCliqueList,
							newLocalMutualCliqueList, localRecursiveLiteralList);
					actualMutualClique = mutualClique;
					break;
				case XY_CLIQUE:
					mutualClique = this.generateMutualXYClique(pcgOrNode, (XYClique) clique, localCliqueStack,
							localSharableCliqueList, newLocalSharableMutualCliqueList,
							newLocalMutualCliqueList, localRecursiveLiteralList);
					actualMutualClique = mutualClique;
					break;
				default:
					this.deALSContext.logError(logger, "Unknown clique type in generateMutualRecursiveOrNode");
					throw new ProgramGeneratorException("Unknown clique type in generateMutualRecursiveOrNode");
			}

			for (IMutualClique localMutualClique : newLocalMutualCliqueList)
				localMutualCliqueList.add(localMutualClique);

			mutualClique.setMutualCliqueList(newLocalMutualCliqueList);
			localMutualCliqueList.add(mutualClique);
			localSharableMutualCliqueList.add(mutualClique);
		}

		recursiveOrNode.setParentClique((CliqueBaseNode) mutualClique);
		recursiveOrNode.setClique((CliqueBaseNode) actualMutualClique);

		if (actualMutualClique instanceof FSCliqueNode) 
			recursiveOrNode.setEvaluationType(((FSCliqueNode) actualMutualClique).getEvaluationType());
				
		recursiveOrNode.setRecursiveRelationName(actualMutualClique.getRecursiveRelationName());
		
		this.deALSContext.logTrace(logger, "Exiting generateMutualRecursiveOrNode with {}", recursiveOrNode);

		return recursiveOrNode;
	}

	private IMutualClique generateMutualClique(PCGOrNode pcgOrNode, Clique clique, 
			NodeStack<IClique> localCliqueStack, NodeList<IClique> localSharableCliqueList,
			NodeList<IMutualClique> localSharableMutualCliqueList, NodeList<IMutualClique> localMutualCliqueList,
			NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateMutualClique for {}", pcgOrNode);

		IMutualClique 	mutualClique 	= null;
		String 			predicateName 	= pcgOrNode.getPredicateName();
		int 			arity 			= pcgOrNode.getArity();
		Binding 		searchBinding	= pcgOrNode.getBindingPattern();
		CliquePredicate cliquePredicate = null;

		if ((cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding)) == null) {
			// since cliquePredicate with that binding does not exist, we know
			// there is a catch-all binding pattern i.e. all-free in performing a semi-naive fixpoint
			// In the future, we can more intelligent by choosing the clique
			// predicate with the most restricted binding that satisfy this binding
			searchBinding = new Binding(arity, BindingType.FREE);
			cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding);
		}

		if (cliquePredicate != null) {
			mutualClique = (IMutualClique) this.createClique(pcgOrNode, searchBinding, CliqueType.MUTUAL_CLIQUE);
			localCliqueStack.push(mutualClique);

			this.generateCliqueExitAndRecursiveOrNodes(pcgOrNode, clique, (CliqueNode) mutualClique, 
					localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList,
					localMutualCliqueList, localRecursiveLiteralList);

			localCliqueStack.pop();
			localSharableMutualCliqueList.clear();
		}

		this.deALSContext.logTrace(logger, "Exiting generateMutualClique with {}", mutualClique);

		return mutualClique;
	}

	private RecursiveOrNode createRecursiveOrNode(PCGOrNode pcgOrNode, VariableMappings variableMappings, 
			VariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering createRecursiveOrNode for {}", pcgOrNode);

		String predicateName = ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), pcgOrNode.getBindingPattern(),
												pcgOrNode.getXYStageVariableBinding());

		NodeArguments 	arguments 			= generateArguments(pcgOrNode.getArguments(), variableMappings);
		Binding 		binding 			= pcgOrNode.getExecutionBindingPattern().copy();
		VariableList 	freeVariableList 	= new VariableList();

		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		// APS changed on 3/9/2013 to include xy stage binding
		if (pcgOrNode.hasXYStageVariableBinding()) {
			Binding executionBinding = pcgOrNode.getExecutionBindingPattern().copy();
			binding = new Binding(executionBinding.getArity() + 1);
			for (int i = 0; i < executionBinding.getArity(); i++)
				binding.setBinding(i, executionBinding.getBinding(i));

			binding.setBinding(binding.getArity() - 1, pcgOrNode.getXYStageVariableBinding());
		} else {
			binding = pcgOrNode.getExecutionBindingPattern().copy();
		}

		RecursiveOrNode recursiveOrNode;
		if (pcgOrNode.getBaseClique().getType() == CompilerType.XY_CLIQUE) {
			recursiveOrNode = new XYRecursiveOrNode(predicateName, arguments, binding, 
					pcgOrNode.getXYStageVariableBinding(), freeVariableList);
		} else {
			recursiveOrNode = new RecursiveOrNode(predicateName, arguments, binding, freeVariableList);
		}

		this.deALSContext.logTrace(logger, "Exiting createRecursiveOrNode with {}", recursiveOrNode);

		return recursiveOrNode;
	}

	private CliqueBaseNode createClique(PCGOrNode pcgOrNode, Binding searchBinding, CliqueType cliqueType) {
		this.deALSContext.logTrace(logger, "Entering createClique for {}", pcgOrNode);

		CliqueBaseNode 	clique 				= null;
		// it can be used for resetting cursor
		String 			predicateName 		= ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), searchBinding);
		NodeArguments 	arguments 			= new NodeArguments();
		VariableList 	freeVariableList 	= new VariableList();

		ProgramGenerator.generateUniqueArguments(pcgOrNode.getArguments(), arguments, freeVariableList);
		// binding for clique should always be all-free
		Binding binding = new Binding(arguments.size(), BindingType.FREE);
		String recursiveRelationName = this.generateUniqueRelationName(predicateName);

		// APS 4/22/2013
		// we override the setting if there are FS aggregates present
		for (int i = 0; i < pcgOrNode.getPredicate().getArity(); i++) {
			if ((pcgOrNode.getPredicate().getArgumentTypeAdornment().get(i) == ArgumentType.FSAGGREGATE)) {
				if (cliqueType == CliqueType.CLIQUE)
					cliqueType = CliqueType.FS_CLIQUE;
				else if (cliqueType == CliqueType.MUTUAL_CLIQUE)
					cliqueType = CliqueType.MUTUAL_FS_CLIQUE;
				break;
			}
		}

		EvaluationType evaluationType = EvaluationType.getEvaluationType(this.deALSContext.getConfiguration().getProperty("deals.interpreter.fsclique.evaluationtype").toLowerCase()); 
		
		switch (cliqueType) {
			case CLIQUE:
				clique = new CliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, pcgOrNode.isSharableClique());
				break;
			case FS_CLIQUE:
				clique = FSCliqueNode.createFSCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, pcgOrNode.isSharableClique(),
						pcgOrNode.getPredicate().getArgumentTypeAdornment(), evaluationType);
				break;
			case MUTUAL_CLIQUE:
				clique = new MutualCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName);
				break;
			case MUTUAL_FS_CLIQUE:
				clique = (CliqueBaseNode) MutualFSCliqueNode.createMutualFSCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, 
						pcgOrNode.getPredicate().getArgumentTypeAdornment(), evaluationType);
				break;
			case XY_CLIQUE:
				clique = new XYCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName, 
						pcgOrNode.isSharableClique());
				// Right now XY rules have no rewriting methods. So pcgOrNode is
				// always sharable. i.e., in a rule like <-p(I,X),p(I+1,Y), clique is shared. --HW
				break;
			case MUTUAL_XY_CLIQUE:
				clique = new MutualXYCliqueNode(predicateName, arguments, binding, freeVariableList, recursiveRelationName);
				break;
			default:
				this.deALSContext.logError(logger, "Unknown clique type in createClique");
				throw new ProgramGeneratorException("Unknown clique type in createClique");
		}

		if (!(clique instanceof XYCliqueNode))
			clique.setIsLinearRecursive(((Clique) pcgOrNode.getChild(0)).isLinearRecursive());

		this.deALSContext.logTrace(logger, "Exiting createClique with {}", clique.toStringNode());

		return clique;
	}

	private OrNode createRecursiveDummyOrNode(PCGOrNode pcgOrNode, Binding searchBinding) {
		this.deALSContext.logTrace(logger, "Entering createRecursiveDummyOrNode for {}",pcgOrNode);

		String 			predicateName 		= ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), searchBinding);
		NodeArguments 	arguments 			= new NodeArguments();
		VariableList 	freeVariableList	= new VariableList();

		ProgramGenerator.generateUniqueArguments(pcgOrNode.getArguments(), arguments, freeVariableList);

		Binding binding = new Binding(arguments.size(), BindingType.FREE);
		OrNode 	orNode 	= new OrNode(predicateName, arguments, binding, freeVariableList);

		this.deALSContext.logTrace(logger, "Exiting createRecursiveDummyOrNode with {}", orNode);

		return orNode;
	}

	private OrNode createRecursiveLiteral(PCGOrNode pcgOrNode, VariableMappings variableMappings, 
			VariableList boundVariableList, CliqueBase clique, CliqueBaseNode generatedClique,
			NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering createRecursiveLiteral for {}", pcgOrNode);

		String predicateName = ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), pcgOrNode.getBindingPattern(),
				pcgOrNode.getXYStageVariableBinding());

		NodeArguments arguments = generateArguments(pcgOrNode.getArguments(), variableMappings);
		Binding binding = pcgOrNode.getExecutionBindingPattern().copy();

		// APS changed on 3/9/2013 to include xy stage binding
		if (pcgOrNode.hasXYStageVariableBinding()) {
			Binding executionBinding = pcgOrNode.getExecutionBindingPattern().copy();
			binding = new Binding(executionBinding.getArity() + 1);
			for (int i = 0; i < executionBinding.getArity(); i++)
				binding.setBinding(i, executionBinding.getBinding(i));

			binding.setBinding(binding.getArity() - 1, pcgOrNode.getXYStageVariableBinding());
		} else {
			binding = pcgOrNode.getExecutionBindingPattern().copy();
		}

		VariableList freeVariableList = new VariableList();

		Utilities.getVariables(arguments, freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		RecursiveLiteral recursiveLiteral = null;

		// APS 2/5/2014
		// eager monotonic uses the aggregate relation
		String relationName = generatedClique.getRecursiveRelationName();
		EvaluationType evaluationType = EvaluationType.SemiNaive;
		if (generatedClique instanceof FSCliqueNode) {
			evaluationType = ((FSCliqueNode) generatedClique).getEvaluationType();
			switch (evaluationType) {
				//case MonotonicSemiNaive:	
				case SSC:
				case EagerMonotonic:
					relationName = AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX + 
							generatedClique.getPredicateName().substring(0, generatedClique.getPredicateName().lastIndexOf("_"));
					break;				
			}
		}
		
		if (clique.isLinearRecursive()) {
			recursiveLiteral = new LinearRecursiveLiteral(predicateName, arguments, binding, freeVariableList, relationName);
		} else if (clique.isNonLinearRecursive()) {
			// APS 11/1/2013 so we get semi-naive evaluation for quadratic rules
			if (binding.allFree())
				recursiveLiteral = new LinearRecursiveLiteral(predicateName, arguments, binding, freeVariableList, relationName);
			else
				recursiveLiteral = new NonLinearRecursiveLiteral(predicateName, arguments, binding, freeVariableList, relationName);
		} else {
			this.deALSContext.logError(logger, "Clique is neither linear nor non-linear");
			throw new ProgramGeneratorException("Clique is neither linear nor non-linear");
		}

		recursiveLiteral.setEvaluationType(evaluationType);
		recursiveLiteral.setFSAggregateType(pcgOrNode.getPredicate().getFSAggregateType());

		localRecursiveLiteralList.add(recursiveLiteral);

		String recursionType = clique.isLinearRecursive() ? "Linear" : "Non-Linear";
		this.deALSContext.logTrace(logger, "Exiting createRecursiveLiteral {} with recursion type {}", recursiveLiteral, recursionType);

		return recursiveLiteral;
	}

	// Given a predicate 'pred' with binding pattern, 'bffb' the magic predicate name is created as 'magic_bffb_pred'
	private void generateMagicExitRule(PCGOrNode pcgOrNode, OrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering generateMagicExitRule for {}", pcgOrNode);

		NodeList<OrNode> 	frontEqualityList 	= new NodeList<>();
		NodeList<OrNode> 	backEqualityList 	= new NodeList<>();
		AndNode 			magicExitRule 		= this.constructMagicExitRule(pcgOrNode);

		if ((magicExitRule != null) 
				&& this.unifyAndOrNodes(magicExitRule, orNode, frontEqualityList, backEqualityList)) {
			orNode.addChild(magicExitRule);
			this.addEqualityOrNodes(magicExitRule, frontEqualityList, backEqualityList);
			this.generateBacktrackMap(magicExitRule, BacktrackType.ALL_BACKTRACK, false);
		}

		frontEqualityList.clear();
		backEqualityList.clear();

		this.deALSContext.logTrace(logger, "Exiting generateMagicExitRule");
	}

	private AndNode constructMagicExitRule(PCGOrNode pcgOrNode) {
		this.deALSContext.logTrace(logger, "Entering constructMagicExitRule for {}", pcgOrNode);

		AndNode magicExitRule = null;
		String postfix;

		// here, we make a very special assumption that magic predicate begins
		// with "$$_magic_" in order to locate the spot for adding the magic exit rule
		if ((postfix = Utilities.getPostfix(pcgOrNode.getPredicateName(), Rewriter.MAGIC_PREDICATE_NAME_PREFIX)) != null) {
			OrNode stackOrNode = this.searchGlobalOrNodeStack(postfix);

			if (stackOrNode != null) {
				NodeArguments 	magicArguments 			 = new NodeArguments();
				String 			uniqueMagicPredicateName = ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), 
																								pcgOrNode.getBindingPattern());
				Binding 		magicBinding 			 = new Binding(pcgOrNode.getArity(), BindingType.FREE);

				magicExitRule = new AndNode(uniqueMagicPredicateName, magicArguments, magicBinding);

				// set magic arguments to bound argument of stackOrNode
				// we assume that the number of bound arguments in stack_node
				// is the same as the number of arguments in magic and node
				int orNodeCount = 0;
				for (int i = 0; i < stackOrNode.getArity(); i++) {
					if (stackOrNode.getBinding(i) == BindingType.BOUND) {
						Variable variable1 = ProgramGenerator.generateUniqueVariable();
						Variable variable2 = ProgramGenerator.generateUniqueVariable();
						VariableList freeVariableList = new VariableList();

						Binding equalityBinding = new Binding(2);

						if (pcgOrNode.getExecutionBinding(orNodeCount) == BindingType.BOUND) {
							equalityBinding.setBinding(0, BindingType.BOUND);
						} else {
							equalityBinding.setBinding(0, BindingType.FREE);
							freeVariableList.add(variable1);
						}

						magicArguments.add(variable1);

						NodeArguments equalityArguments = new NodeArguments(2);
						equalityArguments.set(0, variable1);
						equalityArguments.set(1, variable2);
						equalityBinding.setBinding(1, BindingType.BOUND);

						variable2.setValue(stackOrNode.getArgument(i));
						EqualityMixedBindingNode orNode = new EqualityMixedBindingNode(equalityArguments, equalityBinding, 
																						freeVariableList);
						magicExitRule.addChild(orNode);

						orNodeCount++;
					}
				}

				this.deALSContext.logTrace(logger, "Magic exit rule is generated for {} is: ");
				this.deALSContext.logTrace(logger, "{}", magicExitRule.toStringTree());
			}
		}

		if (magicExitRule == null)
			this.deALSContext.logTrace(logger, "Exiting constructMagicExitRule with null");
		else
			this.deALSContext.logTrace(logger, "Exiting constructMagicExitRule with {}", magicExitRule);

		return magicExitRule;
	}

	/*************************************************************
	 * search for RecursiveOrNode on stack corresponding to magic predicate
	 * 
	 * Given a predicate 'pred' with binding pattern, say 'bffb' the magic predicate name is written as 'magic_bffb_pred'.
	 * Thus, while search for the right clique on stack, we need to first extract the binding pattern from the 
	 * magic predicate name, note that the prefix 'magic_' has already been stripped. Thus, we construct the binding 
	 * pattern for clique and then, compare the predicate name and the binding pattern
	 **************************************************************/
	private OrNode searchGlobalOrNodeStack(String predicateName) {
		this.deALSContext.logTrace(logger, "Entering searchGlobalOrNodeStack for {}", predicateName);

		OrNode stackOrNode = null;

		if (this.deALSContext.isDebugEnabled()) {
			this.deALSContext.logDebug(logger, "this.globalOrNodeStack = ");
			for (OrNode orNode : this.globalOrNodeStack)
				this.deALSContext.logDebug(logger, orNode.toStringNode());
		}

		for (OrNode orNode : this.globalOrNodeStack) {
			if (this.deALSContext.isDebugEnabled()) {
				this.deALSContext.logDebug(logger, "checking {} with predicateName = {}", orNode.toStringNode(), predicateName);
				this.deALSContext.logDebug(logger, "orNode.getPredicateName() = {}" + orNode.getPredicateName());
			}

			if (orNode.getPredicateName().equals(predicateName)) {
				if (this.deALSContext.isDebugEnabled()) {
					this.deALSContext.logDebug(logger, "bindingPattern.isPrefix succeeds. predicateName = {}", predicateName);
					this.deALSContext.logDebug(logger, "orNode.getPredicateName() = {}", orNode.getPredicateName());
					this.deALSContext.logDebug(logger, "{}", orNode.toStringNode());
				}

				stackOrNode = orNode;
				break;
			}
		}

		if (stackOrNode == null)
			this.deALSContext.logTrace(logger, "Exiting searchGlobalOrNodeStack with null");
		else
			this.deALSContext.logTrace(logger, "Exiting searchGlobalOrNodeStack with {}", stackOrNode);

		return stackOrNode;
	}

	private void addEqualityOrNodes(AndNode andNode, NodeList<OrNode> frontEqualityList,
			NodeList<OrNode> backEqualityList) {
		this.deALSContext.logTrace(logger, "Entering addEqualityOrNodes for {}", andNode);

		if (frontEqualityList.size() > 0 || backEqualityList.size() > 0) {
			NodeList<OrNode> complexObjectEqualityList = new NodeList<>();
			VariableList boundVariableList = new VariableList();

			// get all bound variables from the head
			Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(), boundVariableList);

			// The equality orNode list came in the same order as in the arguments
			// We need to match through them in the same order in order to regenerating the free variable list 
			// for the new equality because as we introduce equalities, previous equalities could change the binding 
			// pattern of this equality. The boundVariableList is updated as processing occurs
			for (OrNode equalityOrNode : frontEqualityList) {
				this.regenerateFreeVariableList(equalityOrNode, boundVariableList);

				Utilities.getVariables(equalityOrNode.getArguments(), boundVariableList);
			}

			// After we have regenerated the freeVariableList, we need to add in
			// front of all children of the andNode in the reverse order
			OrNode equalityOrNode2;
			for (int i = frontEqualityList.size() - 1; i >= 0; i--) {
				equalityOrNode2 = frontEqualityList.get(i);
				complexObjectEqualityList.clear();

				if (this.extractFreeFunctorFromOrNode(equalityOrNode2, complexObjectEqualityList)) {
					this.addOrNodesToAndNode(equalityOrNode2, complexObjectEqualityList, andNode,
							boundVariableList, true);
				} else {
					andNode.addChildAtHead(equalityOrNode2);
				}
			}

			// We do not need to regenerate the freeVariableList because the original
			// term extracted from the andNode is bound anyway regardless of how
			// many previous equalities there were.
			for (OrNode equalityOrNode : backEqualityList)
				andNode.addChild(equalityOrNode);

			complexObjectEqualityList.clear();
		}

		this.deALSContext.logTrace(logger, "Exiting addEqualityOrNodes for {}", andNode);
	}
	
	private String generateUniqueRelationName(String predicateName) {
		String relationName = predicateName;
		int numberCreated = 1;
		// if first relation for this predicateName, we don't append the count
		if (this.uniqueRelationNameMapping.containsKey(predicateName)) {
			numberCreated = this.uniqueRelationNameMapping.get(predicateName);
			numberCreated++;
			relationName += "_" + numberCreated;
		}
				
		this.uniqueRelationNameMapping.put(predicateName, numberCreated);
				
		return relationName;
	}

	private static Variable generateUniqueVariable() {
		return new Variable(UNIQUE_VARIABLE_NAME_PREFIX + variableCount++);
	}

	private static String generateUniquePredicateName(String predicateName, Binding predicateBinding) {
		return predicateName + "_" + predicateBinding.createBindingString();
	}

	// APS added 3/9/2013 to account for execution binding not being added
	private static String generateUniquePredicateName(String predicateName, Binding predicateBinding, 
			BindingType xyBindingType) {
		String uniqueString = predicateName + "_" + predicateBinding.createBindingString();
		if (xyBindingType != null)
			uniqueString += xyBindingType.toString();
		return uniqueString;
	}

	private void regenerateFreeVariableList(OrNode orNode, VariableList boundVariableList) {
		VariableList freeVariableList = new VariableList();

		Utilities.getVariables(orNode.getArguments(), freeVariableList);
		Utilities.removeBoundVariables(this.deALSContext, boundVariableList, freeVariableList);

		orNode.overWriteFreeVariableList(freeVariableList);
	}

	@SuppressWarnings("static-method")
	private IClique findFromNodeList(String predicateNameWithBinding, int arity, NodeList<IClique> nodeList) {
		for (IClique node : nodeList) {
			if (predicateNameWithBinding.equals(node.getPredicateName()) && arity == node.getArity())
				return node;
		}
		return null;
	}

	private IClique findFromNodeList(String predicateName, int arity, Binding predicateBinding, 
			NodeList<IClique> nodeList) {
		String predicateNameWithBinding = ProgramGenerator.generateUniquePredicateName(predicateName, predicateBinding);
		return this.findFromNodeList(predicateNameWithBinding, arity, nodeList);
	}

	private Triple<Boolean, IClique, IClique> matchCliqueOrSubClique(String predicateName, int arity, 
			Binding predicateBinding, NodeList<IClique> sharableCliqueList) {

		boolean status = false;
		String predicateNameWithBinding = ProgramGenerator.generateUniquePredicateName(predicateName, predicateBinding);
		IClique actualClique = null;
		IClique parentClique = null;

		for (IClique clique : sharableCliqueList) {
			parentClique = clique;
			if ((predicateNameWithBinding.equals(parentClique.getPredicateName())) 
					&& (arity == parentClique.getArity())) {
				actualClique = parentClique;
				status = true;
				break;
			}

			// stupid type erasure
			NodeList<IClique> cliqueList = new NodeList<>();
			for (IMutualClique mc : parentClique.getMutualCliqueList())
				cliqueList.add(mc);
			// we try to share the mutual clique if the clique is sharable
			actualClique = this.findFromNodeList(predicateNameWithBinding, arity, cliqueList);
			if (actualClique != null) {
				status = true;
				break;
			}
		}

		if (!status) {
			parentClique = null;
			actualClique = null;
		}

		return new Triple<>(status, parentClique, actualClique);
	}

	private Triple<Boolean, IClique, IClique> findSharableMutualClique(String predicateName, int arity, 
			Binding predicateBinding, NodeList<IMutualClique> sharableMutualCliqueList) {
		NodeList<IClique> cliqueList = new NodeList<>();
		for (IMutualClique mc : sharableMutualCliqueList)
			cliqueList.add(mc);

		return findSharableClique(predicateName, arity, predicateBinding, cliqueList);
	}

	private Triple<Boolean, IClique, IClique> findSharableClique(String predicateName, int arity, Binding predicateBinding, 
			NodeList<IClique> sharableCliqueList) {

		boolean status = false;
		IClique parentClique = null;
		IClique actualClique = null;

		Triple<Boolean, IClique, IClique> retvalTriple = this.matchCliqueOrSubClique(predicateName, arity, predicateBinding,
						sharableCliqueList);
		parentClique = retvalTriple.getSecond();
		actualClique = retvalTriple.getThird();

		if (retvalTriple.getFirst()) {
			status = true;
		} else {
			Binding allFreeBinding = new Binding(arity, BindingType.FREE);
			retvalTriple = this.matchCliqueOrSubClique(predicateName, arity, allFreeBinding, sharableCliqueList);
			parentClique = retvalTriple.getSecond();
			actualClique = retvalTriple.getThird();
			if (retvalTriple.getFirst())
				status = true;
		}

		if (!status) {
			parentClique = null;
			actualClique = null;
		}

		return new Triple<>(status, parentClique, actualClique);
	}

	private static void generateUniqueArguments(CompilerTypeList oldArguments, NodeArguments newArguments, 
			VariableList freeVariableList) {
		Variable variable;

		for (int i = 0; i < oldArguments.size(); i++) {
			variable = ProgramGenerator.generateUniqueVariable();
			variable.setDataType(TypeInferrer.inferTermDataType(oldArguments.get(i)));

			newArguments.add(variable);

			if (freeVariableList != null)
				freeVariableList.add(variable);
		}
	}

	private boolean extractFreeFunctorFromOrNode(OrNode orNode, NodeList<OrNode> functorEqualityList) {
		this.deALSContext.logTrace(logger, "Entering extractFreeFunctorFromOrNode for {}", orNode);

		NodeArguments 	arguments 	= orNode.getArguments();
		Argument 		argument 	= null;

		if (orNode.getPredicateName().equals(BuiltInPredicate.EQUALITY_PREDICATE_NAME)
				&& orNode.getArity() == 2) {
			// we do not create equality for top-level complex object in a equality & can not be free-free
			// and we only need to perform extraction for free arguments
			if (orNode.getBinding(0) == BindingType.FREE) {
				argument = arguments.get(0);
			} else {
				if (orNode.getBinding(1) == BindingType.FREE)
					argument = arguments.get(1);
			}
		} else {
			Pair<Boolean, Argument> retvalPair;
			for (int i = 0; i < orNode.getArity(); i++) {
				argument = arguments.get(i);

				if (orNode.getBinding(i) == BindingType.FREE) {
					retvalPair = this.extractFreeFunctorFromTerm(argument, functorEqualityList, true);
					if (retvalPair.getFirst()) {
						argument = retvalPair.getSecond();
						arguments.set(i, argument);
					}
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting extractFreeFunctorFromOrNode with status = {}", (functorEqualityList.size() > 0));

		return (functorEqualityList.size() > 0);
	}
	
	private boolean extractConstantFiltersFromAggregateNode(OrNode node, NodeList<OrNode> equalityPredicateList) {
		edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateRelationNode aggregateNode = 
				(edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.AggregateRelationNode)node;
		AggregateInfo[] aggregateInfos = aggregateNode.getAggregateInfos();
		for (int i = aggregateNode.getArity() - aggregateInfos.length; i < aggregateNode.getArity(); i++) {
			Argument arg = aggregateNode.getArgument(i);
			if (((arg instanceof Variable) && ((Variable)arg).deepDereference().isConstant())) {
				NodeArguments arguments = new NodeArguments(2);
				arguments.set(0, ((Variable)arg).deepDereference());
				arguments.set(1, arg);
				
				((Variable)arg).makeFree();

				Binding equalityBinding = new Binding(2);
				equalityBinding.setBinding(0, BindingType.BOUND);
				equalityBinding.setBinding(1, BindingType.FREE);
				equalityPredicateList.add(new EqualityMixedBindingNode(arguments, equalityBinding, new VariableList()));
			}
		}
		
		return (!equalityPredicateList.isEmpty());
	}

	private void addOrNodesToAndNode(OrNode orNode, NodeList<OrNode> orNodeList, AndNode andNode,
			VariableList boundVariableList, boolean addInFront) {
		this.deALSContext.logTrace(logger, "Entering addOrNodesToAndNode for {}", orNode.toStringNode());

		// regenerating the free variable list for the original node
		this.regenerateFreeVariableList(orNode, boundVariableList);
		Utilities.getVariables(orNode.getArguments(), boundVariableList);

		OrNode complexObjectEqualityOrNode;
		// regenerating the free variable list for the new complex object equality ornodes
		for (int i = orNodeList.size() - 1; i >= 0; i--) {
			complexObjectEqualityOrNode = orNodeList.get(i);
			this.regenerateFreeVariableList(complexObjectEqualityOrNode, boundVariableList);

			Utilities.getVariables(complexObjectEqualityOrNode.getArguments(), boundVariableList);
		}

		if (addInFront) {
			// if we are to add in front, we have add them in reverse order
			for (int i = 0; i < orNodeList.size(); i++)
				andNode.addChildAtHead(orNodeList.get(i));
			
			andNode.addChildAtHead(orNode);
		} else {
			andNode.addChild(orNode);

			for (int i = orNodeList.size() - 1; i >= 0; i--)
				andNode.addChild(orNodeList.get(i));			
		}

		this.deALSContext.logTrace(logger, "Exiting addOrNodesToAndNode for {}", orNode.toStringNode());
	}

	private void addOrNodesToAndNode(OrNode orNode, NodeList<OrNode> orNodeList, AndNode andNode,
			VariableList boundVariableList) {
		this.addOrNodesToAndNode(orNode, orNodeList, andNode, boundVariableList, false);
	}

	private Pair<Boolean, Argument> extractFreeFunctorFromTerm(Argument argument, 
			NodeList<OrNode> complexObjectEqualityList) {
		return extractFreeFunctorFromTerm(argument, complexObjectEqualityList, false);
	}

	private Pair<Boolean, Argument> extractFreeFunctorFromTerm(Argument argument, NodeList<OrNode> complexObjectEqualityList,
			boolean isTopLevel) {
		this.deALSContext.logTrace(logger, "Entering extractFreeFunctorFromTerm for {}", argument);

		boolean status = false;

		/*if (argument instanceof BinaryExpression) {
			BinaryExpression expr = (BinaryExpression)argument;
			if (isTopLevel) {
				Pair<OrNode, Argument> retvalPair = this.generateFunctorEqualityOrNode(expr, BindingType.BOUND, BindingType.FREE);
				OrNode functorEquality = retvalPair.getFirst();
				argument = retvalPair.getSecond();

				complexObjectEqualityList.add(functorEquality);
				status = true;
			}			
		} else */if (argument instanceof InterpreterFunctor) {
			InterpreterFunctor functor = (InterpreterFunctor) argument;

			if (isTopLevel && !functor.isBound()) {
				Argument args = functor.getArguments();

				Pair<Boolean, Argument> retvalPair = this.extractFreeFunctorFromTerm(args, complexObjectEqualityList);
				args = retvalPair.getSecond();

				Pair<OrNode, Argument> retvalPair2 = this.generateFunctorEqualityOrNode(argument, BindingType.BOUND, BindingType.FREE);
				OrNode functorEquality = retvalPair2.getFirst();
				argument = retvalPair2.getSecond();

				complexObjectEqualityList.add(functorEquality);
				status = true;
			}
		} else 	if (argument instanceof InterpreterList) {
			InterpreterList list = (InterpreterList) argument;

			if (isTopLevel && !list.isEmpty() && !list.isBound()) {
				Argument head = list.getHead();
				InterpreterList tail = list.getTail();

				Pair<Boolean, Argument> retvalPair = this.extractFreeFunctorFromTerm(head, complexObjectEqualityList);
				if (retvalPair.getFirst())
					list.setHead(retvalPair.getSecond());

				if (tail != null) {
					Pair<Boolean, Argument> retvalPair2 = this.extractFreeFunctorFromTerm(tail,complexObjectEqualityList);
					if (retvalPair2.getFirst())
						list.setTail((InterpreterList) retvalPair2.getSecond());
				}

				Pair<OrNode, Argument> retvalPair3 
					= this.generateFunctorEqualityOrNode(argument, BindingType.BOUND, BindingType.FREE);
				OrNode listEquality = retvalPair3.getFirst();
				argument = retvalPair3.getSecond();

				complexObjectEqualityList.add(listEquality);
				status = true;
			}
		}

		// case COMPILER_TYPE_LIST:
		this.deALSContext.logTrace(logger, "Exiting extractFreeComplexObjectFromTerm with status = {}", status);

		return new Pair<>(status, argument);
	}

	private Pair<OrNode, Argument> generateFunctorEqualityOrNode(Argument functorTerm, 
			BindingType variableBinding, BindingType functorBinding) {
		this.deALSContext.logTrace(logger, "Entering generateFunctorEqualityOrNode for {}", functorTerm);

		Variable variable = ProgramGenerator.generateUniqueVariable();
		if (functorTerm.getDataType() == DataType.UNKNOWN)
			variable.setDataType(DataType.COMPLEX);
		else
			variable.setDataType(functorTerm.getDataType());

		NodeArguments arguments = new NodeArguments(2);
		arguments.set(0, variable);
		arguments.set(1, functorTerm);

		OrNode functorEqualityOrNode;
		VariableList freeVariableList = new VariableList();
		Binding equalityBinding = new Binding(2);
		equalityBinding.setBinding(0, variableBinding);
		equalityBinding.setBinding(1, functorBinding);

		functorEqualityOrNode = new EqualityMixedBindingNode(arguments, equalityBinding, freeVariableList);

		this.deALSContext.logTrace(logger, "Exiting generateComplexObjectEqualityOrNode with {} and {}",
					functorEqualityOrNode, variable);

		return new Pair<OrNode, Argument>(functorEqualityOrNode, variable);
	}

	private XYCliqueNode generateXYClique(PCGOrNode pcgOrNode, XYClique clique,
			NodeList<IClique> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering generateXYClique for {}", pcgOrNode);

		XYCliqueNode xyClique = null;
		String predicateName = pcgOrNode.getPredicateName();
		int arity = pcgOrNode.getArity();
		Binding searchBinding = pcgOrNode.getBindingPattern();
		XYCliquePredicate cliquePredicate;

		Pair<XYCliquePredicate, Integer> retvalPair = clique.getCliquePredicate(predicateName, arity, searchBinding);
		int index = retvalPair.getSecond();

		if ((cliquePredicate = retvalPair.getFirst()) == null) {
			// since cliquePredicate with that binding does not exist, we know there is a catch-all
			// binding pattern i.e. all-free in performing a semi-naive fixpoint
			// In the future, we can more intelligent by choosing the clique predicate with the most
			// restricted binding that satisfy this binding

			searchBinding = new Binding(arity, BindingType.FREE);

			retvalPair = clique.getCliquePredicate(predicateName, arity, searchBinding);
			cliquePredicate = retvalPair.getFirst();
			index = retvalPair.getSecond();
		}

		if (cliquePredicate != null) {
			// for generating recursive_literal. if a clique or mutualClique is
			// put on this stack, then recursive literal is generated.
			NodeStack<IClique> localCliqueStack = new NodeStack<>();
			// for sharing clique
			NodeList<IClique> newLocalSharableCliqueList = new NodeList<>();
			// for sharing mutualClique
			NodeList<IMutualClique> localSharableMutualCliqueList = new NodeList<>();
			
			// if a mutualClique is put on this stack, then the mutualClique can be shared
			NodeList<IMutualClique> localMutualCliqueList = new NodeList<>();
			// for storing in clique so that
			NodeList<RecursiveLiteral> localRecursiveLiteralList = new NodeList<>();

			// Determine whether it is linear or non-linear, we do it once and use the result later
			clique.determineRecursiveType();

			// make all external cliques sharable to inside this clique.
			// and we also make those internal cliques sharable to outside this clique
			if (pcgOrNode.isSharableClique())
				newLocalSharableCliqueList = localSharableCliqueList;

			xyClique = (XYCliqueNode) this.createClique(pcgOrNode, searchBinding, CliqueType.XY_CLIQUE);

			xyClique.orderedCliqueIndex = index;

			localCliqueStack.push(xyClique);
			this.generateXYCliqueOrNodes(pcgOrNode, clique, xyClique, localCliqueStack, 
					newLocalSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList,
					localRecursiveLiteralList);

			localCliqueStack.pop();

			if (this.deALSContext.isDebugEnabled()) {
				this.deALSContext.logDebug(logger, "localRecursiveLiteralList ==");
				for (int i = 0; i < localRecursiveLiteralList.size(); i++)
					this.deALSContext.logDebug(logger, "i = {} : ", i, localRecursiveLiteralList.get(i).toStringNode());
			}

			xyClique.setRecursiveLiteralList(localRecursiveLiteralList);
			xyClique.setMutualCliqueList(localMutualCliqueList);

			// we clear this list if it is not sharable
			if (!pcgOrNode.isSharableClique())
				newLocalSharableCliqueList.clear();

			localSharableMutualCliqueList.clear();
			localCliqueStack.clear();
		}

		this.deALSContext.logTrace(logger, "Exiting generateXYClique with {}", xyClique.toStringNode());

		return xyClique;
	}

	private void generateXYCliqueOrNodes(PCGOrNode pcgOrNode, XYClique clique, 
			XYCliqueNode xyClique, NodeStack<IClique> localCliqueStack,
			NodeList<IClique> localSharableCliqueList, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateXYCliqueExitRecursiveOrNode with {}", pcgOrNode);

		XYCliquePredicate 	cliquePredicate;
		String 				predicateName 	= pcgOrNode.getPredicateName();
		int 				arity			= pcgOrNode.getArity();
		Binding 			searchBinding 	= pcgOrNode.getBindingPattern();
		OrNode[] 			yorNode 		= new OrNode[3];

		Pair<XYCliquePredicate, Integer> retvalPair = clique.getCliquePredicate(predicateName, arity, searchBinding);

		if ((cliquePredicate = retvalPair.getFirst()) == null) {
			// since cliquePredicate with that binding does not exist, we know there is a catch-all binding 
			// pattern i.e. all-free in performing a semi-naive fixpoint. In the future, we can more
			// intelligent by choosing the clique predicate with the most restricted binding that satisfy this binding
			searchBinding = new Binding(arity, BindingType.FREE);
			retvalPair = clique.getCliquePredicate(predicateName, arity, searchBinding);
			cliquePredicate = retvalPair.getFirst();
		}

		if (cliquePredicate != null) {
			OrNode cliqueExitOrNode = this.createRecursiveDummyOrNode(pcgOrNode, searchBinding);
			OrNode cliqueRecursiveOrNode = this.createRecursiveDummyOrNode(pcgOrNode, searchBinding);
			if ((cliqueExitOrNode != null) && this.unifyWithClique(cliqueExitOrNode, xyClique))
				xyClique.addExitRulesOrNode(cliqueExitOrNode);

			if ((cliqueRecursiveOrNode != null)
					&& this.unifyWithClique(cliqueRecursiveOrNode, xyClique))
				xyClique.addXRulesOrNode(cliqueRecursiveOrNode);

			// Generate magic exit rule
			this.generateMagicExitRule(pcgOrNode, cliqueExitOrNode);
			AndNode andNode;

			for (PCGAndNode pcgAndNode : cliquePredicate.getExitRules()) {
				andNode = this.generateCliqueAndNode(pcgAndNode, clique,
						cliqueExitOrNode, localCliqueStack, localSharableCliqueList, 
						localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);

				if (andNode != null)
					cliqueExitOrNode.addChild(andNode);
			}

			for (PCGAndNode pcgAndNode : cliquePredicate.getXRules()) {
				andNode = this.generateCliqueAndNode(pcgAndNode, clique,
						cliqueRecursiveOrNode, localCliqueStack, localSharableCliqueList, 
						localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);

				if (andNode != null)
					cliqueRecursiveOrNode.addChild(andNode);
			}

			/************************************************************
			 * XY CLIQUE Y RULE
			 ************************************************************/
			yorNode[CliqueRuleType.Y_RULE.getId()] = this.createRecursiveDummyOrNode(pcgOrNode, searchBinding);
			if ((yorNode[CliqueRuleType.Y_RULE.getId()] != null)
					&& this.unifyWithClique(yorNode[CliqueRuleType.Y_RULE.getId()], xyClique))
				xyClique.addYRulesOrNode(yorNode[CliqueRuleType.Y_RULE.getId()]);

			for (PCGAndNode pcgAndNode : cliquePredicate.getYRules()) {
				andNode = this.generateCliqueAndNode(pcgAndNode, clique, yorNode[CliqueRuleType.Y_RULE.getId()],
						localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);

				if (andNode != null)
					yorNode[CliqueRuleType.Y_RULE.getId()].addChild(andNode);
			}

			/************************************************************
			 * XY CLIQUE COPY RULE
			 ************************************************************/
			yorNode[CliqueRuleType.COPY_RULE.getId()] = this.createRecursiveDummyOrNode(pcgOrNode, searchBinding);
			xyClique.addCopyRulesOrNode(yorNode[CliqueRuleType.COPY_RULE.getId()]);

			for (PCGAndNode pcgAndNode : cliquePredicate.getCopyRules()) {
				andNode = this.generateCliqueAndNode(pcgAndNode, clique, yorNode[CliqueRuleType.COPY_RULE.getId()],
						localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);

				if (andNode != null)
					yorNode[CliqueRuleType.COPY_RULE.getId()].addChild(andNode);
			}

			/************************************************************
			 * XY CLIQUE DELETE RULE
			 ************************************************************/
			yorNode[CliqueRuleType.DELETE_RULE.getId()] = this.createRecursiveDummyOrNode(pcgOrNode, searchBinding);
			xyClique.addDeleteRulesOrNode(yorNode[CliqueRuleType.DELETE_RULE.getId()]);

			for (PCGAndNode pcgAndNode : cliquePredicate.getDeleteRules()) {
				andNode = this.generateCliqueAndNode(pcgAndNode, clique, yorNode[CliqueRuleType.DELETE_RULE.getId()],
						localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList,
						localRecursiveLiteralList);

				if (andNode != null)
					yorNode[CliqueRuleType.DELETE_RULE.getId()].addChild(andNode);
			}
		} else {
			this.deALSContext.logError(logger, "Clique predicate can not be found in clique");
			throw new ProgramGeneratorException("Clique predicate can not be found in clique");
		}

		this.deALSContext.logTrace(logger, "Exiting generateXYCliqueExitRecursiveOrNode with {}", xyClique.toStringNode());
	}

	private OrNode generateXYCliqueOrNode(PCGOrNode pcgOrNode, CliqueBase clique, VariableMappings variableMappings,
			VariableList boundVariableList, NodeStack<IClique> localCliqueStack,
			NodeList<IClique> localSharableCliqueList, NodeList<IMutualClique> localSharableMutualCliqueList,
			NodeList<IMutualClique> localMutualCliqueList, NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateXYCliqueOrNode for {}", pcgOrNode);

		CliqueBaseNode 		generatedClique 	= null;
		OrNode 				orNode 				= null;
		String 				predicateName 		= pcgOrNode.getPredicateName();
		int 				arity 				= pcgOrNode.getArity();
		Binding 			searchBinding 		= pcgOrNode.getBindingPattern();
		XYCliquePredicate 	cliquePredicate;

		Pair<XYCliquePredicate, Integer> retvalPair = ((XYClique) clique).getCliquePredicate(predicateName, arity, searchBinding);

		if ((cliquePredicate = retvalPair.getFirst()) == null) {
			Binding allFreeBinding = new Binding(arity, BindingType.FREE);

			retvalPair = ((XYClique) clique).getCliquePredicate(predicateName, arity, allFreeBinding);
			cliquePredicate = retvalPair.getFirst();

			if (cliquePredicate == null)
				throw new ProgramGeneratorException("All-free catch-all clique predicate can not be found in clique");

			generatedClique = (CliqueBaseNode) this.findFromNodeList(predicateName, arity, allFreeBinding, localCliqueStack);
		} else {
			generatedClique = (CliqueBaseNode) this.findFromNodeList(predicateName, arity, searchBinding, localCliqueStack);
		}

		if (generatedClique != null)
			orNode = this.createRecursiveLiteral(pcgOrNode, variableMappings, boundVariableList, clique, generatedClique,
					localRecursiveLiteralList);
		else
			orNode = this.generateMutualRecursiveOrNode(pcgOrNode, clique, variableMappings, boundVariableList, localCliqueStack,
					localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);

		if (orNode != null) {
			// Propagating new/old xy literal mark from pcg node
			orNode.setXYPredicateType(pcgOrNode.getPredicate().getXYPredicateType());
			/* Negation is allowed within XY clique. Generate extra node for
			 * negation. Non-recursive negation node is generated in generateOrNode(). */
			if (pcgOrNode.getPredicate().isNegative())
				orNode = new NegationOrNode(orNode);
		}

		this.deALSContext.logTrace(logger, "Exiting generateXYCliqueOrNode with {}", orNode);

		return orNode;
	}

	private MutualXYCliqueNode generateMutualXYClique(PCGOrNode pcgOrNode, XYClique clique, 
			NodeStack<IClique> localCliqueStack, NodeList<IClique> localSharableCliqueList,
			NodeList<IMutualClique> localSharableMutualCliqueList, NodeList<IMutualClique> localMutualCliqueList,
			NodeList<RecursiveLiteral> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateMutualXYClique for {}", pcgOrNode);

		MutualXYCliqueNode 	mutualXYClique 	= null;
		String 				predicateName 	= pcgOrNode.getPredicateName();
		int 				arity 			= pcgOrNode.getArity();
		Binding 			searchBinding 	= pcgOrNode.getBindingPattern();
		XYCliquePredicate 	cliquePredicate = null;

		Pair<XYCliquePredicate, Integer> retvalPair = clique.getCliquePredicate(predicateName, arity, searchBinding);
		int index = retvalPair.getSecond();

		if ((cliquePredicate = retvalPair.getFirst()) == null) {
			// since cliquePredicate with that binding does not exist, we know
			// there is a catch-all binding pattern i.e. all-free in performing a semi-naive fixpoint
			// In the future, we can more intelligent by choosing the clique
			// predicate with the most restricted binding that satisfy this binding
			searchBinding = new Binding(arity, BindingType.FREE);

			retvalPair = clique.getCliquePredicate(predicateName, arity, searchBinding);
			cliquePredicate = retvalPair.getFirst();
			index = retvalPair.getSecond();
		}

		if (cliquePredicate != null) {
			mutualXYClique = (MutualXYCliqueNode) this.createClique(pcgOrNode,
					searchBinding, CliqueType.MUTUAL_XY_CLIQUE);

			mutualXYClique.orderedCliqueIndex = index;

			localCliqueStack.push(mutualXYClique);

			this.generateXYCliqueOrNodes(pcgOrNode, clique,
					mutualXYClique, localCliqueStack, localSharableCliqueList,
					localSharableMutualCliqueList, localMutualCliqueList,
					localRecursiveLiteralList);

			localCliqueStack.pop();

			localSharableMutualCliqueList.clear();
		}

		this.deALSContext.logTrace(logger, "Exiting generateMutualXYClique with {}", mutualXYClique.toStringNode());

		return mutualXYClique;
	}

	/*************************************************************
	 * unifying Node with Clique or Mutual Clique - we can assume that all
	 * arguments are free unique variables in node since a Clique is used to
	 * unify with a dummy clique_exit_or_node or clique_recursive_or_node. Thus,
	 * no special equality rewriting is necessary
	 **************************************************************/
	private boolean unifyWithClique(Node<?> node, CliqueBaseNode clique) {
		this.deALSContext.logTrace(logger, "Entering unifyWithClique node = {} clique = {}", node, clique.toStringNode());

		boolean status = false;

		if (node.getPredicateName().equals(clique.getPredicateName())
				&& node.getArity() == clique.getArity())
			status = Unifier.unifyTerms(this.deALSContext, node.getArguments(), clique.getArguments());

		this.deALSContext.logTrace(logger, "Exiting unifyWithClique with status = {}", status);

		return status;
	}

	private boolean unifyAndOrNodes(AndNode andNode, OrNode orNode,
			NodeList<OrNode> frontEqualityList, NodeList<OrNode> backEqualityList) {
		if (orNode == null)
			this.deALSContext.logTrace(logger, "Entering unifyAndOrNodes AndNode = {} OrNode = null", andNode);
		else
			this.deALSContext.logTrace(logger, "Entering unifyAndOrNodes AndNode = {} OrNode = {}", andNode, orNode);

		boolean status = false;

		if (orNode == null) {
			status = true;
		} else if (andNode.getArity() == orNode.getArity()) {
			// This uniqueVariableList keeps track of the variables in the toTerms
			// such that if there is repeat occurrence of variable in the andNode,
			// we generate equality with temporary variable
			NodeArguments uniqueVariableList = new NodeArguments();

			NodeArguments toTerms = andNode.getArguments();
			NodeArguments fromTerms = orNode.getArguments();

			NodeArguments fromTermList = new NodeArguments();
			OrNode equalityOrNode;

			Argument toTerm, fromTerm;

			status = true;

			for (int i = 0; i < toTerms.size(); i++) {
				equalityOrNode = null;
				toTerm = toTerms.get(i);
				fromTerm = fromTerms.get(i);
				if ((fromTerm instanceof Variable) && ((Variable)fromTerm).isAnonymous())
					continue;

				Triple<Boolean, OrNode, Argument> retvalTriple 
					= this.unifyTopTerm(toTerm, fromTerm, andNode.getBinding(i), uniqueVariableList);
				status = retvalTriple.getFirst();
				if (!status)
					break;

				equalityOrNode = retvalTriple.getSecond();
				toTerm = retvalTriple.getThird();

				if (equalityOrNode != null)
					this.unifyAndOrNodes2(andNode, frontEqualityList,
							backEqualityList, toTerm, fromTerm, toTerms,
							fromTermList, i, equalityOrNode);

				// We have to remember all the free from term we have come across so far
				if (andNode.getBinding(i) == BindingType.FREE)
					fromTermList.add(fromTerm);
			}

			if (!status) {
				frontEqualityList.clear();
				backEqualityList.clear();
			}
		}

		this.deALSContext.logTrace(logger, "Exiting unifyAndOrNodes with status = {}", status);

		return status;
	}

	private void unifyAndOrNodes2(AndNode andNode, NodeList<OrNode> frontEqualityList, 
			NodeList<OrNode> backEqualityList, Argument toTerm, Argument fromTerm, NodeArguments toTermList, 
			NodeArguments fromTermList, int toTermIndex, OrNode equalityOrNode) {

		this.deALSContext.logTrace(logger, "Entering unifyAndOrNodes2");

		if (toTerm instanceof Variable) {
			Variable tempVariable = (Variable) toTerm;

			// Short-circuiting the fromTerm, making sure that the toTerm is pointing
			// the last element at the end of the list
			if ((fromTerm instanceof Variable) && ((Variable) fromTerm).isBound())
				tempVariable.setValue(((Variable) fromTerm).deepDereference());
			else
				tempVariable.setValue(fromTerm);

			toTermList.set(toTermIndex, toTerm);

			// If the argument is originally bound, then the temporary variable should be bound
			// and the equality generated should be put in front. Otherwise, we have the converse.
			if (andNode.getBinding(toTermIndex) == BindingType.BOUND) {
				frontEqualityList.add(equalityOrNode);
			} else if (andNode.getBinding(toTermIndex) == BindingType.FREE) {
				// If this free variable is pointing to some from terms currently
				// pointed to by some previous free variables, then it should be bound
				if (tempVariable.isBound() && fromTermList.contains(tempVariable.getValue())) {
					NodeArguments arguments = new NodeArguments(2);
					VariableList freeVariableList = new VariableList();
					Binding equalityBinding = new Binding(2);

					arguments.set(0, equalityOrNode.getArgument(0));
					arguments.set(1, equalityOrNode.getArgument(1));
					equalityOrNode.getArguments().clear();

					equalityBinding.setBinding(0, BindingType.BOUND);
					equalityBinding.setBinding(1, BindingType.BOUND);
					equalityOrNode = new ComparisonAllBoundNode(BuiltInPredicate.EQUALITY_PREDICATE_NAME, arguments, freeVariableList);
				}

				backEqualityList.add(equalityOrNode);
			}
		} else {
			this.deALSContext.logError(logger, "Argument 1 of equality should be variable");
			throw new ProgramGeneratorException("Argument 1 of equality should be variable");
		}

		this.deALSContext.logTrace(logger, "Exiting unifyAndOrNodes2");
	}

	private Triple<Boolean, OrNode, Argument> unifyTopTerm(Argument toTerm, Argument fromTerm, BindingType binding,
			NodeArguments uniqueVariableList) {

		this.deALSContext.logTrace(logger, "Entering unifyTopTerm for toTerm = {}, fromTerm = {}", toTerm, fromTerm);

		boolean status = true;
		Argument value;
		OrNode equalityOrNode = null;

		// If the toTerm is a variable or input variable, we do assignment from toTerm from fromTerm
		if ((toTerm instanceof Variable) || (toTerm instanceof InputVariable)) {
			// if toTerm has occured before as a variable, we perform equality rewrite
			if (toTerm instanceof Variable) {
				// We ensure that the andNode has only unique variables unless
				// the fromTerm is exactly the same as the variable is currently pointing to
				// e.g. ... p(Y,Y), .... p(X,X) <- .... in this case, we do not need to rewrite with equality
				if (uniqueVariableList.contains(toTerm)) {
					if (toTerm.isBound()
							&& ((Variable) toTerm).getValue() != fromTerm) {
						Pair<OrNode, Argument> retvalPair = this.generateAndEquality(toTerm, binding);
						equalityOrNode = retvalPair.getFirst();
						toTerm = retvalPair.getSecond();
					}
				} else {
					uniqueVariableList.add(toTerm);
				}
			}

			// Short-circuiting the fromTerm, making sure that the toTerm is pointing to
			// the last element at the end of the list
			if ((fromTerm instanceof Variable) && ((Variable) fromTerm).isBound()) {
				value = ((Variable) fromTerm).deepDereference();

				if (toTerm instanceof Variable)
					((Variable) toTerm).setValue(value);
				else if (toTerm instanceof InputVariable)
					((InputVariable) toTerm).setValue(value);
			} else {
				if (toTerm instanceof Variable) {
					if (DataType.isNumeric(toTerm.getDataType()) && DataType.isNumeric(fromTerm.getDataType())) {
						if (toTerm.getDataType() == fromTerm.getDataType()) {
							((Variable) toTerm).setValue(fromTerm);
						} else {
							value = fromTerm.reduce();
							if (value instanceof DbTypeBase)
								((Variable) toTerm).setValue(DataType.cast((DbNumericType)value, toTerm.getDataType()));
							else
								((Variable) toTerm).setValue(value);							
						}
					} else {
						((Variable) toTerm).setValue(fromTerm);
					}
				} else if (toTerm instanceof InputVariable) {
					((InputVariable) toTerm).setValue(fromTerm);
				}
			}
		} else {
			if ((fromTerm instanceof Variable)
					&& ((Variable) fromTerm).isBound())
				value = ((Variable) fromTerm).deepDereference();
			else
				value = fromTerm;

			// We can unify value if both of them are bound excluding inputvariables
			if (toTerm.isGround() && value.isGround()
					&& (!(toTerm instanceof InputVariable))
					&& (!(value instanceof InputVariable))) {
				status = Unifier.unifyValue(this.deALSContext, toTerm, value);
			} else {
				Pair<OrNode, Argument> retvalPair = this.generateAndEquality(toTerm, binding);
				equalityOrNode = retvalPair.getFirst();
				toTerm = retvalPair.getSecond();				
			}
		}

		this.deALSContext.logTrace(logger, "Exiting unifyTopTerm for toTerm = {}, fromTerm = {} with status = {}",
				toTerm, fromTerm, status);

		return new Triple<>(status, equalityOrNode, toTerm);
	}

	private Pair<OrNode, Argument> generateAndEquality(Argument toTerm, BindingType binding) {
		this.deALSContext.logTrace(logger, "Entering generateAndEquality for toTerm = {}, binding = {}", toTerm, binding);

		Variable var = ProgramGenerator.generateUniqueVariable();
		var.setDataType(toTerm.getDataType());
		VariableList freeVariableList = new VariableList();
		OrNode equalityOrNode = null;
		Binding equalityBinding = new Binding(2);
		NodeArguments arguments = new NodeArguments(2);

		arguments.set(0, var);
		arguments.set(1, toTerm);

		if (binding == BindingType.BOUND) {
			Utilities.getVariables(toTerm, freeVariableList);

			equalityBinding.setBinding(0, BindingType.BOUND);

			boolean isBound = false;
			if (toTerm instanceof Variable)
				isBound = ((Variable) toTerm).hasValueAssigned();
			else
				isBound = toTerm.isBound();
			
			if (isBound) {
				equalityBinding.setBinding(1, BindingType.BOUND);
				equalityOrNode = new ComparisonAllBoundNode(BuiltInPredicate.EQUALITY_PREDICATE_NAME, arguments, freeVariableList);
			} else {
				equalityBinding.setBinding(1, BindingType.FREE);
				equalityOrNode = new EqualityMixedBindingNode(arguments, equalityBinding, freeVariableList);
			}
		} else if (binding == BindingType.FREE) {
			freeVariableList.add(var);
			equalityBinding.setBinding(0, BindingType.FREE);
			equalityBinding.setBinding(1, BindingType.BOUND);

			equalityOrNode = new EqualityMixedBindingNode(arguments, equalityBinding, freeVariableList);
		} else {
			this.deALSContext.logError(logger, "AndNode has incorrect binding");
			throw new ProgramGeneratorException("AndNode has incorrect binding");
		}

		toTerm = var;

		this.deALSContext.logTrace(logger, "Exiting generateAndEquality with {}", equalityOrNode);

		return new Pair<>(equalityOrNode, toTerm);
	}

	private NodeArguments generateArguments(CompilerTypeList pcgNodeArguments, VariableMappings variableMappings) {
		NodeArguments nodeArguments = new NodeArguments(pcgNodeArguments.size());
		for (int i = 0; i < pcgNodeArguments.size(); i++)
			nodeArguments.set(i,  Utilities.convertToArgument(pcgNodeArguments.get(i), variableMappings, this.deALSContext.getDatabase().getTypeManager()));
		
		return nodeArguments;
	}
	
	public static void compressVariableAssignments(OrNode orNode) {
		orNode.getArguments().compressVariableAssignments(false);

		ProgramGenerator.compressVariableList(orNode.getFreeVariableList());

		if (orNode.hasChildren())
			for (AndNode rule : orNode.getChildren())
				ProgramGenerator.compressVariableAssignments(rule);

		if (orNode instanceof RecursiveOrNode) {
			CliqueBaseNode clique = ((RecursiveOrNode) orNode).getClique();

			// compress the exit rules
			ProgramGenerator.compressVariableAssignments(clique.getExitRulesOrNode());

			if (clique instanceof CliqueNode) {
				// compress the recursive rules
				ProgramGenerator.compressVariableAssignments(((CliqueNode) clique).getRecursiveRulesOrNode());
			} else if (clique instanceof XYCliqueNode) {
				XYCliqueNode xyClique = (XYCliqueNode) clique;
				// compress each type of xy rule
				ProgramGenerator.compressVariableAssignments(xyClique.getXRulesOrNode());

				ProgramGenerator.compressVariableAssignments(xyClique.getYRulesOrNode());

				ProgramGenerator.compressVariableAssignments(xyClique.getCopyRulesOrNode());

				ProgramGenerator.compressVariableAssignments(xyClique.getDeleteRulesOrNode());
			}
		} else if (orNode instanceof MaterializedPredicate) {
			ProgramGenerator.compressVariableAssignments(((MaterializedPredicate) orNode).getMaterializedRule());
		} else if (orNode instanceof NegationOrNode) {
			ProgramGenerator.compressVariableAssignments(((NegationOrNode) orNode).getLiteralToBeNegated());
		} else if (orNode instanceof ReadOnlyRelationNode) {
			ProgramGenerator.compressVariableAssignments(((ReadOnlyRelationNode) orNode));
		}
	}

	private static void compressVariableAssignments(AndNode andNode) {
		andNode.getArguments().compressVariableAssignments(false);
		if (andNode.hasChildren())
			for (OrNode literal : andNode.getChildren())
				ProgramGenerator.compressVariableAssignments(literal);
	}

	private static void compressVariableList(VariableList variableList) {
		if (variableList == null)
			return;

		for (int i = variableList.size() - 1; i >= 0; i--) {
			Variable variable = variableList.get(i);
			Argument deferenced = variable.deepDereference();
			if ((deferenced != variable)
					&& (!(deferenced instanceof InputVariable))
					&& (!(deferenced instanceof InterpreterList))) {
				variableList.set(i, (Variable) variable.deepDereference());
			}
		}
	}
	
	private void expandSugaredArguments(OrNode orNode) {
		if (orNode instanceof XYRecursiveOrNode) {
			XYCliqueNode clique = (XYCliqueNode)((XYRecursiveOrNode)orNode).getClique();
			this.expandSugaredArguments(clique.getExitRulesOrNode());
			this.expandSugaredArguments(clique.getXRulesOrNode());
			this.expandSugaredArguments(clique.getYRulesOrNode());
			this.expandSugaredArguments(clique.getCopyRulesOrNode());
			this.expandSugaredArguments(clique.getDeleteRulesOrNode());
		} else if (orNode instanceof RecursiveOrNode) {
			CliqueNode clique = (CliqueNode)((RecursiveOrNode)orNode).getClique();
			this.expandSugaredArguments(clique.getExitRulesOrNode());
			this.expandSugaredArguments(clique.getRecursiveRulesOrNode());
		} else {			
			for (int i = orNode.getNumberOfChildren() - 1; i >= 0; i--)
				this.expandSugaredArguments(orNode.getChild(i));			
		}
	}
	
	private void expandSugaredArguments(AndNode andNode) {
		for (int i = andNode.getNumberOfChildren() - 1; i >= 0; i--) {
			OrNode child = andNode.getChild(i);
			if (!(child instanceof ComparisonNode)) {
				for (int j = 0; j < child.getArity(); j++) {
					if (child.getArgument(j) instanceof Expression) {
						Expression expr = (Expression) child.getArgument(j);
						Pair<OrNode, Argument> retvalPair = this.generateFunctorEqualityOrNode(expr, BindingType.FREE, BindingType.BOUND); 
						EqualityMixedBindingNode orNode = (EqualityMixedBindingNode) retvalPair.getFirst();
						andNode.insertChild(orNode, i);
							
						child.getArguments().set(j, retvalPair.getSecond());
					}
				}
			}
			this.expandSugaredArguments(child);
		}
	}
	
	private void attachContext(Node<?> node) {
		node.attachContext(this.deALSContext);
		for (Node<?> child : node.getChildren())
			attachContext(child);
			
		if (node instanceof XYCliqueNode) {
			XYCliqueNode clique = (XYCliqueNode)node;
			attachContext(clique.getExitRulesOrNode());
			attachContext(clique.getXRulesOrNode());
			attachContext(clique.getYRulesOrNode());
			attachContext(clique.getDeleteRulesOrNode());
			attachContext(clique.getCopyRulesOrNode());
		} else if (node instanceof RecursiveOrNode) {
			RecursiveOrNode ron = (RecursiveOrNode)node;
			attachContext(ron.getClique());
		}		
	}	
			
	public static ProgramGenerationResult generateProgram(DeALSContext deALSContext, CompilationResult compilationResult) {
		QueryForm queryForm = compilationResult.getQueryForm();
		ProgramGenerator programGenerator = new ProgramGenerator(deALSContext);
		String message;

		// STEP 7 - generate program for the Relevant PCG
		PipelinedProgram program = programGenerator.doGenerateProgram(compilationResult);
		if (program.isValid()) {
			queryForm.setProgram(program);

			deALSContext.logInfo(logger, "[BEGIN Step 8 - Generated Program for Query Form '{}' BEGIN]{}", compilationResult.getQueryForm(), program);
			deALSContext.logInfo(logger, "[END Step 8 - Generated Program for Query Form '{}' END]\n", compilationResult.getQueryForm());
			
			ProgramGenerator.compressVariableAssignments(program.getRoot());

			deALSContext.logInfo(logger, "[BEGIN Step 9 - Compress Variable Assignments for Query Form '{}' BEGIN]{}", queryForm, program);
			deALSContext.logInfo(logger, "[END Step 9 - Compress Variable Assignments for Query Form '{}' END]\n", queryForm);
			
			if (deALSContext.getConfiguration().compareProperty("deals.interpreter.optimize", "true"))
				new Optimizer(deALSContext).optimize(queryForm, program);
	
			message = compilationResult.getMessage() + "\nCompilation complete.";
		} else {
			deALSContext.logInfo(logger, "[BEGIN Step 8 - Generated Program for Query Form '{}' BEGIN]", compilationResult.getQueryForm());
			deALSContext.logInfo(logger, "NO PROGRAM GENERATED!");
			deALSContext.logInfo(logger, "[END Step 8 - Generated Program for Query Form '{}' END]\n", compilationResult.getQueryForm());
			
			deALSContext.logError(logger, "No program generated for query form");
			throw new CompilerException("No program generated for query form");
		}
		
		program.getRoot().attachContext(deALSContext);

		//if (compilationResult.getCompiledProgram() != null)
		//	compilationResult.getCompiledProgram().delete();

		return new ProgramGenerationResult(queryForm, message);
	}
}