package edu.ucla.cs.wis.bigdatalog.interpreter.relational;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.BuiltInAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.compilation.CompilationResult;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.Rewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerString;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerInt;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeInferrer;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.ProgramGeneratorException;
import edu.ucla.cs.wis.bigdatalog.interpreter.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;
import edu.ucla.cs.wis.bigdatalog.interpreter.OperatorProgram;
import edu.ucla.cs.wis.bigdatalog.interpreter.ProgramGenerationResult;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.Utilities;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterFunctor;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InterpreterList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.Expression;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive.CliqueType;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AggregateArgument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AliasedArgument;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AliasedVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.ComparisonExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.JoinConditionExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.OperatorArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.VariableMappings;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.CliqueOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.FilterOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.JoinOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.NegationOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.Operator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.OperatorType;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.ProjectionOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.SortOperator;
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator.UnionOperator;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class ProgramGenerator {
	private static Logger logger = LoggerFactory.getLogger(ProgramGenerator.class.getName());

	private static String UNIQUE_VARIABLE_NAME_PREFIX = "Var_";
	protected static int 			uniqueVariableCounter;
	
	protected	DeALSContext 		deALSContext;	
	protected 	TypeManager			typeManager;
	protected Stack<PCGOrNode> 		globalPCGOrNodeStack;
	protected List<Operator>		usedRelations;
	
	public ProgramGenerator(DeALSContext deALSContext) { 
		this.deALSContext = deALSContext;
		this.typeManager = deALSContext.getDatabase().getTypeManager();
		this.usedRelations = new ArrayList<>();
	}

	private OperatorProgram doGenerateProgram(CompilationResult compilationResult) {
		PCGOrNode program = compilationResult.getCompiledProgram();
		logger.info("Compiled PCG Tree:" + program.toStringTree());

		PCGOrNode queryFormRootOrNode = compilationResult.getCompiledProgram();
		ProgramGenerator.assignInputValues(queryFormRootOrNode);
		
		ProgramGenerator.uniqueVariableCounter = 1;
		this.globalPCGOrNodeStack = new Stack<>(); // for generating magic exit rule

		VariableMappings variableMappings = new VariableMappings();
		List<Operator> localSharableCliqueList = new ArrayList<>();
		Stack<Operator> localCliqueStack = new Stack<>();
		List<Operator> localSharableMutualCliqueList = new ArrayList<>();
		List<Operator> localMutualCliqueList = new ArrayList<>();
		List<Operator> localRecursiveLiteralList = new ArrayList<>();
		Operator rootOperator = this.generateOperator(null, queryFormRootOrNode, variableMappings, null,
				localSharableCliqueList, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);

		rootOperator = ProgramGenerator.handleNoArgCase(rootOperator, compilationResult.getQueryForm(), deALSContext);
		ProgramGenerator.renameOperators(rootOperator);

		this.globalPCGOrNodeStack.clear();
		localSharableCliqueList.clear();

//		this.deALSContext.logInfo(logger, "[BEGIN Step 8a - Generate Initial Program for Query Form '{}' BEGIN]{}", compilationResult.getCompiledProgram(), rootOperator.toStringTree());
//		this.deALSContext.logInfo(logger, "[END Step 8a - Generate Initial Program for Query Form '{}' END]\n", compilationResult.getCompiledProgram());
//
//		rootOperator = this.optimize(compilationResult.getQueryForm(), rootOperator);
//
//		ProgramGenerator.adjustOperatorNames(rootOperator);
//
//		this.synchronizeProgramWithQueryForm(compilationResult.getQueryForm(), queryFormRootOrNode, rootOperator, variableMappings);
//
//		this.deALSContext.logInfo(logger, "[BEGIN Step 8b - Optimize Operator Program for Query Form '{}' BEGIN]{}", compilationResult.getCompiledProgram(), rootOperator.toStringTree());
//		this.deALSContext.logInfo(logger, "[END Step 8b - Optimize Operator Program for Query Form '{}' END]\n", compilationResult.getCompiledProgram());
		
		this.deALSContext.logTrace(logger, "Exiting doGenerateProgram with {}", rootOperator);

		return new OperatorProgram(rootOperator, compilationResult.getProgramRules());
	}
		
	private Operator generateOperator(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings, CliqueBase clique, 
			List<Operator> localSharableCliqueList, Stack<Operator> localCliqueStack, List<Operator> localSharableMutualCliqueList, 
			List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateOperator for {}", pcgOrNode);

		Operator operator = null;
		
		switch (pcgOrNode.getPredicateType()) {
			case BUILT_IN:
				//operator = this.generateBuiltInOperator(pcgAndNode, pcgOrNode, variableMappings, clique, localSharableCliqueList,
				//		localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
				switch (pcgOrNode.getBuiltInPredicateType()) {
					case BINARY:
						operator = this.createBinaryOperator(pcgOrNode, variableMappings);
						break;
			
					case AGGREGATE:
						operator = this.generateAggregateOperator(pcgAndNode, pcgOrNode, variableMappings, clique, localSharableCliqueList, 
								localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
						break;
						
					case AGGREGATE_FS:
						operator = this.generateFSAggregateOperator(pcgAndNode, pcgOrNode, variableMappings, clique, localSharableCliqueList, 
								localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
						break;
					
					case SORT:
						operator = this.generateSortOperator(pcgOrNode, variableMappings, localSharableCliqueList, clique, 
								localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
						break;
					
					default: {
						this.deALSContext.logError(logger, "Unknown built-in type in program generator");
						throw new ProgramGeneratorException("Unknown built-in type in program generator");
					}
				}
			break;

			case BASE:
				operator = this.generateBaseRelationOperator(pcgAndNode, pcgOrNode, variableMappings);
			break;
	
			case DERIVED:
				if (pcgOrNode.isRecursive()) {
					// operator = this.generateRecursiveOperator(pcgAndNode, pcgOrNode, variableMappings, localSharableCliqueList);
					// victor: use the more recent implemented generateRecursiveOperator which takes care of mutual recursion
					operator = this.generateRecursiveOperator(pcgOrNode, (Clique) pcgOrNode.getBaseClique(), variableMappings,
							localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
				} else {
					operator = this.generateNonRecursiveOperator(pcgAndNode, pcgOrNode, variableMappings,
							localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
				}
			break;
		}
		
		if (operator != null)
			operator.setPCGNode(pcgAndNode);

		this.deALSContext.logTrace(logger, "Exiting generateOperator with {}", operator);

		return operator;
	}
	
	private Pair<Operator, VariableMappings> generateNegationOperator(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings,
			List<Operator> localSharableCliqueList, CliqueBase clique, Stack<Operator> localCliqueStack, 
			List<Operator> localSharableMutualCliqueList, List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateNegativeOperator for {}", pcgOrNode);

		VariableMappings predicateVariableMappings = new VariableMappings();

		Operator operator = this.generateOperator(pcgAndNode, pcgOrNode, predicateVariableMappings, clique, localSharableCliqueList,  
				localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);

		if (operator != null) {
			NegationOperator negationOperator = new NegationOperator("negation");
			negationOperator.addChild(operator);			
			
			Map<CompilerTypeBase, Integer> variableToPredicateMap = getVariableToPredicateMap(pcgAndNode);
			
			for (PCGOrNode child : pcgAndNode.getChildren()) {
				if (child.getExecutionBindingPattern().allFree() || isMagicPredicate(child.getPredicateName()) || (child != pcgOrNode))
					continue;
				
				for (int j = 0; j < child.getArity(); j++) {
					if ((child.getExecutionBinding(j) == BindingType.BOUND) && (!child.isBuiltInPredicateType(BuiltInPredicateType.BINARY))) {
						CompilerTypeBase compilerArgument = pcgOrNode.getArgument(j);
						if (variableToPredicateMap.containsKey(compilerArgument)) {
							negationOperator.addCondition(ComparisonOperation.EQUALITY, 
									variableToPredicateMap.get(compilerArgument), 
									this.convertToArgument(compilerArgument, variableMappings),
									0,
									this.convertToArgument(compilerArgument, predicateVariableMappings));
						}
					}
				}
			}	
			variableMappings.merge(predicateVariableMappings);
			operator = negationOperator;
		}
		
		this.deALSContext.logTrace(logger, "Exiting generateNegativeOperator with {}", operator);

		return new Pair<>(operator, predicateVariableMappings);
	}

	private Operator generateOperators(PCGAndNode pcgAndNode, VariableMappings allVariableMappings, CliqueBase clique, 
			List<Operator> localSharableCliqueList, Stack<Operator> localCliqueStack,  List<Operator> localSharableMutualCliqueList, 
			List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {

		if (pcgAndNode.getNumberOfChildren() == 1) {
			VariableMappings vm = new VariableMappings();
			Operator operator = generateOperator(pcgAndNode, pcgAndNode.getChild(0), vm, clique, localSharableCliqueList, 
					localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
			allVariableMappings.merge(vm);
			return operator;
		}
		
		this.deALSContext.logTrace(logger, "Entering generateOperators for {}", pcgAndNode);
		
		Operator 			operator				= null;
		Operator 			childOperator 			= null;	
		JoinOperator 		joinOperator 			= new JoinOperator(pcgAndNode.getPredicateName());				
		PCGOrNode 			pcgOrNode 				= null;
		VariableMappings 	localVariableMappings 	= new VariableMappings();
		List<Operator>		assignmentOperators		= new ArrayList<>();
		List<Operator>		operators 				= new ArrayList<>();

		// APPLY FILTER - convert comparison operators into a filter and introduce it into query pipeline	
		// Pair<filter predicate, {upstream operator => {variables in filter predicate that it sets}}>
		List<Pair<Operator, Map<Operator, VariableList>>> filterPredicateToUpstreamOperatorsList = new LinkedList<>();
		Set<Operator> aboveJoinFilterExpressions = new HashSet<>();

		Map<CompilerTypeBase, Integer> variableToPredicateMap = getVariableToPredicateMap(pcgAndNode);		
		for (int i = 0; i < pcgAndNode.getNumberOfChildren(); i++) {
			VariableMappings literalVariableMappings = new VariableMappings();
			childOperator = null;
			pcgOrNode = pcgAndNode.getChild(i);
			if (pcgOrNode.isRecursiveLiteral(clique)) {
				childOperator = this.generateRecursiveOperator(pcgOrNode, (Clique) clique, literalVariableMappings, 
						localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, localMutualCliqueList, 
						localRecursiveLiteralList);
			} else {
				if (pcgOrNode.getPredicate().isNegative()) {
					Pair<Operator, VariableMappings> pair = this.generateNegationOperator(pcgAndNode, pcgOrNode, localVariableMappings, localSharableCliqueList, 
							clique, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
					childOperator = pair.getFirst();
					literalVariableMappings = pair.getSecond();
				} else if (ProgramGenerator.isMagicPredicate(pcgOrNode.getPredicateName())) {
					PCGOrNode magicNode = pcgOrNode;
					pcgOrNode = pcgAndNode.getChild(++i);
					childOperator = this.generateOperator(pcgAndNode, pcgOrNode, localVariableMappings, clique, localSharableCliqueList, 
							localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
					
					Operator magicOperator = this.generateOperator(pcgAndNode, magicNode, localVariableMappings, clique, localSharableCliqueList, 
							localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
					
					if (magicOperator.getOperatorType() == OperatorType.RECURSION) {
						JoinOperator magicJoinOperator = new JoinOperator("magic_join");
						magicJoinOperator.addChild(magicOperator);
						magicJoinOperator.addChild(childOperator);
	
						if (!pcgOrNode.isBuiltInPredicateType(BuiltInPredicateType.BINARY)) {
							for (int j = 0; j < pcgOrNode.getArity(); j++) {
								if (pcgOrNode.getExecutionBinding(j) == BindingType.BOUND) {
									CompilerTypeBase compilerArgument = pcgOrNode.getArgument(j);
									CompilerTypeBase magicArgument = magicNode.getArgument(j);
									magicJoinOperator.addCondition(ComparisonOperation.EQUALITY,
											0,  
											this.convertToArgument(magicArgument,  localVariableMappings),
											1,
											this.convertToArgument(compilerArgument, localVariableMappings));
								}
							}
						}
						childOperator = magicJoinOperator;
					} else {
						magicOperator.addChild(childOperator);
						childOperator = magicOperator;
					}
				} else {
					if (pcgOrNode.isBuiltInPredicate() && (pcgOrNode.getBuiltInPredicateType() == BuiltInPredicateType.BINARY))
						literalVariableMappings = allVariableMappings;
					
					childOperator = this.generateOperator(pcgAndNode, pcgOrNode, literalVariableMappings, clique, localSharableCliqueList,  
							localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
				}
			}
			
			if (childOperator == null)
				return null;

			localVariableMappings.merge(literalVariableMappings);

			if (childOperator.getOperatorType() == OperatorType.ASSIGNMENT) {
				assignmentOperators.add(childOperator);
				continue;
			} 
			
			// add join condition 
			if ((i > 0) 
				&& !pcgOrNode.isBuiltInPredicateType(BuiltInPredicateType.BINARY)
				&& !isMagicPredicate(pcgOrNode.getPredicateName()) 
				&& !pcgOrNode.getExecutionBindingPattern().allFree()) {
				for (int j = 0; j < pcgOrNode.getArity(); j++) {
					if (pcgOrNode.getExecutionBinding(j) == BindingType.BOUND) {
						CompilerTypeBase compilerArgument = pcgOrNode.getArgument(j);
						if (variableToPredicateMap.containsKey(compilerArgument)) {
							if (literalVariableMappings == null)
								literalVariableMappings = localVariableMappings;
				
							joinOperator.addCondition(ComparisonOperation.EQUALITY, 
									variableToPredicateMap.get(compilerArgument),  
									this.convertToArgument(compilerArgument, localVariableMappings),
									operators.size(),
									this.convertToArgument(compilerArgument, literalVariableMappings));
						}
					}
				}
			}
			
			if (childOperator.getOperatorType() == OperatorType.COMPARISON) {
				Map<Operator, VariableList> upstreamOperatorToVariableMapping = getResponsibleUpstreamOperators(operators, childOperator);
				if (upstreamOperatorToVariableMapping.isEmpty()) {
					this.deALSContext.logError(logger, childOperator.toString() + " depends on 0 upstream predicates.");
					throw new CompilerException(childOperator.toString() + " depends on 0 upstream predicates.");
				}

				// collect the filters but do not install them into the plan yet - we might be combining multiple into a single filter
				// if filter predicate requires multiple upstream providers, we might place it above the join 
				if (requiresMultipleOperators(childOperator, upstreamOperatorToVariableMapping)) {
					// try to replace a variable based on the join to form a new but equivalent expression in the hope it is isolated to a single predicate
					List<JoinConditionExpression> joinConditions = joinOperator.getConditions();
					if (joinConditions.isEmpty()) {
						aboveJoinFilterExpressions.add(childOperator);
					} else {
						boolean done = false;
						Map<Operator, VariableList> operatorToVariableMapping = new HashMap<>();
						for (Operator op : operators) {
							VariableList vars = new VariableList();
							for (Argument arg : op.getArguments())
								if (arg instanceof Variable)
									vars.add((Variable) arg);
							operatorToVariableMapping.put(op,  vars);
						}
						
						for (JoinConditionExpression jce : joinConditions) {
							Operator newChildOperator = new Operator(childOperator.getName(), childOperator.getOperatorType(), childOperator.getArguments().copy());
							for (int k = 0; k < 4; k++) {
								if (k == 0 && jce.getLeft() == childOperator.getArgument(0))
									newChildOperator.getArguments().set(0, jce.getRight());
								else if (k == 1 && jce.getLeft() == childOperator.getArgument(1))
									newChildOperator.getArguments().set(1, jce.getRight());
								else if (k == 2 && jce.getRight() == childOperator.getArgument(0)) 
									newChildOperator.getArguments().set(0, jce.getLeft());
								else if (k == 3 && jce.getRight() == childOperator.getArgument(1))
									newChildOperator.getArguments().set(1, jce.getLeft());
								
								if (!requiresMultipleOperators(newChildOperator, operatorToVariableMapping)) {
									filterPredicateToUpstreamOperatorsList.add(new Pair<>(newChildOperator, operatorToVariableMapping));
									done = true;
									break;
								}
							}
							if (!done) {
								aboveJoinFilterExpressions.add(childOperator);
								break;
							}
						}
					}
				} else {
					filterPredicateToUpstreamOperatorsList.add(new Pair<>(childOperator, upstreamOperatorToVariableMapping));
				}
				
				continue;
			}
			
			operators.add(childOperator);
			
			allVariableMappings.merge(literalVariableMappings);
		}

		if (operators.isEmpty() && assignmentOperators.isEmpty())
			return null;
			
		if (operators.size() > 1) {
			operator = joinOperator;
			for (Operator co : operators)
				joinOperator.addChild(co);
		} else if (operators.size() == 1) {
			operator = operators.get(0);
		}
		
		// 1) group filter conditions by upstream operator to build filter predicate
		Map<Operator, FilterOperator> operatorToFilterMap = new HashMap<>();
		for (Pair<Operator, Map<Operator, VariableList>> entry : filterPredicateToUpstreamOperatorsList) {
			Operator rawFO = entry.getFirst();
			HashMap<Operator, VariableList> innerMap = (HashMap<Operator, VariableList>) entry.getSecond();
			for (Map.Entry<Operator, VariableList> innerEntry : innerMap.entrySet()) {
				if (!operatorToFilterMap.containsKey(innerEntry.getKey()))
					operatorToFilterMap.put(innerEntry.getKey(), new FilterOperator(rawFO.getName()));
				
				operatorToFilterMap.get(innerEntry.getKey()).addExpression(rawFO.getName(), 
						rawFO.getArgument(0), rawFO.getArgument(1));
			}
		}
		
		for (Map.Entry<Operator, FilterOperator> entry : operatorToFilterMap.entrySet()) {
			FilterOperator filterOperator = entry.getValue();
			// if already has filter, merge into existing filter
			switch (entry.getKey().getOperatorType()) {
				case FILTER:
					for (ComparisonExpression expr : filterOperator.getExpressions())
						((FilterOperator)entry.getKey()).addExpression(expr);
					break;
				default:
					if (!insertOperator(operator, entry.getKey(), filterOperator)) {
						filterOperator.addChild(operator);
						operator = filterOperator;
					}
					break;
			}
		}
				
		if (!aboveJoinFilterExpressions.isEmpty()) {
			FilterOperator aboveJoinFilter = new FilterOperator(operator.getName());
			for (Operator op : aboveJoinFilterExpressions)
				aboveJoinFilter.addExpression(op.getName(), op.getArgument(0), op.getArgument(1));
			
			aboveJoinFilter.addChild(operator);
			operator = aboveJoinFilter;
		}

		if (assignmentOperators.size() > 0 || needsProjection(pcgAndNode, operator, localVariableMappings)) {
			String name = "";
			if (operator != null)
				name = operator.getName();
			
			OperatorArguments projectionOperatorArguments = this.generateArguments(pcgAndNode, localVariableMappings);		
			ProjectionOperator projectionOperator = new ProjectionOperator(name, projectionOperatorArguments);
			
			for (Operator ao : assignmentOperators) {
				for (Argument arg : projectionOperator.getArguments()) {
					if (arg == ao.getArgument(0)) {
						projectionOperator.removeArgument(arg);
						projectionOperator.addArgument(ao.getArgument(1), arg);
						break;
					}
				}
			}
			
			if (projectionOperator.getArity() > 0) {
				allVariableMappings.clear();
				allVariableMappings.merge(localVariableMappings);
				if (operator != null)
					projectionOperator.addChild(operator);
				operator = projectionOperator;
				
				//special case for only assignment(s) - APS 11/11/2015
				// convert the projection operator to a tuple
				if (operator.getNumberOfChildren() == 0) {		
					// convert to tuple operator
					Operator tupleOperator = new Operator("TUPLE", OperatorType.TUPLE);
					OperatorArguments args = new OperatorArguments();					
					for (int i = 0; i < operator.getArity(); i++)
						args.add(operator.getArgument(i));		
											
					tupleOperator.setArguments(args);
					operator = tupleOperator;
				}
			}
		}
		
		// if limit operator exists, make that parent of this operator
		operator = this.generateLimitOperator(pcgAndNode, operator, localVariableMappings);
		
		operator.setPCGNode(pcgAndNode);
		
		this.deALSContext.logTrace(logger, "Exiting generateOperators with {}", operator);

		allVariableMappings.merge(localVariableMappings);
		
		return operator;
	}
	
	private static boolean insertOperator(Operator parentOperator, Operator oldChild, Operator newChild) {
		// old child becomes grandchild
		for (int i = 0; i < parentOperator.getNumberOfChildren(); i++) {
			if (parentOperator.getChild(i) == oldChild) {
				newChild.addChild(oldChild);
				parentOperator.replaceChild(i, newChild); 
				return true;
			}
			
			if (insertOperator(parentOperator.getChild(i), oldChild, newChild))
				return true;
		}
		return false;
	}
		
	private static boolean needsProjection(PCGAndNode pcgAndNode, Operator operator, VariableMappings variableMappings) {
		if (operator.getOperatorType() == OperatorType.FILTER)
			operator = operator.getChild(0);
		
		if (pcgAndNode.getArity() == 0)
			return false;
		
		if (pcgAndNode.getArity() != operator.getArity())
			return true;
		
		Argument arg;
		for (int i = 0; i < operator.getArity(); i++) {
			arg = operator.getArgument(i);
			if (arg instanceof AliasedArgument)
				arg = ((AliasedArgument)arg).getAlias();
			
			if (!(arg instanceof Variable)) {
				if (pcgAndNode.getArgument(i).isVariable())
					return true;
				
				continue;
			}
			
			if (variableMappings.get((Variable) arg) != null)
				continue;
			
			boolean found = false;
			// order is irrelevant, so check entire argument list
			for (int j = 0; j < pcgAndNode.getArity(); j++) {
				if (pcgAndNode.getArgument(j).isVariable() 
					&& (((Variable)arg).getName().toLowerCase().equals(((CompilerVariable)pcgAndNode.getArgument(j)).toStringVariableName().toLowerCase()))) {
					found = true;
					break;
				}
			}
			
			if (!found)
				return true;
		}
		
		switch (operator.getOperatorType()) {
			case PROJECT:
			case AGGREGATE:
			case AGGREGATE_FS:
			case BASE_RELATION:
			case RECURSIVE_CLIQUE:
			case MUTUAL_RECURSIVE_CLIQUE:
			case RECURSIVE_RELATION:
				return false;
		}
		
		return true;
	}
	
	private static boolean isProjectionDistinct(Operator projectedFromOperator, OperatorArguments projectionOperatorArguments) {
		Operator originalProjectedFromOperator = null;
		if (projectedFromOperator.getOperatorType() == OperatorType.FILTER) {
			originalProjectedFromOperator = projectedFromOperator;
			projectedFromOperator = projectedFromOperator.getChild(0);			
		}
		
		switch (projectedFromOperator.getOperatorType()) {			
			case AGGREGATE:
			case AGGREGATE_FS:
				// if we project only the grouping arguments, distinct = false
				if (projectedFromOperator.getArity() == projectionOperatorArguments.size())
					return false;
				
				OperatorArguments groupByArguments = new OperatorArguments();
				for (Argument arg : projectedFromOperator.getArguments()) {
					if ((arg instanceof AggregateArgument) 
							|| ((arg instanceof AliasedArgument) 
									&& ((AliasedArgument)arg).getArgument() instanceof AggregateArgument))
						break;
							
					groupByArguments.add(arg);
				}
				
				if (!groupByArguments.isEmpty()) {
					int i;
					for (i = 0; i < groupByArguments.size() && i < projectionOperatorArguments.size(); i++)
						if (!projectionOperatorArguments.contains(groupByArguments.get(i)))
							return true;
					
					if (i < groupByArguments.size())
						return true;
				}
				
				return false;
			//case PROJECT:
			//	return ((ProjectionOperator)projectedFromOperator).isDistinct();
			case RECURSIVE_CLIQUE:
			case MUTUAL_RECURSIVE_CLIQUE:
			case RECURSIVE_RELATION:
			case BASE_RELATION:
				OperatorArguments projectedArguments = new OperatorArguments(projectionOperatorArguments);
				
				// if certain filters and the projection combine to account for all arguments from relation, no need for distinction
				if ((originalProjectedFromOperator != null) && (originalProjectedFromOperator.getOperatorType() == OperatorType.FILTER)) {
					FilterOperator filterOperator = (FilterOperator)originalProjectedFromOperator;
					
					for (ComparisonExpression expr : filterOperator.getExpressions()) {
						if ((expr.getOperation() == ComparisonOperation.EQUALITY)
								|| (expr.getOperation() == ComparisonOperation.INEQUALITY)) {
							if (!projectedArguments.contains(expr.getLeft()))
								projectedArguments.add(expr.getLeft());
							if (!projectedArguments.contains(expr.getRight()))
								projectedArguments.add(expr.getRight());
						}
					}
				}
				
				if (projectedFromOperator.getArity() == projectedArguments.size())
					return false;
				
				if (projectedFromOperator.getArity() == projectedArguments.copy().removeConstants().size())
					return false;
		}
		return true;
	}
	
	private static Map<CompilerTypeBase, Integer> getVariableToPredicateMap(PCGAndNode pcgAndNode) {
		Map<CompilerTypeBase, Integer> variableToPredicateMap = new HashMap<>();
		List<PCGOrNode> nodes = new ArrayList<>();
		for (int i = 0; i < pcgAndNode.getNumberOfChildren(); i++) {
			if (pcgAndNode.getChild(i).isBuiltInPredicateType(BuiltInPredicateType.BINARY) 
				&& ComparisonOperation.getOperation(pcgAndNode.getChild(i).getPredicateName()) != ComparisonOperation.NONE)
				continue;
			nodes.add(pcgAndNode.getChild(i));
		}
		
		for (int i = 0; i < nodes.size(); i++) {
			PCGOrNode child = nodes.get(i);
			// skip magic predicates - we're not going to join to them
			if (isMagicPredicate(child.getPredicateName()))
				continue;
			for (int j = 0; j < child.getArity(); j++) {
				if ((child.getExecutionBinding(j) == BindingType.FREE) 
						&& !(child.getArgument(j).isVariable() && ((CompilerVariable)child.getArgument(j)).isAnonymous()))
					variableToPredicateMap.put(child.getArgument(j), i);				
			}
		}	
		return variableToPredicateMap;
	}
	

	private OperatorArguments generateProjectionOperatorArguments(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings) {
		if (pcgAndNode == null)
			return new OperatorArguments();
		
		CompilerTypeList allArguments = pcgAndNode.getArguments().copy();
		
		for (int i = allArguments.size() - 1; i >= 0; i--)
			if (allArguments.get(i).isNil() || (allArguments.get(i).isConstant() && allArguments.get(i).toString().equals("nil")))
				allArguments.remove(i);
		
		for (PCGOrNode child : pcgAndNode.getChildren()) {
			for (int i = 0; i < child.getArity(); i++) {
				CompilerTypeBase childArg = child.getArgument(i);
				if (allArguments.contains(childArg))
					continue;
				
				if (childArg.isVariable() && !((CompilerVariable)childArg).isAnonymous())
					allArguments.add(childArg);
				else if (childArg.isAnyAggregate())
					allArguments.add(childArg);
				else if (childArg.isConstant())
					allArguments.add(childArg);
			}
		}
		
		OperatorArguments projectionOperatorArguments = new OperatorArguments();
		CompilerTypeBase compilerArg;
				
		for (int i = 0; i < pcgOrNode.getArity(); i++) {
			compilerArg = pcgOrNode.getArgument(i);
			if (allArguments.contains(compilerArg)) {
				if (compilerArg.isVariable()) {
					if (((CompilerVariable)compilerArg).hasValueAssigned()) {
						// include only if argument is being projected
						for (int j = 0; j < pcgAndNode.getArity(); j++) {
							if (pcgAndNode.getArgument(j) == compilerArg) {
								projectionOperatorArguments.add(Utilities.convertToConstant(compilerArg, this.typeManager));								
								break;
							}
						}
					} else {
						projectionOperatorArguments.add(this.convertToArgument(compilerArg, variableMappings));
						//projectionOperatorArguments.add(edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.convertToArgument(compilerArg, variableMappings, this.typeManager));
					}
				} else if (compilerArg.isAnyAggregate()) {
					Argument constant = Utilities.convertToConstant(pcgAndNode.getArgument(i), this.typeManager);
					if (constant == null)
						projectionOperatorArguments.add(this.convertToArgument(pcgAndNode.getArgument(i), variableMappings));
					else
						projectionOperatorArguments.add(constant);
				} else if (compilerArg.isConstant()){
					for (int j = 0; j < pcgAndNode.getArity(); j++) {
						if (pcgAndNode.getArgument(j) == compilerArg) {
							projectionOperatorArguments.add(Utilities.convertToConstant(compilerArg, this.typeManager));								
							break;
						}
					}
				}
			}
		}
		
		// handle the case where variables are projected multiple times (e.g. pcgAndNode(X,X) <- pcgOrNode(X, _).)
		// if the pcgOrNode accounts for all of the pcgAndNode's variables, modify the arguments to include the duplicates
		if (pcgAndNode.getNumberOfChildren() == 1) {
			if (pcgAndNode.getArguments().hasDuplicates()) {
				for (int i = 0; i < pcgAndNode.getArity(); i++) {
					if (pcgAndNode.getArgument(i).isVariable()) {
						if (i >= projectionOperatorArguments.size())
							projectionOperatorArguments.add(variableMappings.get((CompilerVariable) pcgAndNode.getArgument(i)));
						else if (projectionOperatorArguments.get(i) != variableMappings.get((CompilerVariable) pcgAndNode.getArgument(i)))
							projectionOperatorArguments.add(i, variableMappings.get((CompilerVariable) pcgAndNode.getArgument(i)));
					}
				}
			}
			
			if (pcgAndNode.getArity() == pcgOrNode.getArity()) {			
				CompilerTypeList args = pcgAndNode.getArguments();
				OperatorArguments orderedOperatorArguments = new OperatorArguments();
				for (int i = 0; i < pcgOrNode.getArity(); i++) {
					if ((args.get(i) instanceof CompilerVariable) && !args.get(i).isBound()) {
						if (variableMappings.get((CompilerVariable) args.get(i)) != projectionOperatorArguments.get(i))
							orderedOperatorArguments.add(variableMappings.get((CompilerVariable) args.get(i)));
						else
							orderedOperatorArguments.add(projectionOperatorArguments.get(i));
					} else {
						orderedOperatorArguments.add(projectionOperatorArguments.get(i));
					}
				}
				
				projectionOperatorArguments = orderedOperatorArguments;
			}
		}
				
		return projectionOperatorArguments;
	}
		
	private Operator generateLimitOperator(PCGAndNode pcgAndNode, Operator operator, VariableMappings variableMappings) {
		this.deALSContext.logTrace(logger, "Entering generateLimitOperator for {}", pcgAndNode);
		
		for (PCGOrNode child : pcgAndNode.getChildren()) {
			if ((child.getPredicate() instanceof BuiltInPredicate) 
					&& child.getPredicateName().equals(BuiltInPredicate.LIMIT_PREDICATE_NAME)) {
				// if we have a limit node, and the limit argument is a variable in the head, since there will not be a child
				// to bind the variable in the body, we must mark the variable's binding as bound 
				Argument limitArgument = convertToArgument( child.getArgument(0), variableMappings);
				
				boolean found = false;
				for (int j = 0; j < pcgAndNode.getNumberOfChildren() && !found; j++) {
					if (pcgAndNode.getChild(j).getArguments().contains(child.getArgument(0)))
						found = true;
				}
				
				OperatorArguments arguments = generateArguments(pcgAndNode, variableMappings);
				
				Operator limitOperator = new Operator(pcgAndNode.getPredicateName(), OperatorType.LIMIT, arguments);
				limitOperator.addChild(operator);
				operator = limitOperator;
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting generateLimitOperator with {}", (operator == null) ? "null" : operator);

		return operator;
	}
	
	private Pair<OperatorArguments, List<ComparisonExpression>> generateRelationArguments(PCGOrNode pcgOrNode, 
			VariableMappings variableMappings, Integer relationId) {
		CompilerTypeBase			compilerArg 				= null;
		Argument 					operatorArg 				= null;
		Variable 					var 						= null;
		OperatorArguments 			relationOperatorArguments 	= new OperatorArguments();
		List<ComparisonExpression> 	filterExpressions 			= new ArrayList<>();
		
		BasePredicate bp = null;
		
		if (pcgOrNode.getBasePredicate() != null)
			bp = this.deALSContext.getModuleManager().getActiveModule().getBasePredicate(pcgOrNode.getPredicateName());
		
		// 1) Generate base relation operator
		for (int i = 0; i < pcgOrNode.getArity(); i++) {
			compilerArg = pcgOrNode.getArgument(i);
			operatorArg = this.convertToArgument(compilerArg, variableMappings);

			// 1) rename arguments to match table definition if base relation
			if ((bp != null) && (compilerArg instanceof CompilerVariable)) {
				String name = bp.getBasePredicateStructuralAttribute(i).getColumnName();
				if (Character.isLowerCase(name.charAt(0)))
					name = new String(Character.toUpperCase(name.charAt(0)) + name.substring(1, name.length()));
				
				((Variable)operatorArg).rename(name);
			}
			
			//if (relationId != null && operatorArg instanceof Variable)
			//	((Variable)operatorArg).rename(relationId + "." + ((Variable)operatorArg).getName());

			// 3 options for why the argument can be bound
			// 1) constant
			// 2) variable bound to constant
			// 3) variable set by an earlier (left) node in rule body
			if (pcgOrNode.getBinding(i) == BindingType.BOUND) {
				if (compilerArg.isConstant()) {
					if (bp != null) {
						var = new Variable(bp.getBasePredicateStructuralAttribute(i).getColumnName(), 
							bp.getBasePredicateStructuralAttribute(i).getDataType());
					} else {
						var = ProgramGenerator.generateUniqueVariable();
						var.setDataType(compilerArg.getDataType());
					}
					
					filterExpressions.add(new ComparisonExpression(ComparisonOperation.EQUALITY, var, operatorArg));
					operatorArg = var;
				} else if (compilerArg.isVariable() && ((CompilerVariable)compilerArg).hasValueAssigned()) {
					((Variable)operatorArg).makeFree();
					filterExpressions.add(new ComparisonExpression(ComparisonOperation.EQUALITY, operatorArg, 
							Utilities.convertToConstant(compilerArg, this.typeManager)));
				}
			} 

			relationOperatorArguments.add(operatorArg);
		}
		return new Pair<>(relationOperatorArguments, filterExpressions);
	}
	
	private Pair<OperatorArguments, List<ComparisonExpression>> generateRecursiveRelationArguments(PCGOrNode pcgOrNode, 
			Operator cliqueOperator, VariableMappings variableMappings) {
		Pair<OperatorArguments, List<ComparisonExpression>> pair = this.generateRelationArguments(pcgOrNode, variableMappings, this.usedRelations.size());
		OperatorArguments 				relationOperatorArguments 	= pair.getFirst();
		List<ComparisonExpression> 		filterExpressions 			= pair.getSecond();
		
		List<Integer> positionsToRemove = new ArrayList<>();
		for (int i = 0; i < relationOperatorArguments.size(); i++) {
			if (relationOperatorArguments.get(i) instanceof Variable) {
				Argument arg = ((Variable)cliqueOperator.getArgument(i)).deepDereference();
				//if (arg instanceof Variable)
					//((Variable)relationOperatorArguments.get(i)).rename(((Variable)arg).getName());
			}
		}
		
		for (int i = positionsToRemove.size() - 1; i >= 0; i--)
			relationOperatorArguments.remove(positionsToRemove.get(i).intValue());

		return new Pair<>(relationOperatorArguments, filterExpressions);
	}
		
	private Operator generateBaseRelationOperator(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings) {
		this.deALSContext.logTrace(logger, "Entering generateBaseRelationOperator for {}", pcgOrNode);

		VariableMappings 			relationVariableMappings 		= new VariableMappings();
		Pair<OperatorArguments, List<ComparisonExpression>> pair 	= this.generateRelationArguments(pcgOrNode, relationVariableMappings, this.usedRelations.size());		
		OperatorArguments 			baseRelationOperatorArguments 	= pair.getFirst();
		List<ComparisonExpression> 	filterExpressions 				= pair.getSecond();
		
		OperatorArguments 			projectionOperatorArguments 	= this.generateProjectionOperatorArguments(pcgAndNode, 
																		pcgOrNode, relationVariableMappings);
		
		variableMappings.merge(relationVariableMappings);

		Operator operator = new Operator(pcgOrNode.getPredicateName(), OperatorType.BASE_RELATION, baseRelationOperatorArguments);
		
		this.usedRelations.add(operator);
		
		// generate filter operator (if necessary)
		if (!filterExpressions.isEmpty()) {
			FilterOperator filterOperator = new FilterOperator(operator.getName(), filterExpressions);
			filterOperator.addChild(operator);
			operator = filterOperator;
		}

		// add projection (if necessary)
		if (!projectionOperatorArguments.isEmpty() 
				&& !Utilities.isExactMatch(projectionOperatorArguments, operator, true)) {		
			ProjectionOperator projectionOperator = new ProjectionOperator(pcgOrNode.getPredicateName(), projectionOperatorArguments);
			projectionOperator.addChild(operator);
			operator = projectionOperator;
		}
					
		this.deALSContext.logTrace(logger, "Exiting generateBaseRelationOperator with {}", operator.toStringTree());

		return operator;
	}

	private Operator generateAggregateOperator(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings, CliqueBase clique, 
			List<Operator> localSharableCliqueList, Stack<Operator> localCliqueStack, List<Operator> localSharableMutualCliqueList, 
			List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {
		
		this.deALSContext.logTrace(logger, "Entering generateAggregateOperator for {}", pcgOrNode);

		Operator childOperator = this.generateOperators((PCGAndNode) pcgOrNode.getChild(0), variableMappings,  clique, localSharableCliqueList, 
				localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);

		if (childOperator.getOperatorType() == OperatorType.PROJECT) {
			((ProjectionOperator)childOperator).setDistinct(false);
			// see if we indeed need the projection
			if (childOperator.getArguments().hasConstant()) {
				if (childOperator.getArguments().copy().removeConstants().equals(childOperator.getChild(0).getArguments()))
					childOperator = childOperator.getChild(0);				
			} else {
				childOperator.getArguments().uniqueify();
			}
		}

		OperatorArguments 		arguments = new OperatorArguments();
		Map<Integer, Argument> 	constants = new HashMap<>();
		Argument 				constantOperatorArg, var;
		CompilerTypeBase 		compilerArg = null;
		Argument 				aggregateArgument = null;
		// build expressions from bound variables ONLY over aggregate argument (not grouping arguments)
		List<ComparisonExpression> filterExpressions = new ArrayList<>();
				
		for (int i = 0; i < pcgOrNode.getArity(); i++) {
			compilerArg = pcgOrNode.getArgument(i);
			if (compilerArg.isAnyAggregate() && (compilerArg instanceof BuiltInAggregate)) {
				BuiltInAggregate bia = (BuiltInAggregate) compilerArg;
				aggregateArgument = new AggregateArgument(bia.getAggregateName(), 
						this.convertToArgument(bia.getAggregateTerm(), variableMappings));

				if (pcgAndNode != null) {
					var = this.convertToArgument(pcgAndNode.getArgument(i), variableMappings);

					// two cases to place filter over aggregate value - constant or variable bound to constant
					CompilerTypeBase filterArg = pcgAndNode.getArgument(i);
					constantOperatorArg = Utilities.convertToConstant(filterArg, this.typeManager);
					
					if ((constantOperatorArg != null) && filterArg.isConstant()) {
						var = ProgramGenerator.generateUniqueVariable();
						((Variable)var).setDataType(filterArg.getDataType());
						filterExpressions.add(new ComparisonExpression(ComparisonOperation.EQUALITY, var, constantOperatorArg));							
					} else if (filterArg.isVariable() && ((CompilerVariable)filterArg).hasValueAssigned()) {
						((Variable)var).makeFree();
						filterExpressions.add(new ComparisonExpression(ComparisonOperation.EQUALITY, var, constantOperatorArg));
					}
					
					aggregateArgument = new AliasedArgument(aggregateArgument, var);
				}
				
				arguments.add(aggregateArgument);
					
				continue;
			} 
			
			if (pcgOrNode.getBinding(i) == BindingType.BOUND) {
				if (compilerArg instanceof CompilerVariable) {
					if (((CompilerVariable)compilerArg).isAnonymous())
						continue;
					else if (((CompilerVariable)compilerArg).hasValueAssigned())
						compilerArg = ((CompilerVariable)compilerArg).deepDereference();
					if (compilerArg.isConstant())
						constants.put(i, Utilities.convertToConstant(compilerArg, this.typeManager));	
					else
						arguments.add(this.convertToArgument(compilerArg, variableMappings));
				} else if (compilerArg.isConstant()) {
					constants.put(i, Utilities.convertToConstant(compilerArg, this.typeManager));
				}
			} else {
				arguments.add(this.convertToArgument(compilerArg, variableMappings));	
			}
		}		

		Operator aggregateOperator = new Operator(pcgOrNode.getPredicateName(), OperatorType.AGGREGATE, arguments);
		Operator operator = aggregateOperator;
		operator.addChild(childOperator);
		
		if (!filterExpressions.isEmpty()) {
			FilterOperator filter = new FilterOperator(pcgOrNode.getPredicateName(), filterExpressions);			
			filter.addChild(operator);
			operator = filter;
		}
		
		// if any constants are supposed to be projected
		// put above the projection above the aggregate, and remove constants from projection under aggregate
		OperatorArguments projectionArguments = this.generateProjectionOperatorArguments(pcgAndNode, pcgOrNode, variableMappings);
		
		if (projectionArguments.size() != aggregateOperator.getArity()) {
			ProjectionOperator projectionOperator = new ProjectionOperator(pcgOrNode.getPredicateName(), projectionArguments);
			projectionOperator.addChild(operator);
			operator = projectionOperator;
		}
		
		// alias the aggregates to connect with the new outer projection
		OperatorArguments aggregateArgs = aggregateOperator.getArguments();
		int projectionArgumentsIndex = projectionArguments.size() - 1;
		for (int i = aggregateArgs.size() - 1; i >= 0; i--) {
			if (!(aggregateArgs.get(i) instanceof AggregateArgument))
				break;
			aggregateArgs.set(i, new AliasedArgument(aggregateArgs.get(i), projectionArguments.get(projectionArgumentsIndex--)));
		}
		
		if (operator.getChild(0).getOperatorType() == OperatorType.PROJECT) {
			Operator child = operator.getChild(0);
			for (int i = 0; i < child.getArity(); i++) {
				if (child.getArgument(i).isConstant()) {
					Argument childArg = child.getArgument(i);
					if (childArg instanceof AliasedArgument)
						childArg = ((AliasedArgument)childArg).getArgument();
					
					Argument arg = operator.getArgument(i);
					if (arg instanceof AliasedArgument)
						arg = ((AliasedArgument)arg).getArgument();

					if (arg instanceof AggregateArgument) {
						((AggregateArgument)arg).setTerm(childArg);
						child.getArguments().remove(i);
					}
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting generateAggregateOperator with {}", operator);

		return operator;
	}
	
	private Operator generateFSAggregateOperator(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings, CliqueBase clique, 
			List<Operator> localSharableCliqueList, Stack<Operator> localCliqueStack, List<Operator> localSharableMutualCliqueList, 
			List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {
		
		this.deALSContext.logTrace(logger, "Entering generateFSAggregateOperator for {}", pcgOrNode);

		CliquePredicate cp;
		if(clique == null) {
			cp = null;
		} else {
			cp = ((Clique)clique).getCliquePredicate(pcgOrNode.getPredicateName(),
					pcgOrNode.getArity(), pcgOrNode.getBindingPattern());
		}

//		CliquePredicate cp = ((Clique)clique).getCliquePredicate(pcgOrNode.getPredicateName(),
//				pcgOrNode.getArity(), pcgOrNode.getBindingPattern());
		
		boolean argByName = false;
		PCGAndNode pcgAndNodeToGenerate = null;
		if (cp == null) {
			pcgAndNodeToGenerate = (PCGAndNode)pcgOrNode.getChild(0);
		} else {
			pcgAndNodeToGenerate = cp.getRecursiveRule(0);
			argByName = true;
		}
		Operator childOperator = this.generateOperators(pcgAndNodeToGenerate, variableMappings,  clique, localSharableCliqueList, 
			localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
		
		if (childOperator.getOperatorType() == OperatorType.PROJECT) {
			((ProjectionOperator)childOperator).setDistinct(false);
			// see if we indeed need the projection
			if (childOperator.getArguments().hasConstant()) {
				if (childOperator.getArguments().copy().removeConstants().equals(childOperator.getChild(0).getArguments()))
					childOperator = childOperator.getChild(0);
			} else {
				childOperator.getArguments().uniqueify();
			}
		}

		OperatorArguments 		arguments = new OperatorArguments();
		Map<Integer, Argument> 	constants = new HashMap<>();
		Argument 				constantOperatorArg, var;
		CompilerTypeBase 		compilerArg = null;
		Argument 				aggregateArgument = null;
		// build expressions from bound variables ONLY over aggregate argument (not grouping arguments)
		List<ComparisonExpression> filterExpressions = new ArrayList<>();
		
		for (int i = 0; i < pcgOrNode.getArity(); i++) {
			compilerArg = pcgOrNode.getArgument(i);
			if (compilerArg.isAnyAggregate() && (compilerArg instanceof BuiltInAggregate)) {
				BuiltInAggregate bia = (BuiltInAggregate) compilerArg;
				if (bia instanceof FSAggregate)
					((FSAggregate)bia).useAlternativeName();
				
				aggregateArgument = new AggregateArgument(bia.getAggregateName(), 
						this.convertToArgument(bia.getAggregateTerm(), variableMappings, argByName));

				if (pcgAndNode != null) {
					var = this.convertToArgument(pcgAndNode.getArgument(i), variableMappings, argByName);
				
					// two cases to place filter over aggregate value - constant or variable bound to constant
					CompilerTypeBase filterArg = pcgAndNode.getArgument(i);
					constantOperatorArg = Utilities.convertToConstant(filterArg, this.typeManager);
					
					if ((constantOperatorArg != null) && filterArg.isConstant()) {
						var = ProgramGenerator.generateUniqueVariable();
						((Variable)var).setDataType(filterArg.getDataType());
						filterExpressions.add(new ComparisonExpression(ComparisonOperation.EQUALITY, var, constantOperatorArg));							
					} else if (filterArg.isVariable() && ((CompilerVariable)filterArg).hasValueAssigned()) {
						((Variable)var).makeFree();
						filterExpressions.add(new ComparisonExpression(ComparisonOperation.EQUALITY, var, constantOperatorArg));
					}
				}
				arguments.add(aggregateArgument);

				// exit loop after aggregate argument found - ignore last arg
				break;
			} 
			
			if (pcgOrNode.getBinding(i) == BindingType.BOUND) {
				if (compilerArg instanceof CompilerVariable) {
					if (((CompilerVariable)compilerArg).isAnonymous())
						continue;
					else if (((CompilerVariable)compilerArg).hasValueAssigned())
						compilerArg = ((CompilerVariable)compilerArg).deepDereference();
					if (compilerArg.isConstant())
						constants.put(i, Utilities.convertToConstant(compilerArg, this.typeManager));	
					
					arguments.add(this.convertToArgument(compilerArg, variableMappings, argByName));
				} else if (compilerArg.isConstant()) {
					constants.put(i, Utilities.convertToConstant(compilerArg, this.typeManager));
					arguments.add(this.convertToArgument(compilerArg, variableMappings, argByName));
				}
			} else {
				arguments.add(this.convertToArgument(compilerArg, variableMappings, argByName));	
			}
		}		

		Operator aggregateOperator = new Operator(pcgOrNode.getPredicateName(), OperatorType.AGGREGATE_FS, arguments);
		Operator operator = aggregateOperator;
		operator.addChild(childOperator);
		
		if (!filterExpressions.isEmpty()) {
			FilterOperator filter = new FilterOperator(pcgOrNode.getPredicateName(), filterExpressions);			
			filter.addChild(operator);
			operator = filter;
		}
		
		// if any constants are supposed to be projected
		// put above the projection above the aggregate, and remove constants from projection under aggregate
		OperatorArguments projectionArguments = this.generateProjectionOperatorArguments(pcgAndNode, pcgOrNode, variableMappings);
		// ignore last argument in predicate
		projectionArguments.remove(operator.getArity() - 1);
		
		if (projectionArguments.size() != aggregateOperator.getArity()) {
			ProjectionOperator projectionOperator = new ProjectionOperator(pcgOrNode.getPredicateName(), projectionArguments);
			projectionOperator.addChild(operator);					
			operator = projectionOperator;
		}
		
		// alias the aggregates to connect with the new outer projection
		OperatorArguments aggregateArgs = aggregateOperator.getArguments();
		int projectionArgumentsIndex = projectionArguments.size() - 1;
		for (int i = aggregateArgs.size() - 1; i >= 0; i--) {
			if (!(aggregateArgs.get(i) instanceof AggregateArgument))
				break;
			aggregateArgs.set(i, new AliasedArgument(aggregateArgs.get(i), projectionArguments.get(projectionArgumentsIndex--)));
		}

		this.deALSContext.logTrace(logger, "Exiting generateAggregateOperator with {}", operator);

		return operator;
	}
	
	private Operator generateSortOperator(PCGOrNode pcgOrNode, VariableMappings variableMappings, List<Operator> localSharableCliqueList, 
			CliqueBase clique, Stack<Operator> localCliqueStack, List<Operator> localSharableMutualCliqueList,
			List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {

		Operator childOperator = this.generateOperators((PCGAndNode) pcgOrNode.getChild(0), variableMappings, clique, 
				localSharableCliqueList, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
		
		// this is stupid, but eliminate sort order list if exists on underlying (likely) projection
		if (childOperator.getArgument(childOperator.getArity() - 1) instanceof InterpreterList)
			childOperator.getArguments().remove(childOperator.getArity() - 1);
		
		OperatorArguments arguments = new OperatorArguments();
		for (int i = 0; i < pcgOrNode.getArity() - 1; i++)
			arguments.add(this.convertToArgument(pcgOrNode.getArgument(i), variableMappings));

		this.unifyTerms(childOperator.getArguments(), arguments);
		
		CompilerTypeList ctl = ((CompilerList)pcgOrNode.getArgument(pcgOrNode.getArity() - 1)).toCompilerTypeList();
		SortOperator operator = new SortOperator(pcgOrNode.getPredicateName(), arguments);
		
		for (int i = 0; i < ctl.size(); i++) {
			CompilerFunctor func = (CompilerFunctor) ctl.get(i);
			int index = ((CompilerInt)func.getArgument(0)).getValue();
			
			String direction = "asc";
			if (func.getArgument(1).toString().equals("desc"))
				direction = "desc";
			operator.addSortOrder(index, direction.equals("asc"));
		}

		operator.addChild(childOperator);
				
		return operator;
	}
	
	private Operator createBinaryOperator(PCGOrNode pcgOrNode, VariableMappings variableMappings) {
		this.deALSContext.logTrace(logger, "Entering createBinaryOperator for {}", pcgOrNode);

		String 				predicateName 	= pcgOrNode.getPredicateName();
		OperatorArguments 	arguments 		= generateArguments(pcgOrNode, variableMappings);
		Operator 			operator 		= null;

		if (arguments.size() == 2) {
			if (predicateName.equals(BuiltInPredicate.EQUALITY_PREDICATE_NAME) 
					&& !pcgOrNode.getExecutionBindingPattern().allBound())
				operator = new Operator(predicateName, OperatorType.ASSIGNMENT, arguments);
			else if (predicateName.equals(BuiltInPredicate.EQUALITY_PREDICATE_NAME)
					&& arguments.get(0) instanceof Variable && arguments.get(1) instanceof BinaryExpression
			&& ((Variable) arguments.get(0)).getName().equals("Ss")
			&& ((Variable)((BinaryExpression) arguments.get(1)).getLeft()).getName().equals("Sz")
			&& (((BinaryExpression) arguments.get(1)).getRight()).toString().equals("1")) {
				operator = new Operator(predicateName, OperatorType.ASSIGNMENT, arguments);
			} else {
				// remove duplicate variables and keep the last instance to better unify filters with base relations
				//VariableMappings deduped = new VariableMappings();
				//for (int i = variableMappings.getNumberOfVariables()-1; i >= 0; i--)
				//	if (deduped.getVariable(variableMappings.compilerVariables.get(i)) == null)
				//		deduped.put(variableMappings.compilerVariables.get(i), variableMappings.variables.get(i));			
				arguments = new OperatorArguments();
				for (int i = 0; i < pcgOrNode.getArity(); i++) {
					CompilerTypeBase compilerArg = pcgOrNode.getArgument(i);
					if (compilerArg.isVariable()) {
						if (((CompilerVariable)compilerArg).hasValueAssigned())
							compilerArg = ((CompilerVariable)compilerArg).deepDereference();
						else if (((CompilerVariable)compilerArg).isAnonymous())
							continue;
						
						Argument arg = null;
						if (compilerArg instanceof CompilerVariable)
							arg = variableMappings.get((CompilerVariable) compilerArg);
						
						if (arg == null)
							arguments.add(this.convertToArgument(compilerArg, variableMappings));
						else
							arguments.add(arg);
					} else {
						generateArgument(compilerArg, variableMappings, arguments);
					}
				}
				
				//arguments = generateArguments(pcgOrNode, deduped);

				operator = new Operator(predicateName, OperatorType.COMPARISON, arguments);
			}
		}/* else {
			this.deALSContext.logError(logger, "Binary built-in relation has wrong number of arguments");
			throw new ProgramGeneratorException("Binary built-in relation has wrong number of arguments");
		}*/

		this.deALSContext.logTrace(logger, "Exiting createBinaryOperator with {}", operator);

		return operator;
	}

	private UnionOperator createUnionOperator(PCGOrNode pcgOrNode, VariableMappings variableMappings, List<Operator> childOperators) {
		this.deALSContext.logTrace(logger, "Entering createUnionOperator for {}", pcgOrNode);
		
		UnionOperator 		operator 		= null;
		OperatorArguments operatorArguments = new OperatorArguments();
		
		for (int i = 0; i < pcgOrNode.getArity(); i++)
			operatorArguments.add(this.convertToArgument(pcgOrNode.getArgument(i), variableMappings));
		
		// match argument names with 1st child's argument names
		Operator firstChild = childOperators.get(0);
		for (int i = 0; i < firstChild.getArity(); i++) {
			Argument unionArg = operatorArguments.get(i);
			if (unionArg instanceof Variable) {
				String name = ((Variable) unionArg).getName(); // use as default in case all else fails
				Argument childArg = firstChild.getArgument(i);
				if (childArg instanceof Variable) {
					name = ((Variable)childArg).getName();
				} else if (childArg instanceof AliasedArgument) {
					Variable var = (Variable)((AliasedArgument)childArg).getAlias();
					name = var.getName();
				}
				
				((Variable)unionArg).rename(name);
			}
		}
		
		operator = new UnionOperator(pcgOrNode.getPredicateName(), operatorArguments);
		for (Operator child : childOperators)
			operator.addChild(child);
		
		this.deALSContext.logTrace(logger, "Exiting createUnionOperator with {}", operator);
		
		return operator;
	}
	
	private Operator generateNonRecursiveOperator(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings,
			Stack<Operator> localCliqueStack,
			List<Operator> localSharableCliqueList,
			List<Operator> localSharableMutualCliqueList,
			List<Operator> localMutualCliqueList,
			List<Operator> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateNonRecursiveOperator for {}", pcgOrNode);

		this.globalPCGOrNodeStack.push(pcgOrNode);		
		
		VariableMappings childVariableMappings = new VariableMappings();
		
		List<Operator> childOperators = new ArrayList<>();
		for (PCGOrNodeChild child : pcgOrNode.getChildren()) {
			Operator childOperator = this.generateOperators((PCGAndNode)child, childVariableMappings, null,
					localSharableCliqueList, localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList);
			
			Operator projectFromOperator = childOperator;
			if (projectFromOperator.getOperatorType() == OperatorType.FILTER)
				projectFromOperator = projectFromOperator.getChild(0);
			
			if (needsProjection((PCGAndNode) child, projectFromOperator, childVariableMappings)) {
				OperatorArguments projectionOperatorArguments = this.generateArguments(((PCGAndNode)child), childVariableMappings);
				if (!Utilities.isExactMatch(projectionOperatorArguments, projectFromOperator)) {
					ProjectionOperator projectionOperator = new ProjectionOperator(projectFromOperator.getName(), projectionOperatorArguments);
					projectionOperator.addChild(childOperator);					
					childOperator = projectionOperator; 
				}
			}
			
			childOperators.add(childOperator);
		}
		
		Operator operator = null;
		
		// check if we need to add a magic exit rule.
		// sometimes, magic predicate may not be a clique, so check and generate magic exit rule if necessary
		Operator magicExitOperator = this.generateMagicExitOperator(pcgOrNode, variableMappings);
		this.globalPCGOrNodeStack.pop();
		
		if (childOperators.size() == 1) {
			operator = childOperators.get(0);
			
			OperatorArguments projectionOperatorArguments = this.generateArguments(pcgOrNode, childVariableMappings);
			Operator projectFromOperator = operator;
			if (projectFromOperator.getOperatorType() == OperatorType.FILTER)
				projectFromOperator = projectFromOperator.getChild(0);
			
			if (!Utilities.isExactMatch(projectionOperatorArguments, projectFromOperator)) {
				if (pcgAndNode == null) {
					if (OperatorType.isProjectionOperatorType(projectFromOperator.getOperatorType())) {
						for (int i = 0; i < projectFromOperator.getArguments().size() && i < projectionOperatorArguments.size(); i++) {
							if (projectFromOperator.getArgument(i) == projectionOperatorArguments.get(i))
								continue;
							
							if (projectionOperatorArguments.get(i) instanceof Variable) {
								if (projectFromOperator.getArgument(i) instanceof Variable)
									projectFromOperator.getArguments().set(i, new AliasedArgument(projectFromOperator.getArgument(i), projectionOperatorArguments.get(i)));
								else if (projectFromOperator.getArgument(i) instanceof AggregateArgument)
									projectFromOperator.getArguments().set(i, new AliasedArgument(projectFromOperator.getArgument(i), projectionOperatorArguments.get(i)));
								else if (projectFromOperator.getArgument(i) instanceof AliasedArgument)
									((AliasedArgument)projectFromOperator.getArgument(i)).setAlias(projectionOperatorArguments.get(i));							
							}
						}
					}
				} else {
					if (operator.getOperatorType() == OperatorType.PROJECT) {
						/*if (projectFromOperator.getArity() == projectionOperatorArguments.size()) {
							for (int i = 0; i < projectFromOperator.getArity(); i++) {
								Argument left = projectFromOperator.getArgument(i);
								Argument right = projectionOperatorArguments.get(i);
								if ((left instanceof Variable) && (right instanceof Variable)) {
									// if the names don't match, we have to alias the variable
									if (((Variable)left).toStringVariableName().equals(((Variable)right).toStringVariableName())) {
										this.unifyTerm(left,  right);
									} else {
										projectFromOperator.getArguments().set(i, new AliasedArgument(left, right));
									}
								}
							}
						}*/
						//this.unifyTerms(projectFromOperator.getArguments(), projectionOperatorArguments);
						for (int i = 0; i < projectFromOperator.getArguments().size() && i < projectionOperatorArguments.size(); i++) {
							if (projectFromOperator.getArgument(i) == projectionOperatorArguments.get(i))
								continue;
							
							if (projectionOperatorArguments.get(i) instanceof Variable) {
								if (projectFromOperator.getArgument(i) instanceof Variable)
									projectFromOperator.getArguments().set(i, new AliasedArgument(projectFromOperator.getArgument(i), projectionOperatorArguments.get(i)));
								else if (projectFromOperator.getArgument(i) instanceof AggregateArgument)
									projectFromOperator.getArguments().set(i, new AliasedArgument(projectFromOperator.getArgument(i), projectionOperatorArguments.get(i)));
								else if (projectFromOperator.getArgument(i) instanceof AliasedArgument)
									((AliasedArgument)projectFromOperator.getArgument(i)).setAlias(projectionOperatorArguments.get(i));							
							}
						}
					} else {
						ProjectionOperator projectionOperator = new ProjectionOperator(pcgOrNode.getPredicateName(), projectionOperatorArguments); 
						int[] projectedPositions = new int[pcgOrNode.getArity()];
						for (int i = 0; i < pcgOrNode.getArity(); i++)
							if (!(pcgOrNode.getArgument(i).isVariable() && ((CompilerVariable)pcgOrNode.getArgument(i)).isAnonymous()))
								projectedPositions[i] = 1;
						
						this.unifyOperators(projectionOperatorArguments, operator.getArguments(), projectedPositions);
						projectionOperator.addChild(operator);
						operator = projectionOperator;
					}
				}
			}
			variableMappings.merge(childVariableMappings);
		} else if (childOperators.size() > 1) {
			int numberOfChildren = childOperators.size();
			if (magicExitOperator != null)
				numberOfChildren++;
			
			if (numberOfChildren > 1) {
				List<Operator> unionChildOperators = new ArrayList<>();
			
				if (magicExitOperator != null)
					unionChildOperators.add(magicExitOperator);
				
				for (Operator childOperator : childOperators)
					unionChildOperators.add(childOperator);
				
				operator = createUnionOperator(pcgOrNode, variableMappings, unionChildOperators);
			} else {
				operator = childOperators.get(0);
			}
		} else if (magicExitOperator != null) {
			operator = magicExitOperator;
			variableMappings.merge(childVariableMappings);
		}
				
		this.deALSContext.logTrace(logger, "Exiting generateNonRecursiveOperator with {}", operator);

		return operator;
	}

	private Operator generateRecursiveOperator(PCGAndNode pcgAndNode, PCGOrNode pcgOrNode, VariableMappings variableMappings,
			List<Operator> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering generateRecursiveOperator for {}", pcgOrNode);

		Operator 		cliqueOperator 		= null;
		Operator 		mutualClique 		= null;
		//Operator		recursiveOrNode 	= null;
		Operator		recursiveOperator	= null;
		CliqueBase 		baseClique 			= pcgOrNode.getBaseClique();
		boolean 		status 				= true;

		// Clique Sharing Rules:
		// - all cliques or mutual cliques within unsharable clique are not sharable outside this unsharable clique
		// - all cliques or mutual cliques within unsharable clique are sharable within that unsharable clique
		// - all cliques or mutual cliques within sharable clique are sharable outside this sharable clique
		// - all tp1, tp2, linear or non-linear magic, gen.magic cliques are not sharable
		// - all semi-naive or naive clique are sharable
		
		Triple<Boolean, Operator, Operator> retvalTriple;
		if (pcgOrNode.isSharableClique()
				&& ((retvalTriple = ProgramGenerator.findSharableClique(pcgOrNode.getPredicateName(), pcgOrNode.getArity(), 
						pcgOrNode.getBindingPattern(), localSharableCliqueList)) != null) 
				&& retvalTriple.getFirst()) {
			cliqueOperator = retvalTriple.getSecond();
			mutualClique = retvalTriple.getThird();
			
			Pair<OperatorArguments, List<ComparisonExpression>> pair = this.generateRecursiveRelationArguments(pcgOrNode, 
					cliqueOperator, variableMappings);

			recursiveOperator = new Operator(cliqueOperator.getName(), OperatorType.RECURSIVE_RELATION, pair.getFirst());
			//this.usedRelations.add(recursiveOperator);
		} else {
			this.globalPCGOrNodeStack.push(pcgOrNode);
			switch (baseClique.getType()) {
				case CLIQUE:
					cliqueOperator = this.generateCliqueOperator(pcgOrNode, (Clique) baseClique, localSharableCliqueList);
					mutualClique = cliqueOperator;
					recursiveOperator = cliqueOperator;
					//recursiveOrNode.setParentClique(clique);
					//recursiveOrNode.setClique(mutualClique);
					//recursiveOperator = ProgramGenerator.createRecursiveOperator(pcgOrNode, variableMappings);					
					//recursiveOperator.addChild(operator);
					
					for (int i = 0; i < recursiveOperator.getArity(); i++) {
						if ((pcgOrNode.getArgument(i).isVariable()) && (recursiveOperator.getArgument(i) instanceof Variable))
							variableMappings.put((CompilerVariable)pcgOrNode.getArgument(i), 
									(Variable)recursiveOperator.getArgument(i));
					}

					if ((pcgAndNode == null) && !pcgOrNode.getBindingPattern().allFree()) {
						Pair<OperatorArguments, List<ComparisonExpression>> pair = this.generateRelationArguments(pcgOrNode, variableMappings, null);
						recursiveOperator.setArguments(pair.getFirst());
												
						if (!pair.getSecond().isEmpty()) {
							// no pcgAndNode, but constants in PCGOrNode, put filter and projection over recursive clique						
							FilterOperator filterOperator = new FilterOperator(recursiveOperator.getName(), pair.getSecond());
							filterOperator.addChild(recursiveOperator);
							recursiveOperator = filterOperator;
						}					
					}
				break;
				default:
					status = false;
				break;
			}

			this.globalPCGOrNodeStack.pop();

			if (pcgOrNode.isSharableClique())
				localSharableCliqueList.add(cliqueOperator);
		}
		
		if (pcgAndNode != null) {
			OperatorArguments projectionOperatorArguments = this.generateProjectionOperatorArguments(pcgAndNode, pcgOrNode, variableMappings);
			if (!projectionOperatorArguments.isEmpty() && !Utilities.isExactMatch(projectionOperatorArguments, recursiveOperator)) {
				ProjectionOperator projectionOperator = new ProjectionOperator(pcgOrNode.getPredicateName(), projectionOperatorArguments); 
				projectionOperator.addChild(recursiveOperator);
				recursiveOperator = projectionOperator;
			}
		}
		
		//if (status)//recursiveOrNode.setRecursiveRelationName(mutualClique.getRecursiveRelationName());
		
		this.deALSContext.logTrace(logger, "Exiting generateRecursiveOperator with status = {}", status);

		return recursiveOperator;
	}

	private Operator generateCliqueOperator(PCGOrNode pcgOrNode, Clique clique, List<Operator> localSharableCliqueList) {
		this.deALSContext.logTrace(logger, "Entering generateCliqueOperator for {}", pcgOrNode);

		//Operator	 	cliqueNode 		= null;
		CliqueOperator	cliqueOperator  = null;
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
			// for generating Operator - if a clique or mtualClique is
			// put on this stack, then Operator is generated.
			Stack<Operator> localCliqueStack = new Stack<>();
			// for sharing clique
			List<Operator> newLocalSharableCliqueList = new ArrayList<>();
			// for sharing mutualClique
			List<Operator> localSharableMutualCliqueList = new ArrayList<>();
			// if a mutualClique is put on this stack, then the mutualClique can be shared
			List<Operator> localMutualCliqueList = new ArrayList<>();
			// for storing in clique
			List<Operator> localRecursiveLiteralList = new ArrayList<>();

			// Determine whether it is linear or non-linear, we do it once and use the result later
			clique.determineRecursiveType();

			// Make all external cliques sharable to inside this clique.
			// And we also make those internal cliques sharable to outside this clique
			if (pcgOrNode.isSharableClique())
				newLocalSharableCliqueList = localSharableCliqueList;		
			
			List<Operator> exitRuleChildOperators = new ArrayList<>();
			VariableMappings variableMappings = new VariableMappings();			
			
			Operator magicExitRule = this.generateMagicExitOperator(pcgOrNode, variableMappings);
			if (magicExitRule != null)
				exitRuleChildOperators.add(magicExitRule);
			
			for (PCGAndNode pcgAndNode : cliquePredicate.getExitRules())
				exitRuleChildOperators.add(this.generateOperators(pcgAndNode, variableMappings, clique, localSharableCliqueList, 
						localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList));

			Operator exitRulesOperator = null;

			if (exitRuleChildOperators.size() == 1) {
				exitRulesOperator = exitRuleChildOperators.get(0);
			} else if (exitRuleChildOperators.size() > 1) {
				exitRulesOperator = createUnionOperator(pcgOrNode, variableMappings, exitRuleChildOperators);

				//for (Operator childOperator : exitRuleChildOperators)
				//	exitRulesOperator.addChild(childOperator);
			}
						
			CliqueType cliqueType = CliqueType.CLIQUE;
			for (int i = 0; i < pcgOrNode.getPredicate().getArity(); i++)
				if ((pcgOrNode.getPredicate().getArgumentTypeAdornment().get(i) == ArgumentType.FSAGGREGATE))
					cliqueType = CliqueType.FS_CLIQUE;

			cliqueOperator = this.createCliqueOperator(pcgOrNode, clique, searchBinding, cliqueType);
			localCliqueStack.push(cliqueOperator);

			if (exitRulesOperator != null) {
				if (this.unifyWithClique(exitRulesOperator, cliqueOperator, false))
					cliqueOperator.addExitRulesOperator(exitRulesOperator);
				// case for right linear recursion with magic
				else if ((exitRulesOperator.getOperatorType() == OperatorType.FILTER)
						&& (exitRulesOperator.getNumberOfChildren() == 0))
					cliqueOperator.addExitRulesOperator(convertToTuple((FilterOperator) exitRulesOperator));
				else if (exitRulesOperator.getOperatorType() == OperatorType.ASSIGNMENT) {
					Operator tupleOperator = new Operator("TUPLE", OperatorType.TUPLE);

					OperatorArguments args = new OperatorArguments();
					args.add(new AliasedArgument(exitRulesOperator.getArgument(1), exitRulesOperator.getArgument(0)));
					tupleOperator.setArguments(args);
					this.unifyWithClique(tupleOperator, cliqueOperator, false);
					cliqueOperator.addExitRulesOperator(tupleOperator);
				}
			}
						
			List<Operator> recursiveRuleChildOperators = new ArrayList<>();
			variableMappings = new VariableMappings();
			for (PCGAndNode pcgAndNode : cliquePredicate.getRecursiveRules())
				recursiveRuleChildOperators.add(this.generateOperators(pcgAndNode, variableMappings, clique, localSharableCliqueList, 
						localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList));

			Operator recursiveRulesOperator = recursiveRuleChildOperators.get(0);
			if (recursiveRuleChildOperators.size() > 1) { 
				recursiveRulesOperator = createUnionOperator(pcgOrNode, variableMappings, recursiveRuleChildOperators);
				
				//for (Operator childOperator : recursiveRuleChildOperators)
				//	recursiveRulesOperator.addChild(childOperator);
			}
			
			if (this.unifyWithClique(recursiveRulesOperator, cliqueOperator, true))
				cliqueOperator.addRecursiveRulesOperator(recursiveRulesOperator);
			
			//cliqueNode.setRecursiveLiteralList(localRecursiveLiteralList);
			//cliqueNode.setMutualCliqueList(localMutualCliqueList);
			localCliqueStack.pop();
			// we clear this list if it is not sharable
			if (!pcgOrNode.isSharableClique())
				newLocalSharableCliqueList.clear();

			localSharableMutualCliqueList.clear();
			localCliqueStack.clear();
		}

		this.deALSContext.logTrace(logger, "Exiting generateCliqueOperator with {}", cliqueOperator);

		return cliqueOperator;
	}
	
	private Operator convertToTuple(FilterOperator magicExitFilterOperator) {
		Operator tupleOperator = new Operator("TUPLE", OperatorType.TUPLE);
		
		OperatorArguments args = new OperatorArguments();
		for (ComparisonExpression expr : magicExitFilterOperator.getExpressions())
			args.add(expr.getRight());
				
		tupleOperator.setArguments(args);
		return tupleOperator;
	}
	
	private LinkedHashMap<Operator, VariableList> getResponsibleUpstreamOperators(List<Operator> upstreamOperators, Operator comparisonOperator) {
		LinkedHashMap<Operator, VariableList> map = new LinkedHashMap<>();

		VariableList comparisonVariables = new VariableList();
		Utilities.getVariables(comparisonOperator.getArguments(), comparisonVariables);		
		
		for (Operator upstreamOperator : upstreamOperators) {
			// go left-to-right through the children and find all the operators that are responsible for the variables
			if (upstreamOperator == comparisonOperator) 
				break;
				
			if (upstreamOperator.getOperatorType() == OperatorType.COMPARISON)
				continue;
			
			if (upstreamOperator.getOperatorType() == OperatorType.FILTER)
				upstreamOperator = upstreamOperator.getChild(0);

			VariableList upstreamOperatorChildVariables = new VariableList();	
			Utilities.getVariables(upstreamOperator.getArguments(), upstreamOperatorChildVariables);
				
			VariableList intersectingVariables = new VariableList();
			edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.getIntersectingVariables(comparisonVariables, upstreamOperatorChildVariables, 
					intersectingVariables);
				
			if (intersectingVariables.isEmpty())
				continue;
			
			map.put(upstreamOperator, intersectingVariables);
		}
		
		// if we only have 1 upstream operator responsible for the downstream operator,
		// make sure it is the most upstream operator we can find
		if (map.isEmpty())
			return map;
		
		if (map.size() == 1) {
			Operator uo = map.entrySet().iterator().next().getKey();
			if (!uo.getOperatorType().isLeaf() && !uo.getOperatorType().isAggregate()) {
				//if (uo.getOperatorType() == OperatorType.PROJECT) uo = uo.getChild(0);
				upstreamOperators = new ArrayList<>();
				
				if (uo.getNumberOfChildren() == 1) {
					upstreamOperators.add(uo.getChild(0));
				} else {
					for (Operator child : uo.getChildren())
						upstreamOperators.add(child);
				}
				
				LinkedHashMap<Operator, VariableList> map2 = getResponsibleUpstreamOperators(upstreamOperators, comparisonOperator);
				if (map2.size() == 1)
					return map2;
			}
		}
		
		return map;
	}
	
	private static boolean requiresMultipleOperators(Operator comparisonOperator, Map<Operator, VariableList> upstreamOperatorToVariableMapping) {
		// if all variables are covered by one operator, multiple operators are not needed (i.e., we push the operator as low as possible.
		VariableList comparisonOperatorVariables = new VariableList();
		Utilities.getVariables(comparisonOperator.getArguments(), comparisonOperatorVariables);
		
		Map<Integer, List<Operator>> map = new HashMap<>();
		for (Map.Entry<Operator, VariableList> entry : upstreamOperatorToVariableMapping.entrySet()) {
			int counter = 0;
			for (Variable variable : comparisonOperatorVariables.variables)
				if (entry.getValue().contains(variable))
					counter++;
			
			if (!map.containsKey(counter))
				map.put(counter, new ArrayList<Operator>());
			
			map.get(counter).add(entry.getKey());
		}
		
		// only 1 operator involved
		if (map.size() == 1 && map.get(map.keySet().toArray()[0]).size() == 1)
			return false;
		
		// we look for a perfect 'covering' operator (2 variables, both used, only 1 operator)
		if (map.containsKey(new Integer(2)) && map.get(new Integer(2)).size() == 1) {
			Operator theOperator = map.get(new Integer(2)).get(0);
			// clear the existing mappings, leaving only the 'covering' operator
			VariableList variableMapping = upstreamOperatorToVariableMapping.get(theOperator);
			upstreamOperatorToVariableMapping.clear();
			upstreamOperatorToVariableMapping.put(theOperator, variableMapping);
			return false;
		}
		
		return true;
	}
	
	private Operator generateRecursiveOperator(PCGOrNode pcgOrNode, Clique clique, VariableMappings variableMappings, 
			Stack<Operator> localCliqueStack, List<Operator> localSharableCliqueList, 
			List<Operator> localSharableMutualCliqueList, List<Operator> localMutualCliqueList, 
			List<Operator> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateRecursiveOperator for {}", pcgOrNode);

		Operator 		cliqueOperator 		= null;
		Operator 		operator 			= null;
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

			cliqueOperator = ProgramGenerator.findFromNodeList(predicateName, arity, allFreeBinding, localCliqueStack);
		} else {
			cliqueOperator = ProgramGenerator.findFromNodeList(predicateName, arity, predicateBinding, localCliqueStack);
		}

		if (cliqueOperator != null) {
			Pair<OperatorArguments, List<ComparisonExpression>> pair = this.generateRecursiveRelationArguments(pcgOrNode, 
					cliqueOperator, variableMappings);

			operator = new Operator(predicateName, OperatorType.RECURSIVE_RELATION, pair.getFirst());
			//this.usedRelations.add(operator);
			localRecursiveLiteralList.add(operator);
		} else {
			operator = this.generateMutualRecursiveOperator(pcgOrNode, clique, new VariableMappings(), 
					localCliqueStack, localSharableCliqueList, localSharableMutualCliqueList, 
					localMutualCliqueList, localRecursiveLiteralList);
			
			VariableMappings newVariableMappings = new VariableMappings();
			for (int i = 0; i < pcgOrNode.getArity(); i++) {
				if (pcgOrNode.getArgument(i).isVariable())
					newVariableMappings.put((CompilerVariable)pcgOrNode.getArgument(i), (Variable) operator.getArgument(i));
			}
			
			variableMappings.merge(newVariableMappings);
		}

		this.deALSContext.logTrace(logger, "Exiting generateRecursiveOperator with {}", operator);

		return operator;
	}

	private Operator generateMutualRecursiveOperator(PCGOrNode pcgOrNode, CliqueBase clique, VariableMappings variableMappings,
			Stack<Operator> localCliqueStack, List<Operator> localSharableCliqueList, List<Operator> localSharableMutualCliqueList,
			List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateMutualRecursiveOperator for {}", pcgOrNode);

		String 	 predicateName 	= pcgOrNode.getPredicateName();
		Operator recursiveOperator = null;// = this.createRecursiveOperator(pcgOrNode, variableMappings);

		Triple<Boolean, Operator, Operator> retvalTriple = ProgramGenerator.findSharableMutualClique(predicateName, pcgOrNode.getArity(),
						pcgOrNode.getBindingPattern(), localSharableMutualCliqueList);

		Operator mutualCliqueOperator = retvalTriple.getSecond();
		Operator actualMutualClique = retvalTriple.getThird();

		// if first() is false, we didn't find a clique to share, so we have to create one
		if (!retvalTriple.getFirst()) {
			// for sharing mutualClique
			List<Operator> newLocalSharableMutualCliqueList = new ArrayList<>();

			// if a mutualClique is put on this stack, then the mutualClique can be shared
			List<Operator> newLocalMutualCliqueList = new ArrayList<>();

			switch (clique.getType()) {
				case CLIQUE:
					mutualCliqueOperator = this.generateMutualCliqueOperator(pcgOrNode, (Clique) clique, localCliqueStack,
							localSharableCliqueList, newLocalSharableMutualCliqueList,
							newLocalMutualCliqueList, localRecursiveLiteralList);
					actualMutualClique = mutualCliqueOperator;
					recursiveOperator = mutualCliqueOperator;
					break;
				default:
					this.deALSContext.logError(logger, "Unknown clique type in generateMutualRecursiveOperator");
					throw new ProgramGeneratorException("Unknown clique type in generateMutualRecursiveOperator");
			}

			for (Operator localMutualClique : newLocalMutualCliqueList)
				localMutualCliqueList.add(localMutualClique);

			//mutualClique.setMutualCliqueList(newLocalMutualCliqueList);
			localMutualCliqueList.add(mutualCliqueOperator);
			localSharableMutualCliqueList.add(mutualCliqueOperator);
			
			for (int i = 0; i < recursiveOperator.getArity(); i++) {
				if ((pcgOrNode.getArgument(i).isVariable()) && (recursiveOperator.getArgument(i) instanceof Variable))
					variableMappings.put((CompilerVariable)pcgOrNode.getArgument(i), 
							(Variable)recursiveOperator.getArgument(i));
			}

			if (!pcgOrNode.getBindingPattern().allFree()) {					
				Pair<OperatorArguments, List<ComparisonExpression>> pair = this.generateRelationArguments(pcgOrNode, variableMappings, this.usedRelations.size());
				recursiveOperator.setArguments(pair.getFirst());
										
				if (!pair.getSecond().isEmpty()) {
					// no pcgAndNode, but constants in PCGOrNode, put filter and projection over recursive clique						
					FilterOperator filterOperator = new FilterOperator(recursiveOperator.getName(), pair.getSecond());
					filterOperator.addChild(recursiveOperator);
					recursiveOperator = filterOperator;
				}					
			}
		}

		//recursiveOrNode.setParentClique((CliqueBaseNode) mutualClique);
		//recursiveOrNode.setClique((CliqueBaseNode) actualMutualClique);
		//recursiveOperator.addChild(mutualClique);
		/*
		if (recursiveOperator.getArity() == mutualClique.getArity())
			for (int i = 0; i < recursiveOperator.getArity(); i++)
				mutualClique.getArguments().set(i, new AliasedArgument(mutualClique.getArgument(i), recursiveOperator.getArgument(i)));
*/
		//recursiveOrNode.setRecursiveRelationName(actualMutualClique.getRecursiveRelationName());
		
		//this.deALSContext.logTrace(logger, "Exiting generateMutualRecursiveOperator with {}", recursiveOperator);
		this.deALSContext.logTrace(logger, "Exiting generateMutualRecursiveOperator with {}", mutualCliqueOperator);
		//return recursiveOperator;
		return mutualCliqueOperator;
	}

	private Operator generateMutualCliqueOperator(PCGOrNode pcgOrNode, Clique clique,  Stack<Operator> localCliqueStack, 
			List<Operator> localSharableCliqueList, List<Operator> localSharableMutualCliqueList, 
			List<Operator> localMutualCliqueList, List<Operator> localRecursiveLiteralList) {

		this.deALSContext.logTrace(logger, "Entering generateMutualCliqueOperator for {}", pcgOrNode);

		CliqueOperator 	mutualCliqueOperator	= null;
		String 			predicateName 	= pcgOrNode.getPredicateName();
		int 			arity 			= pcgOrNode.getArity();
		Binding 		searchBinding	= pcgOrNode.getBindingPattern();
		CliquePredicate cliquePredicate = null;

		if ((cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding)) == null) {
			// since cliquePredicate with that binding does not exist, we know there is a catch-all 
			// binding pattern i.e. all-free in performing a semi-naive fixpoint
			// In the future, we can more intelligent by choosing the clique
			// predicate with the most restricted binding that satisfy this binding
			searchBinding = new Binding(arity, BindingType.FREE);
			cliquePredicate = clique.getCliquePredicate(predicateName, arity, searchBinding);
		}

		if (cliquePredicate != null) {
			VariableMappings variableMappings = new VariableMappings();
			List<Operator> exitRuleChildOperators = new ArrayList<>();
			
			Operator magicExitOperator = this.generateMagicExitOperator(pcgOrNode, variableMappings);
			if (magicExitOperator != null)
				exitRuleChildOperators.add(magicExitOperator);
			
			for (PCGAndNode pcgAndNode : cliquePredicate.getExitRules())
				exitRuleChildOperators.add(this.generateOperators(pcgAndNode, variableMappings, clique, localSharableCliqueList, 
						localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList));
						
			Operator exitRulesOperator = null;

			if (exitRuleChildOperators.size() == 1) {
				exitRulesOperator = exitRuleChildOperators.get(0);
			} else if (exitRuleChildOperators.size() > 1) {
				exitRulesOperator = createUnionOperator(pcgOrNode, variableMappings, exitRuleChildOperators);

				//for (Operator childOperator : exitRuleChildOperators)
				//	exitRulesOperator.addChild(childOperator);
			}
						
			CliqueType cliqueType = CliqueType.MUTUAL_CLIQUE;
			for (int i = 0; i < pcgOrNode.getPredicate().getArity(); i++)
				if ((pcgOrNode.getPredicate().getArgumentTypeAdornment().get(i) == ArgumentType.FSAGGREGATE))
					cliqueType = CliqueType.MUTUAL_FS_CLIQUE;
			
			mutualCliqueOperator = this.createCliqueOperator(pcgOrNode, clique, searchBinding, cliqueType);
			localCliqueStack.push(mutualCliqueOperator);

			if (exitRulesOperator != null) {
				this.unifyWithClique(exitRulesOperator, mutualCliqueOperator, false);
				mutualCliqueOperator.addExitRulesOperator(exitRulesOperator);
				// else if ((exitRulesOperator.getOperatorType() == OperatorType.FILTER)
				//		&& (exitRulesOperator.getNumberOfChildren() == 0))
				//	mutualCliqueOperator.addExitRulesOperator(convertToTuple((FilterOperator) exitRulesOperator));
			}
			
			List<Operator> recursiveRuleChildOperators = new ArrayList<>();
			variableMappings = new VariableMappings();
			for (PCGAndNode pcgAndNode : cliquePredicate.getRecursiveRules())
				recursiveRuleChildOperators.add(this.generateOperators(pcgAndNode, variableMappings, clique, localSharableCliqueList, 
						localCliqueStack, localSharableMutualCliqueList, localMutualCliqueList, localRecursiveLiteralList));
						
			Operator recursiveRulesOperator = recursiveRuleChildOperators.get(0);
			if (recursiveRuleChildOperators.size() > 1) {
				recursiveRulesOperator = createUnionOperator(pcgOrNode, variableMappings, recursiveRuleChildOperators);		

				//for (Operator childOperator : recursiveRuleChildOperators)
				//	recursiveRulesOperator.addChild(childOperator);
			}	
			
			this.unifyWithClique(recursiveRulesOperator, mutualCliqueOperator, true);
			mutualCliqueOperator.addRecursiveRulesOperator(recursiveRulesOperator);
			
			localCliqueStack.pop();
			localSharableMutualCliqueList.clear();
		}

		this.deALSContext.logTrace(logger, "Exiting generateMutualCliqueOperator with {}", mutualCliqueOperator);
		
		return mutualCliqueOperator;
	}

	private Operator createRecursiveOperator(PCGOrNode pcgOrNode, VariableMappings variableMappings) {
		this.deALSContext.logTrace(logger, "Entering createRecursiveOperator for {}", pcgOrNode);

		String predicateName = ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), pcgOrNode.getBindingPattern(),
												pcgOrNode.getXYStageVariableBinding());

		OperatorArguments arguments = new OperatorArguments();
		CompilerTypeBase compilerArg = null;
		for (int i = 0; i < pcgOrNode.getArity(); i++) {
			compilerArg = pcgOrNode.getArgument(i);
			if (compilerArg.isNil())
				continue;
			
			if (compilerArg.isVariable()) {
				if (((CompilerVariable)compilerArg).hasValueAssigned())
					compilerArg = ((CompilerVariable)compilerArg).deepDereference();
				//else if (((CompilerVariable)compilerArg).isAnonymous())
				//	continue;
			}
			
			arguments.add(this.convertToArgument(compilerArg, variableMappings));
		}
		
		Operator operator = new Operator(predicateName, OperatorType.RECURSION, arguments); 
		
		this.deALSContext.logTrace(logger, "Exiting createRecursiveOperator with {}", operator);

		return operator;
	}

	private CliqueOperator createCliqueOperator(PCGOrNode pcgOrNode, Clique clique, Binding searchBinding, CliqueType cliqueType) {
		this.deALSContext.logTrace(logger, "Entering createCliqueOperator for {}", pcgOrNode);

		CliqueOperator	 	operator		= null;
		String 				predicateName 	= ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), searchBinding);
		OperatorArguments 	arguments 		= ProgramGenerator.generateUniqueArguments(pcgOrNode.getArguments());
		
		switch (cliqueType) {
			case CLIQUE:
				operator = new CliqueOperator(predicateName, OperatorType.RECURSIVE_CLIQUE, arguments, 
						clique, EvaluationType.SemiNaive);
				break;
			case MUTUAL_CLIQUE:
				operator = new CliqueOperator(predicateName, OperatorType.MUTUAL_RECURSIVE_CLIQUE, arguments, 
						clique, EvaluationType.SemiNaive);
				break;
			case FS_CLIQUE:
				operator = new CliqueOperator(predicateName, OperatorType.RECURSIVE_CLIQUE, arguments, 
						clique, EvaluationType.MonotonicSemiNaive);
				break;
			case MUTUAL_FS_CLIQUE:
				operator = new CliqueOperator(predicateName, OperatorType.MUTUAL_RECURSIVE_CLIQUE, arguments, 
						clique, EvaluationType.MonotonicSemiNaive);
				break;
			default:
				this.deALSContext.logError(logger, "Unknown clique type in createCliqueOperator");
				throw new ProgramGeneratorException("Unknown clique type in createCliqueOperator");
		}

		this.deALSContext.logTrace(logger, "Exiting createCliqueOperator with {}", operator);

		return operator;
	}

	// Given a predicate 'pred' with binding pattern, 'bffb' the magic predicate name is created as 'magic_bffb_pred'
	private Operator generateMagicExitOperator(PCGOrNode pcgOrNode, VariableMappings variableMappings) {
		this.deALSContext.logTrace(logger, "Entering generateMagicExitOperator for {}", pcgOrNode);

		//Operator magicExitOperator = this.constructMagicExitOperator(pcgOrNode, variableMappings);
		FilterOperator magicExitOperator = null;
		String postfix;

		// here, we make a very special assumption that magic predicate begins
		// with "$$_magic_" in order to locate the spot for adding the magic exit rule
		if ((postfix = getPredicateForMagicPredicate(pcgOrNode.getPredicateName())) != null) {
		//if ((postfix = Utilities.getPostfix(pcgOrNode.getPredicateName(), Rewriter.MAGIC_PREDICATE_NAME_PREFIX)) != null) {
			PCGOrNode stackOrNode = this.searchGlobalOperatorStack(postfix);

			if (stackOrNode != null) {
				String uniqueMagicPredicateName = ProgramGenerator.generateUniquePredicateName(pcgOrNode.getPredicateName(), 
																								pcgOrNode.getBindingPattern());
				magicExitOperator = new FilterOperator(uniqueMagicPredicateName);
				// set magic arguments to bound argument of stackOrNode
				// we assume that the number of bound arguments in stacknode
				// is the same as the number of arguments in magic and node
				for (int i = 0; i < stackOrNode.getArity(); i++)
					if (stackOrNode.getBinding(i) == BindingType.BOUND)
						magicExitOperator.addExpression("=", 
								this.convertToArgument(pcgOrNode.getArgument(i), variableMappings),
								Utilities.convertToConstant(stackOrNode.getArgument(i), this.typeManager));
			}
		}

		this.deALSContext.logTrace(logger, "Exiting generateMagicExitOperator with {}", (magicExitOperator == null) ? "null" : magicExitOperator);
		
		return magicExitOperator;
	}
	
	private static String getPredicateForMagicPredicate(String predicateName) {
		return edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.getPostfix(predicateName, Rewriter.MAGIC_PREDICATE_NAME_PREFIX);
	}
	
	private static boolean isMagicPredicate(String predicateName) {
		return (getPredicateForMagicPredicate(predicateName) != null);
	}

	/*************************************************************
	 * search for RecursiveOrNode on stack corresponding to magic predicate
	 * 
	 * Given a predicate 'pred' with binding pattern, say 'bffb' the magic predicate name is written as 'magic_bffb_pred'.
	 * Thus, while search for the right clique on stack, we need to first extract the binding pattern from the 
	 * magic predicate name, note that the prefix 'magic_' has already been stripped. Thus, we construct the binding 
	 * pattern for clique and then, compare the predicate name and the binding pattern
	 **************************************************************/
	private PCGOrNode searchGlobalOperatorStack(String predicateName) {
		this.deALSContext.logTrace(logger, "Entering searchGlobalOperatorStack for {}", predicateName);

		PCGOrNode stackOrNode = null;

		for (PCGOrNode pcgOrNode : this.globalPCGOrNodeStack) {
			String nameWithBiding = pcgOrNode.getPredicateName() + "_" + pcgOrNode.getBindingPattern();
			if (nameWithBiding.equals(predicateName)) {
				stackOrNode = pcgOrNode;
				break;
			}
		}

		this.deALSContext.logTrace(logger, "Exiting searchGlobalOperatorStack with {}", (stackOrNode == null) ? "null" : stackOrNode);

		return stackOrNode;
	}

	private static Variable generateUniqueVariable() {
		return new Variable(UNIQUE_VARIABLE_NAME_PREFIX + uniqueVariableCounter++);
	}

	private static String generateUniquePredicateName(String predicateName, Binding predicateBinding) {
		return predicateName + "_" + predicateBinding.createBindingString();
	}

	// APS added 3/9/2013 to account for execution binding not being added
	private static String generateUniquePredicateName(String predicateName, Binding predicateBinding, BindingType xyBindingType) {
		String uniqueString = predicateName + "_" + predicateBinding.createBindingString();
		if (xyBindingType != null)
			uniqueString += xyBindingType.toString();
		return uniqueString;
	}
	
	private static Operator findFromNodeList(String predicateNameWithBinding, int arity, List<Operator> list) {
		for (Operator node : list)
			if (predicateNameWithBinding.equals(node.getName()) && arity == node.getArity())
				return node;
		
		return null;
	}

	private static Operator findFromNodeList(String predicateName, int arity, Binding predicateBinding, 
			List<Operator> list) {
		String predicateNameWithBinding = ProgramGenerator.generateUniquePredicateName(predicateName, predicateBinding);
		return ProgramGenerator.findFromNodeList(predicateNameWithBinding, arity, list);
	}

	private static Triple<Boolean, Operator, Operator> matchCliqueOrSubClique(String predicateName, int arity,
			Binding predicateBinding, List<Operator> sharableCliqueList) {

		boolean status = false;
		String predicateNameWithBinding = ProgramGenerator.generateUniquePredicateName(predicateName, predicateBinding);
		CliqueOperator actualClique = null;
		CliqueOperator parentClique = null;

		for (Operator clique : sharableCliqueList) {
			parentClique = (CliqueOperator)clique;
			if ((predicateNameWithBinding.equals(parentClique.getName())) && (arity == parentClique.getArity())) {
				actualClique = parentClique;
				status = true;
				break;
			}

			// stupid type erasure
			List<Operator> cliqueList = new ArrayList<>();
			for (Operator mc : parentClique.getMutualCliqueList())
				cliqueList.add(mc);
			
			// we try to share the mutual clique if the clique is sharable
			actualClique = (CliqueOperator) ProgramGenerator.findFromNodeList(predicateNameWithBinding, arity, cliqueList);
			if (actualClique != null) {
				status = true;
				break;
			}
		}

		if (!status) {
			parentClique = null;
			actualClique = null;
		}

		return new Triple<Boolean, Operator, Operator>(status, parentClique, actualClique);
	}

	private static Triple<Boolean, Operator, Operator> findSharableMutualClique(String predicateName, int arity, 
			Binding predicateBinding, List<Operator> sharableMutualCliqueList) {
		List<Operator> cliqueList = new ArrayList<>();
		for (Operator mc : sharableMutualCliqueList)
			cliqueList.add(mc);

		return findSharableClique(predicateName, arity, predicateBinding, cliqueList);
	}

	private static Triple<Boolean, Operator, Operator> findSharableClique(String predicateName, int arity, Binding predicateBinding, 
			List<Operator> sharableCliqueList) {

		boolean status = false;
		Operator parentClique = null;
		Operator actualClique = null;

		Triple<Boolean, Operator, Operator> retvalTriple = ProgramGenerator.matchCliqueOrSubClique(predicateName, arity, predicateBinding,
						sharableCliqueList);
		parentClique = retvalTriple.getSecond();
		actualClique = retvalTriple.getThird();

		if (retvalTriple.getFirst()) {
			status = true;
		} else {
			Binding allFreeBinding = new Binding(arity, BindingType.FREE);
			retvalTriple = ProgramGenerator.matchCliqueOrSubClique(predicateName, arity, allFreeBinding, sharableCliqueList);
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

	private static OperatorArguments generateUniqueArguments(CompilerTypeList oldArguments) {
		OperatorArguments newArguments = new OperatorArguments();				
		Variable variable;

		for (int i = 0; i < oldArguments.size(); i++) {
			variable = ProgramGenerator.generateUniqueVariable();
			variable.setDataType(TypeInferrer.inferTermDataType(oldArguments.get(i)));
			newArguments.add(variable);
		}
		return newArguments;
	}

	/*************************************************************
	 * unifying Node with Clique or Mutual Clique - we can assume that all arguments are free unique variables in node since a 
	 * clique is used to unify with a child exit or recursive operator. Thus, no special equality rewriting is necessary.
	 **************************************************************/
	private boolean unifyWithClique(Operator operator, Operator clique, boolean isRecursiveRule) {
		this.deALSContext.logTrace(logger, "Entering unifyWithClique node = {} clique = {}", operator, clique);

		boolean status = false;

		if (operator.getArity() == clique.getArity())
			if (operator.getOperatorType() == OperatorType.UNION)
				status = true;
			else if (isRecursiveRule)
				status = this.unifyTerms(clique.getArguments(), operator.getArguments());
			else
				status = true;
		// lastly replace free variables bound to constants since cliques only have constants internally, not in the head
		for (Argument arg : clique.getArguments())
			if ((arg instanceof Variable) && arg.isBound() && ((Variable)arg).deepDereference().isConstant())
				((Variable)arg).makeFree();
		
		this.deALSContext.logTrace(logger, "Exiting unifyWithClique with status = {}", status);

		return status;
	}

	private boolean unifyTerms(OperatorArguments toTerms, OperatorArguments fromTerms) {
		this.deALSContext.logTrace(logger, "Entering unifyTerms");

		if (toTerms.size() == fromTerms.size()) {
			for (int i = 0; i < toTerms.size(); i++)
				unifyTerm(toTerms.get(i), fromTerms.get(i));
		}
		
		this.deALSContext.logTrace(logger, "Exiting unifyTerms");

		return true;
	}
	
	private void unifyTerm(Argument toTerm, Argument fromTerm) {		
		if (fromTerm instanceof AliasedArgument)
			fromTerm = ((AliasedArgument)fromTerm).getAlias();
		
		if (toTerm == fromTerm)
			return;

		// toTerm should be a variable
		if (toTerm instanceof Variable) {
			// We do short circuiting here
			if ((fromTerm instanceof Variable) && ((Variable) fromTerm).isBound())
				((Variable) toTerm).setValue(((Variable) fromTerm).deepDereference());
			else if (fromTerm instanceof AliasedArgument)
				((Variable) toTerm).setValue(((AliasedArgument)fromTerm).getAlias());
			else if (((Variable) toTerm).isBound() && (((Variable) toTerm).deepDereference()) instanceof Variable)
				((Variable)((Variable) toTerm).deepDereference()).setValue(fromTerm);
			else
				((Variable) toTerm).setValue(fromTerm);					
		} else if (toTerm instanceof AliasedArgument) {
			Variable var = (Variable)((AliasedArgument)toTerm).getAlias();
			if ((fromTerm instanceof Variable) && ((Variable) fromTerm).isBound())
				((Variable) var).setValue(((Variable) fromTerm).deepDereference());
			else if (fromTerm instanceof AliasedArgument)
				((Variable) var).setValue(((AliasedArgument)fromTerm).getAlias());
			else if (((Variable) var).isBound() && (((Variable) var).deepDereference()) instanceof Variable)
				((Variable)((Variable) var).deepDereference()).setValue(fromTerm);
			else
				((Variable) var).setValue(fromTerm);
		}
	}
	
	private void unifyOperators(OperatorArguments toTerms, OperatorArguments fromTerms, int[] projectedPositions) {
		this.deALSContext.logTrace(logger, "Entering unifyOperators");

		// silly case in negation queries
		if (fromTerms.size() > 0) {
			Argument toTerm, fromTerm;
			List<Integer> fromTermPositions = new LinkedList<>();
			for (int i = 0; i < projectedPositions.length; i++)
				if (projectedPositions[i] == 1)
					fromTermPositions.add(i);
			
			if (fromTermPositions.size() == toTerms.size()) {		
				for (int i = 0; i < toTerms.size(); i++) {				
					toTerm = toTerms.get(i);
					fromTerm = fromTerms.get(fromTermPositions.get(i));
		
					if (fromTerm instanceof AliasedArgument)
						fromTerm = ((AliasedArgument)fromTerm).getAlias();
					
					if (toTerm == fromTerm)
						continue;
					
					if (toTerm.equals(fromTerm))
						continue;
					
					if (toTerm instanceof Variable) {
						if (fromTerm instanceof Variable) {
							if (((Variable) fromTerm).isBound())
								((Variable) toTerm).setValue(((Variable) fromTerm).deepDereference());
							else
								toTerms.set(i,  new AliasedArgument(fromTerm, toTerm));	//((Variable) toTerm).setValue(fromTerm);
						} else if (fromTerm instanceof AliasedArgument) {
							((Variable) toTerm).setValue(((AliasedArgument)fromTerm).getAlias());
						}
					}
				}
			}
		}
		this.deALSContext.logTrace(logger, "Exiting unifyOperators");		
	}
	
	/*private OperatorArguments generateArguments(PCGNode<?> pcgNode, VariableMappings variableMappings) {
		OperatorArguments arguments = new OperatorArguments();
		CompilerTypeBase compilerArg = null;
		for (int i = 0; i < pcgNode.getArity(); i++) {
			compilerArg = pcgNode.getArgument(i);
			if (compilerArg.isNil())
				continue;

			if (compilerArg.isVariable()) {
				if (((CompilerVariable)compilerArg).hasValueAssigned())
					compilerArg = ((CompilerVariable)compilerArg).deepDereference();
				else if (((CompilerVariable)compilerArg).isAnonymous())
					continue;
				
				arguments.add(edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.convertToArgument(compilerArg, variableMappings, 
						this.typeManager));
			} else if (compilerArg.isFunctor()) {
				// ignore CompilerFunctor name for now
				CompilerFunctor func = (CompilerFunctor)compilerArg;
				CompilerTypeBase funcArg = null;
				for (int j = 0; j < func.getArity(); j++) {
					funcArg = func.getArgument(j);
					if (funcArg.isNil())
						continue;
					
					if (funcArg.isVariable()) {
						if (((CompilerVariable)funcArg).hasValueAssigned())
							funcArg = ((CompilerVariable)funcArg).deepDereference();
					}
					arguments.add(edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.convertToArgument(funcArg, variableMappings, 
						this.typeManager));
				}
			} else {
				arguments.add(edu.ucla.cs.wis.bigdatalog.interpreter.Utilities.convertToArgument(compilerArg, variableMappings, 
						this.typeManager));
			}
		}
		
		return arguments;
	}*/
	
	private OperatorArguments generateArguments(PCGNode<?> pcgNode, VariableMappings variableMappings) {
		OperatorArguments arguments = new OperatorArguments();
		CompilerTypeBase compilerArg = null;
		for (int i = 0; i < pcgNode.getArity(); i++) {
			compilerArg = pcgNode.getArgument(i);
			if (compilerArg.isNil())
				continue;

			generateArgument(compilerArg, variableMappings, arguments);
		}
		return arguments;
	}
	
	private void generateArgument(CompilerTypeBase compilerArg, VariableMappings variableMappings, OperatorArguments arguments) {		
		if (compilerArg.isVariable()) {
			if (((CompilerVariable)compilerArg).hasValueAssigned())
				compilerArg = ((CompilerVariable)compilerArg).deepDereference();
			else if (((CompilerVariable)compilerArg).isAnonymous())
				return;
			
			arguments.add(this.convertToArgument(compilerArg, variableMappings));
			return;
		} else if (compilerArg.isFunctor()) {
			// ignore CompilerFunctor name for now
			CompilerFunctor func = (CompilerFunctor)compilerArg;
			CompilerTypeBase funcArg = null;
			for (int j = 0; j < func.getArity(); j++) {
				funcArg = func.getArgument(j);
				if (funcArg.isNil())
					continue;
				
				if (funcArg.isVariable()) {
					if (((CompilerVariable)funcArg).hasValueAssigned())
						funcArg = ((CompilerVariable)funcArg).deepDereference();
				}
				arguments.add(this.convertToArgument(funcArg, variableMappings));
			}
			return;
		}
		
		arguments.add(this.convertToArgument(compilerArg, variableMappings));		
	}
	
	private static void finalizeVariableAssignments(Operator operator, Stack<CliqueOperator> visitedCliques) {
		if (visitedCliques.contains(operator))
			return;
		
		ProgramGenerator.compressVariableAssignments(operator.getArguments());
		
		if (operator.getOperatorType() == OperatorType.JOIN) {
			for (JoinConditionExpression condition : ((JoinOperator)operator).getConditions()) {
				condition.setLeft(ProgramGenerator.compressVariables(condition.getLeft()));
				condition.setRight(ProgramGenerator.compressVariables(condition.getRight()));
			}
		}
		
		if (operator.getOperatorType() == OperatorType.FILTER) {
			for (ComparisonExpression expression : ((FilterOperator)operator).getExpressions()) {
				expression.setLeft(ProgramGenerator.compressVariables(expression.getLeft()));
				expression.setRight(ProgramGenerator.compressVariables(expression.getRight()));
			}
		}
					
		if (operator.getOperatorType() == OperatorType.RECURSION) {
			ProgramGenerator.finalizeVariableAssignments(operator.getChild(0), visitedCliques);			
		} else if (operator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE 
				|| operator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE) {
			visitedCliques.push((CliqueOperator)operator);
			if (((CliqueOperator)operator).getExitRulesOperator() != null)
				ProgramGenerator.finalizeVariableAssignments(((CliqueOperator)operator).getExitRulesOperator(), visitedCliques);
			ProgramGenerator.finalizeVariableAssignments(((CliqueOperator)operator).getRecursiveRulesOperator(), visitedCliques);
		} else {
			for (Operator childOperator : operator.getChildren())
				ProgramGenerator.finalizeVariableAssignments(childOperator, visitedCliques);			
		}
	}
		
	private static void compressVariableAssignments(OperatorArguments arguments) {
		for (int i = arguments.size() - 1; i >= 0; i--) 
			arguments.set(i, ProgramGenerator.compressVariables(arguments.get(i)));
	}
		
	private static Argument compressVariables(Argument arg) {
		if (arg instanceof Variable) {
			return ((Variable) arg).deepDereference();
		} else if (arg instanceof InterpreterFunctor) {
			InterpreterFunctor intFunctor = (InterpreterFunctor)arg;
			for (int j = intFunctor.getArity() - 1; j >= 0; j--)
				intFunctor.setArgument(j, compressVariables(intFunctor.getArgument(j)));

			return intFunctor;
		} else if (arg instanceof InterpreterList) {
			InterpreterList intList = (InterpreterList)arg;
			intList.setHead(compressVariables(intList.getHead()));
			ProgramGenerator.compressVariables(intList.getTail());
			return intList;
		} else if (arg instanceof ComparisonExpression) {
			ComparisonExpression expr = (ComparisonExpression)arg;
			expr.setLeft(compressVariables(expr.getLeft()));
			expr.setRight(compressVariables(expr.getRight()));
			return expr;
		} else if (arg instanceof AggregateArgument) {
			AggregateArgument aa = (AggregateArgument)arg;
			aa.setTerm(compressVariables(aa.getTerm()));
			return aa;
		} else if (arg instanceof AliasedArgument) {
			AliasedArgument aa = (AliasedArgument)arg;
			aa.setArgument(compressVariables(aa.getArgument()));
			aa.setAlias(compressVariables(aa.getAlias()));
			return aa;
		} else if (arg instanceof AliasedVariable) {
			AliasedVariable av = (AliasedVariable)arg;
			av.setVariable((Variable) compressVariables(av.getVariable()));
			return av;
		} else {//(arg instanceof DbTypeBase) {
			return arg;
		}
	}	

	private Operator optimize(QueryForm queryForm, Operator root) {
		// add filters and remove constants in base relations
		this.removeConstantsFromProjections(queryForm, root);
				
		//root = ProgramGenerator.compressRecursion(root);
		if (root.getOperatorType() == OperatorType.RECURSION)
			root = root.getChild(0);

		ProgramGenerator.finalizeVariableAssignments(root, new Stack<CliqueOperator>());

		root = ProgramGenerator.removeSealingAggregates(null, root);
				
		root = ProgramGenerator.compressProjections(root);

		ProgramGenerator.finalizeVariableAssignments(root, new Stack<CliqueOperator>());
		
		ProgramGenerator.enforceSetSemantics(root);
		
		ProgramGenerator.renameOperators(root);
		
		//ProgramGenerator.renameVariables(root, this.baseRelations);
		
		return root; 
	}
	
	private static void enforceSetSemantics(Operator root) {
		if (root.getOperatorType() == OperatorType.PROJECT) {
			boolean result = ProgramGenerator.isProjectionDistinct(root.getChild(0), root.getArguments());
			((ProjectionOperator)root).setDistinct(result);
		}
		
		for (Operator child : root.getChildren())
			ProgramGenerator.enforceSetSemantics(root, child);
		
		if (root instanceof CliqueOperator) {
			CliqueOperator cliqueOperator = (CliqueOperator)root;
			if (cliqueOperator.getExitRulesOperator() != null)
				ProgramGenerator.enforceSetSemantics(cliqueOperator.getExitRulesOperator());
			
			ProgramGenerator.enforceSetSemantics(cliqueOperator.getRecursiveRulesOperator());
		}
	}
	
	private static void enforceSetSemantics(Operator parent, Operator operator) {
		if (parent.getOperatorType() == OperatorType.PROJECT) {
			if (operator.getOperatorType().isAggregate()) {
				boolean result = ProgramGenerator.isProjectionDistinct(operator, parent.getArguments());
				((ProjectionOperator)parent).setDistinct(result);
			}
		}
		
		for (Operator child : operator.getChildren())
			ProgramGenerator.enforceSetSemantics(operator, child);
		
		if (operator instanceof CliqueOperator) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator;
			if (cliqueOperator.getExitRulesOperator() != null)
				ProgramGenerator.enforceSetSemantics(cliqueOperator.getExitRulesOperator());
			
			ProgramGenerator.enforceSetSemantics(cliqueOperator.getRecursiveRulesOperator());
		}
	}
	
	private static Operator removeSealingAggregates(Operator parent, Operator orNode) {
		/*Sealing aggregate rules have the form:
		 *   fsminshortestpathsLL(X, Z, min<D>) <- fpath2LL(X, Z, D).
		 * where fspath2LL is:
		 *   fpath2LL(X,Y,fsmin<D>) <- arcW(X, Y, D).
		 *   fpath2LL(X,Z,fsmin<D>) <- fpath2LL(X, Y, D1), arcW(Y, Z, D2), D = D1 + D2.
		 */
		// replaced aggregate with ornode in sealing aggregate rules
		// these are min and max stratified aggregates over a literal that is an fs aggregate predicate

		boolean isSealingAggregate = false;
		if (orNode.getOperatorType() == OperatorType.AGGREGATE) {
			// always only one child andNode under Aggregate Operator
			if ((orNode.getChild(0).getOperatorType() == OperatorType.RECURSIVE_CLIQUE) 
					&& ((CliqueOperator)orNode.getChild(0)).getEvaluationType() == EvaluationType.MonotonicSemiNaive) {
				isSealingAggregate = true;		
			}
		}
		
		Operator operator = orNode; 
		
		if (isSealingAggregate) {
			Operator aggregateNode = orNode;
			CliqueOperator clique = (CliqueOperator) orNode.getChild(0);
			
			for (int i = 0; i < clique.getArity(); i++) {
				if (aggregateNode.getArgument(i) instanceof AliasedArgument) {
					AliasedArgument aa = (AliasedArgument)aggregateNode.getArgument(i);
					clique.getArguments().set(i, new AliasedArgument(clique.getArgument(i), aa.getAlias()));
				}
			}
			
			operator = clique;
					
			// root has no parent
			if (parent != null) {
				int index;
				for (index = 0; index < parent.getNumberOfChildren(); index++)
					if (parent.getChild(index) == orNode)
						break;
			
				parent.replaceChild(index, operator);
			}
		} else {
			for (Operator child : orNode.getChildren()) {
				for (Operator grandChild : child.getChildren())
					removeSealingAggregates(child, grandChild);				
			}
		}
		
		return operator;
	}
	
	public static Operator compressProjections(Operator operator) {
		// 1) first remove any empty projections
		operator = doRemoveEmptyProjections(operator);
		
		// 2) compress query plan
		Operator compressedOperator = doCompressProjections(operator, operator);
		
		// 3) remove unnecessary alias arguments
		doRemoveUnnecessaryAliases(compressedOperator);
		
		// 4) reverse aliases to eliminate them
		doReverseAliases(compressedOperator);
		
		// 5) compress query plan
		compressedOperator = doCompressProjections(operator, operator);
		
		return compressedOperator;
	}
	
	private static Operator doReverseAliases(Operator operator) {
		if (operator.getNumberOfChildren() == 0)
			return operator;
		
		if ((operator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE) 
				|| (operator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE)) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator; 
			if (cliqueOperator.getExitRulesOperator() != null)
				cliqueOperator.setExitRulesOperator(doReverseAliases(cliqueOperator.getExitRulesOperator()));
			cliqueOperator.setRecursiveRulesOperator(doReverseAliases(cliqueOperator.getRecursiveRulesOperator()));
		} else {
			for (int i = 0; i < operator.getNumberOfChildren(); i++)
				operator.getChildren()[i] = doReverseAliases(operator.getChild(i));			
		}
		
		if ((operator.getOperatorType() == OperatorType.AGGREGATE) 
				|| (operator.getOperatorType() == OperatorType.AGGREGATE_FS)
				|| (operator.getOperatorType() == OperatorType.PROJECT)) {
			for (int i = 0; i < operator.getArity(); i++) {
				if (operator.getArgument(i) instanceof AliasedArgument) {
					AliasedArgument aa = (AliasedArgument)operator.getArgument(i);
					if (aa.getArgument() instanceof Variable) {
						((Variable)aa.getAlias()).setValue(aa.getArgument());
						operator.getArguments().set(i, aa.getArgument());
					}
				}
			}
		}
		
		return operator;
	}
	
	private static Operator doRemoveEmptyProjections(Operator operator) {
		if (operator.getNumberOfChildren() == 0)
			return operator;
		
		Operator startOperator = null;
		do {
			startOperator = operator;
			if ((operator.getOperatorType() == OperatorType.PROJECT) && (operator.getArity() == 0))
				operator = operator.getChild(0);
		} while (operator != startOperator); 
		
		if ((operator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE) 
				|| (operator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE)) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator; 
			if (cliqueOperator.getExitRulesOperator() != null)
				cliqueOperator.setExitRulesOperator(doRemoveEmptyProjections(cliqueOperator.getExitRulesOperator()));
			cliqueOperator.setRecursiveRulesOperator(doRemoveEmptyProjections(cliqueOperator.getRecursiveRulesOperator()));
		} else {
			for (int i = 0; i < operator.getNumberOfChildren(); i++)
				operator.getChildren()[i] = doRemoveEmptyProjections(operator.getChild(i));			
		}
		
		return operator;
	}
	
	private static void doRemoveUnnecessaryAliases(Operator operator) {
		if (operator.getNumberOfChildren() == 0)
			return;
		
		if (operator.getOperatorType() == OperatorType.AGGREGATE 
				&& operator.getChild(0).getOperatorType() == OperatorType.PROJECT) {
			Operator aggregate = operator;
			Operator projection = operator.getChild(0);
			
			for (int j = 0; j < projection.getArity(); j++) {
				if (projection.getArgument(j) instanceof AliasedArgument) {					
					AliasedArgument aa = ((AliasedArgument)projection.getArgument(j));
					if ((aa.getAlias() instanceof Variable) && (!(aa.getArgument() instanceof Expression))) {
						Variable variableToLookFor = (Variable)aa.getAlias();
						boolean validCase = true;
						// if the aliased argument is only used in aggregate functions, we can remove it
						for (int i = 0; i < aggregate.getArity(); i++) {
							VariableList variableList = new VariableList();
							
							if ((aggregate.getArgument(i) instanceof AliasedArgument) 
									&& (((AliasedArgument)aggregate.getArgument(i)).getArgument() instanceof AggregateArgument)) {
								Utilities.getVariables(((AliasedArgument)aggregate.getArgument(i)).getAlias(), variableList);
								if (variableList.contains(variableToLookFor))
									validCase = false;
							} else if (!(aggregate.getArgument(i) instanceof AggregateArgument)) {
								Utilities.getVariables(aggregate.getArgument(i), variableList);
								if (variableList.contains(variableToLookFor))
								validCase = false;
							}
						}
						
						if (validCase) {
							for (int i = 0; i < aggregate.getArity(); i++) {
								Argument arg = aggregate.getArgument(i);
								if (arg instanceof AliasedArgument)
									arg = ((AliasedArgument)arg).getArgument();
								
								if (arg instanceof AggregateArgument) {
									AggregateArgument aggrArg = (AggregateArgument)arg;
									Argument aggrTerm = aggrArg.getTerm();
									if (aggrTerm == variableToLookFor)
										aggrArg.setTerm(aa.getArgument());
								}
							}
							projection.getArguments().set(j, aa.getArgument());
						}
					}
				}						
			}
		} else {
			if (operator.getOperatorType() == OperatorType.PROJECT) {
				Operator child = operator.getChild(0);
				if (child.getOperatorType() == OperatorType.FILTER)
					child = child.getChild(0);
				
				if (child.getOperatorType() == OperatorType.AGGREGATE 
						|| child.getOperatorType() == OperatorType.PROJECT) {			
					for (int i = 0; i < operator.getArity(); i++) {
						if (operator.getArgument(i) instanceof AliasedArgument 
								&& (((AliasedArgument)operator.getArgument(i)).getArgument() instanceof Variable)
								&& (((AliasedArgument)operator.getArgument(i)).getAlias() instanceof Variable)) {
							AliasedArgument aa = (AliasedArgument)operator.getArgument(i);
							Variable var = (Variable) aa.getArgument();
							
							boolean replace = false;
							List<Integer> indexes = new ArrayList<>();
							for (int j = 0; j < child.getArity(); j++) {
								Argument arg = child.getArgument(j);
								if (arg == var) {
									replace = false;
									break;
								}
								
								if ((arg instanceof AliasedArgument) && ((AliasedArgument)arg).getAlias() == var) { 
									replace = true;
									indexes.add(j);
								}
							}
	
							if (replace) {
								operator.getArguments().set(i, aa.getAlias());
								for (Integer index : indexes)
									((AliasedArgument)child.getArgument(index)).setAlias(aa.getAlias());
							}
						}
					}
				}
			}
		}
		
		if ((operator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE) 
				|| (operator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE)) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator; 
			if (cliqueOperator.getExitRulesOperator() != null)
				doRemoveUnnecessaryAliases(cliqueOperator.getExitRulesOperator());
			doRemoveUnnecessaryAliases(cliqueOperator.getRecursiveRulesOperator());
		} else {
			for (int i = 0; i < operator.getNumberOfChildren(); i++)
				doRemoveUnnecessaryAliases(operator.getChild(i));
		}
	}
	
	private static Operator doCompressProjections(Operator root, Operator operator) {
		if ((operator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE) 
				|| (operator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE)) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator;
			if (cliqueOperator.getExitRulesOperator() != null)
				cliqueOperator.setExitRulesOperator(doCompressProjections(root, cliqueOperator.getExitRulesOperator()));
			cliqueOperator.setRecursiveRulesOperator(doCompressProjections(root, cliqueOperator.getRecursiveRulesOperator()));
		} else {
			if (operator.getNumberOfChildren() == 0)
				return operator;
		
			for (int i = 0; i < operator.getNumberOfChildren(); i++)
				operator.getChildren()[i] = doCompressProjections(root, operator.getChild(i));	

			List<Operator> allOperators = null;
			do {
				allOperators = toList(operator);
				// a <- b <- c becomes b <- c
				if (canRemoveTopProjection(operator, operator.getChild(0)))
					operator = ((ProjectionOperator)operator).getChild();
										
				// a <- b <- c becomes a <- c
				if (canRemoveBottomProjection(operator, operator.getChild(0))) {
					ProjectionOperator a = (ProjectionOperator)operator;
					ProjectionOperator b = (ProjectionOperator)operator.getChild(0);
					a.setChild(b.getChild());
				}

				// a <- b becomes b
				if ((operator.getOperatorType() == OperatorType.PROJECT) 
						&& (operator.getChild(0).getOperatorType().isAggregate()))
					if (mergeIntoAggregate(operator, operator.getChild(0)))
						operator = operator.getChild(0);
				
				// if projection only contains aliased arguments, we can eliminate by pushing aliases into upper operator, if possible
				// a <- b <- c becomes a <- c where a's arguments are renamed to match c's, if necessary
				/*if ((operator.getOperatorType().isAggregate()) 
						&& (operator.getChild(0).getOperatorType() == OperatorType.PROJECT)) {
					if (mergeIntoAggregate2(operator, operator.getChild(0)))
						operator.getChildren()[0] = operator.getChild(0).getChild(0);
				}*/
				
				// a <- b (filter) <- c becomes b (filter) <- c
				if ((operator.getOperatorType() == OperatorType.PROJECT)
						&& (operator.getChild(0).getOperatorType() == OperatorType.FILTER)
						&& (((FilterOperator)operator.getChild(0)).getExpressions().get(0).getOperation() == ComparisonOperation.EQUALITY)
						&& (((FilterOperator)operator.getChild(0)).getExpressions().size() == 1)
						&& (operator.getChild(0).getChild(0).getOperatorType().isAggregate())
						&& operator.getArity() == operator.getChild(0).getChild(0).getArity())
						operator = operator.getChild(0);
								
				if (canRemoveProjectionOverRelation(operator, operator.getChild(0))) {
					operator = operator.getChild(0);
					break;
				}
				
			} while (toList(operator).size() != allOperators.size());
		}

		return operator;
	}
	
	private static List<Operator> toList(Operator operator) {
		List<Operator> operators = new ArrayList<>();
		toList(operator, operators);
		return operators;
	}
	
	private static void toList(Operator operator, List<Operator> operators) {
		operators.add(operator);
		for (Operator child : operator.getChildren())
			toList(child, operators);
		
		if (operator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE || operator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE) {
			CliqueOperator co = (CliqueOperator)operator;
			if (co.getExitRulesOperator() != null)
				toList(co.getExitRulesOperator(), operators);
			toList(co.getRecursiveRulesOperator(), operators);
		}
	}
	
	private static boolean canRemoveProjectionOverRelation(Operator projectionOperator, Operator relationOperator) {
		if ((projectionOperator.getOperatorType() == OperatorType.PROJECT)
				&& ((relationOperator.getOperatorType() == OperatorType.BASE_RELATION) 
						|| (relationOperator.getOperatorType() == OperatorType.RECURSIVE_RELATION)
						|| (relationOperator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE)
						|| (relationOperator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE))) {
			if (relationOperator.getArity() != projectionOperator.getArity())
				return false;
		
			return (Utilities.isExactMatch(relationOperator.getArguments(), projectionOperator, true));
		}
		return false;
	}

	private static boolean canRemoveBottomProjection(Operator topProjection, Operator bottomProjection) {
		if ((topProjection.getOperatorType() != OperatorType.PROJECT) || (bottomProjection.getOperatorType() != OperatorType.PROJECT))
			return false;
		
		if (topProjection.getArity() != bottomProjection.getArity())
			return false;
		
		if (Utilities.isExactMatch(topProjection.getArguments(), bottomProjection))// || subsumes(topProjection, bottomProjection))
			return true;
			
		boolean status = true;
		for (int i = 0; i < topProjection.getArity(); i++) {
			if (topProjection.getArgument(i) == bottomProjection.getArgument(i))
				continue;
			
			// the case of topProjection(a as b) <- bottomProjection(a) 
			if (topProjection.getArgument(i) instanceof AliasedArgument) {
				if (bottomProjection.getArgument(i) != ((AliasedArgument)topProjection.getArgument(i)).getArgument())
					return false;
			}				
		}
		
		return status;
	}
	
	private static boolean canRemoveTopProjection(Operator topProjection, Operator bottomProjection) {
		if ((topProjection.getOperatorType() != OperatorType.PROJECT) || (bottomProjection.getOperatorType() != OperatorType.PROJECT))
			return false;
		
		if (topProjection.getArity() != bottomProjection.getArity())
			return false;
		
		if (Utilities.isExactMatch(topProjection.getArguments(), bottomProjection))
			return true;
			
		boolean status = true;
		for (int i = 0; i < topProjection.getArity(); i++) {
			if (topProjection.getArgument(i) == bottomProjection.getArgument(i))
				continue;
			
			// the case of topProjection(b) <- bottomProjection(a as b) 
			if (bottomProjection.getArgument(i) instanceof AliasedArgument) {
				if (topProjection.getArgument(i) != ((AliasedArgument)bottomProjection.getArgument(i)).getAlias())
					return false;
			} else if (topProjection.getArgument(i) instanceof AliasedArgument) {
				return false;
			}
		}
		
		return status;
	}
	
	private static boolean mergeIntoAggregate(Operator projection, Operator aggregate) {
		if ((projection.getOperatorType() != OperatorType.PROJECT) 
				|| (!aggregate.getOperatorType().isAggregate()))
			return false;
		
		if (projection.getArity() != aggregate.getArity())
			return false;

		Argument projectionArg, aggregateArg;
		Variable projectionVar;
		int mergedCounter = 0;
		int j = 0;
		for (int i = 0; i < projection.getArity(); i++) {
			projectionArg = projection.getArgument(i);
			if (projectionArg.isConstant())
				continue;
			
			aggregateArg = aggregate.getArgument(j++);
			if (projectionArg == aggregateArg) {
				mergedCounter++;
				continue;
			}
				
			projectionVar = null;
			if (projectionArg instanceof Variable)
				projectionVar = (Variable)projectionArg;
			else if (projectionArg instanceof AliasedArgument)
				projectionVar = ((Variable)((AliasedArgument)projectionArg).getAlias());
			
			if (projectionVar != null) {
				if (aggregateArg instanceof Variable) {
					aggregate.getArguments().set(j-1, new AliasedArgument(aggregateArg, projectionVar));
					mergedCounter++;
				} else if (aggregateArg instanceof AggregateArgument) {
					aggregate.getArguments().set(j, new AliasedArgument(aggregateArg, projectionArg));
					mergedCounter++;
				} else if (aggregateArg instanceof AliasedArgument) {
					((AliasedArgument)aggregateArg).setAlias(projectionVar);
					mergedCounter++;
				}
			}
		}
		
		return (mergedCounter == projection.getArity());
	}
	
	private void removeConstantsFromProjections(QueryForm queryForm, Operator operator) {
		Set<DbTypeBase> returnedConstants = new HashSet<>();
		Argument value;
		for (int i = 0; i < queryForm.getArity(); i++) {
			if ((value = Utilities.convertToConstant(queryForm.getArgument(i), this.typeManager)) != null)
				returnedConstants.add((DbTypeBase)value);
		}
		
		if (operator.getOperatorType() == OperatorType.FILTER)
			operator = operator.getChild(0);
		
		if (operator.getOperatorType() == OperatorType.PROJECT) {
			for (int i = operator.getArity() - 1; i >= 0; i--)
				if (operator.getArgument(i) instanceof DbTypeBase)
					if (!returnedConstants.contains(operator.getArgument(i)))
						operator.getArguments().remove(i);
		}
		
		for (Operator child : operator.getChildren())
			removeConstantsFromProjections(child);
		
		if (operator instanceof CliqueOperator) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator;
			if (cliqueOperator.getExitRulesOperator() != null) {
				Operator childOperator = cliqueOperator.getExitRulesOperator();
				for (Operator grandChildOperator : childOperator.getChildren())
					removeConstantsFromProjections(grandChildOperator);
			}
			
			Operator childOperator = cliqueOperator.getRecursiveRulesOperator();
			for (Operator grandChildOperator : childOperator.getChildren())
				removeConstantsFromProjections(grandChildOperator);			
		} 
	}
	
	private static void removeConstantsFromProjections(Operator operator) {
		if ((operator.getOperatorType().isAggregate()) 
				|| (operator.getOperatorType() == OperatorType.PROJECT))
			operator.getArguments().removeConstants();
		
		for (Operator childOperator : operator.getChildren())
			removeConstantsFromProjections(childOperator);
		
		// do not remove constants from top most operators
		if (operator instanceof CliqueOperator) {
			CliqueOperator cliqueOperator = (CliqueOperator)operator;
			if (cliqueOperator.getExitRulesOperator() != null) {
				Operator childOperator = cliqueOperator.getExitRulesOperator();
				for (Operator grandChildOperator : childOperator.getChildren())
					removeConstantsFromProjections(grandChildOperator);
			}
			
			Operator childOperator = cliqueOperator.getRecursiveRulesOperator();
			for (Operator grandChildOperator : childOperator.getChildren())
				removeConstantsFromProjections(grandChildOperator);			
		} 
	}
	
	private static void renameOperators(Operator operator) {
		String name = operator.getName();
		int index = name.indexOf("_");
		if (index > -1) {
			int i;
			for (i = index + 1; i < name.length(); i++) {
				if ((name.charAt(i) != 'b') && (name.charAt(i) != 'f'))
					break;
			}
			
			if (i >= name.length())
				operator.setName(name.substring(0, index));
		}
		
		if ((operator.getOperatorType() == OperatorType.RECURSIVE_CLIQUE) 
				|| (operator.getOperatorType() == OperatorType.MUTUAL_RECURSIVE_CLIQUE)) {
			if (((CliqueOperator)operator).getExitRulesOperator() != null)
				renameOperators(((CliqueOperator)operator).getExitRulesOperator());
			renameOperators(((CliqueOperator)operator).getRecursiveRulesOperator());
		} else {
			for (Operator childOperator : operator.getChildren())
				renameOperators(childOperator);	
		}
	}
	
	private static void renameVariables(Operator operator, List<BasePredicate> baseRelations) {
		switch (operator.getOperatorType()) {
			case BASE_RELATION:
			case RECURSIVE_RELATION:
				BasePredicate relation = null;
				for (BasePredicate br : baseRelations) {
					if (br.getPredicateName().equals(operator.getName())) {
						relation = br;
						break;
					}
				}
				
				if (relation != null) {
					for (int i = 0; i < operator.getArity(); i++) {
						Variable var = (Variable)operator.getArgument(i);
						var.rename(relation.getBasePredicateStructuralAttribute(i).getColumnName());
					}
				}
			
				break;
			
			case RECURSIVE_CLIQUE:
			case MUTUAL_RECURSIVE_CLIQUE:
				CliqueOperator co = (CliqueOperator)operator;
				if (co.getExitRulesOperator() != null)
					renameVariables(co.getExitRulesOperator(), baseRelations);

				renameVariables(co.getRecursiveRulesOperator(), baseRelations);				
				break;
		}
		
		for (Operator child : operator.getChildren())
			renameVariables(child, baseRelations);
	}
	
	private static void renameVariables(Argument arg, String name) {
		if (arg instanceof Variable) {
			
		} else if (arg instanceof InterpreterFunctor) {
			for (Argument a : ((InterpreterFunctor)arg).getArguments().innerArguments)
				renameVariables(a, name);
		} else if (arg instanceof InterpreterList) {	
			InterpreterList il = (InterpreterList)arg;
			while (il != null && !il.isEmpty()) {
				renameVariables(il.getHead(), name);
				il = il.getTail();
			}
		}
	}

	private static void assignInputValues(PCGOrNode root) {
		if (root.getChild(0) instanceof PCGAndNode) {
			if (root.getArity() != ((PCGAndNode)root.getChild(0)).getArity())
				return;
			
			PCGAndNode child = (PCGAndNode)root.getChild(0);
			
			for (int i = 0; i < root.getArity(); i++) {
				if (root.getArgument(i).isInputVariable() 
						&& child.getArgument(i).isVariable()) {					
					((CompilerVariable)child.getArgument(i)).value = ((CompilerInputVariable)root.getArgument(i)).value;
					root.getArguments().set(i, ((CompilerInputVariable)root.getArgument(i)).value);
				}
			}
		} else {
			for (int i = 0; i < root.getArity(); i++)
				if (root.getArgument(i).isInputVariable())
					root.getArguments().set(i, ((CompilerInputVariable)root.getArgument(i)).getValue());
		}
		
		// push down variable assignments
		for (int i = 0; i < root.getNumberOfChildren(); i++)
			pushDownVariableAssignments(root, root.getChild(i));
	}
	
	private static void pushDownVariableAssignments(PCGOrNode orNode, PCGOrNodeChild orNodeChild) {
		if (orNode.getBindingPattern().allFree())
			return;
		
		if (!(orNodeChild instanceof PCGAndNode))
			return;
		
		PCGAndNode andNode = (PCGAndNode)orNodeChild;
		
		if (orNode.getArity() != andNode.getArity())
			return;
		
		for (int i = 0; i < orNode.getArity(); i++)
			if (orNode.getArgument(i).isBound() && !andNode.getArgument(i).isBound() && andNode.getArgument(i).isVariable())
				((CompilerVariable)andNode.getArgument(i)).setValue(orNode.getArgument(i));
				
		for (int i = 0; i < andNode.getNumberOfChildren(); i++)
			pushDownVariableAssignments(andNode, andNode.getChild(i));
	}
	
	private static void pushDownVariableAssignments(PCGAndNode andNode, PCGOrNode orNode) {
		if (andNode.getBindingPattern().allFree())
			return;
		
		if (andNode.getArity() != orNode.getArity()) {
			if (orNode.getBindingPattern().allFree())
				return;
		} else {		
			for (int i = 0; i < andNode.getArity(); i++)
				if (andNode.getArgument(i).isBound() && !orNode.getArgument(i).isBound() && orNode.getArgument(i).isVariable())
					((CompilerVariable)orNode.getArgument(i)).setValue(andNode.getArgument(i));
		}
		
		for (int i = 0; i < orNode.getNumberOfChildren(); i++)
			pushDownVariableAssignments(orNode, orNode.getChild(i));		
	}
	
	private void synchronizeProgramWithQueryForm(QueryForm queryForm, PCGOrNode rootPCGOrNode, Operator rootOperator, 
			VariableMappings variableMappings) {		
		// make sure constants in query form match with program
		if (rootOperator.getOperatorType() == OperatorType.FILTER)
			return;
				
		for (int i = 0; i < queryForm.getArity() && i < rootPCGOrNode.getArity(); i++) {
			if (rootPCGOrNode.getArgument(i).isConstant()) {
				DbTypeBase constant = (DbTypeBase) this.convertToArgument(rootPCGOrNode.getArgument(i), variableMappings);
				if (constant != rootOperator.getArgument(i))
					rootOperator.getArguments().set(i, constant);					
			}
		}
	}
	
	private static Operator handleNoArgCase(Operator operator, QueryForm queryForm, DeALSContext deALSContext) {
		// match query form with program in this special case
		if ((queryForm.getArity() == 0) && (operator.getOperatorType() != OperatorType.PROJECT)) {
			ProjectionOperator projectionOperator = ProjectionOperator.createWithTrueArgument(queryForm.getPredicateName(), deALSContext.getDatabase().getTypeManager().createString("true"));
			projectionOperator.addChild(operator);
			operator = projectionOperator;				
			queryForm.addArgument(new CompilerString("true"));			
		}
				
		return operator;
	}
	
	private static void adjustOperatorNames(Operator operator) {
		switch (operator.getOperatorType()) {
			case RECURSIVE_CLIQUE:
			case MUTUAL_RECURSIVE_CLIQUE:
				CliqueOperator co = (CliqueOperator)operator;
				if (co.getExitRulesOperator() != null)
					adjustOperatorNames(co.getExitRulesOperator());
				
				if (co.getRecursiveRulesOperator() != null)
					adjustOperatorNames(co.getRecursiveRulesOperator());
				
				operator.setName(adjustOperatorName(operator.getName()));
				break;
			case RECURSION:
			case RECURSIVE_RELATION:
				operator.setName(adjustOperatorName(operator.getName()));
				break;
		}
		
		for (Operator child : operator.getChildren())
			adjustOperatorNames(child);		
	}
	
	public static String adjustOperatorName(String name) {
		String newName = name;
		String magicKeyword = "_magic_";
		int index = newName.indexOf(magicKeyword);
		if (index != -1)
			newName = newName.substring(index + magicKeyword.length()); 

		// remove magic prefix
		if (newName.startsWith(Rewriter.DUMMY_PREDICATE_NAME_PREFIX))
			newName = newName.substring(Rewriter.DUMMY_PREDICATE_NAME_PREFIX.length());
		
		newName = removeBinding(newName);
		
		// do it again, in case of '_bf_f' scenarios (magic)
		newName = removeBinding(newName);
		
		return newName;
	}
	
	private static String removeBinding(String name) {
		int index = name.lastIndexOf("_");
		if (index > -1) {
			int i;
			for (i = index + 1; i < name.length(); i++) {
				if ((name.charAt(i) != 'b') && (name.charAt(i) != 'f'))
					break;
			}
			
			if (i >= name.length())
				return name.substring(0, index);
		}
		
		return name;
	}
	
	private Argument convertToArgument(CompilerTypeBase compilerObject, VariableMappings variableMappings, boolean argByName) {
		if (argByName) {
			if (compilerObject.isConstant()) {
				return convertToArgument(compilerObject, variableMappings);
			} else if (compilerObject instanceof CompilerFunctor) {
				CompilerFunctor compilerFunctor = (CompilerFunctor)compilerObject;
				NodeArguments arguments = new NodeArguments(compilerFunctor.getArity());
				for (int i = 0; i < compilerFunctor.getArity(); i++)
					arguments.set(i, convertToArgument(compilerFunctor.getArgument(i), variableMappings, true));

				return new InterpreterFunctor(compilerFunctor.getFunctorName(), arguments);
			} else {
				Argument arg = variableMappings.getByName(((CompilerVariable)compilerObject).getVariableName());
				if (arg != null)
					return arg;
			}
		}
		
		return convertToArgument(compilerObject, variableMappings);	
	}
	
	private Argument convertToArgument(CompilerTypeBase compilerObject, VariableMappings variableMappings) {
		return Utilities.convertToArgument(compilerObject, variableMappings, this.typeManager);
	}
	
	public static ProgramGenerationResult generateProgram(DeALSContext deALSContext, CompilationResult compilationResult) {
		QueryForm queryForm = compilationResult.getQueryForm();
		ProgramGenerator programGenerator = new ProgramGenerator(deALSContext);
		String message;
		
		// generate program for the Relevant PCG
		OperatorProgram program = programGenerator.doGenerateProgram(compilationResult);
		if (program.isValid()) {
			queryForm.setProgram(program);
			
			deALSContext.logInfo(logger, "*****GENERIC PROGRAM*****");
			deALSContext.logInfo(logger, "[BEGIN Step 9 - Generated Program for Query Form '{}' BEGIN]{}", compilationResult.getQueryForm(), program);
			deALSContext.logInfo(logger, "[END Step 9 - Generated Program for Query Form '{}' END]\n", compilationResult.getQueryForm());

			message = compilationResult.getMessage() + "\nCompilation complete.";
		} else {
			deALSContext.logInfo(logger, "[BEGIN Step 9 - Generated Program for Query Form '{}' BEGIN]", compilationResult.getQueryForm());
			deALSContext.logInfo(logger, "NO PROGRAM GENERATED!");
			deALSContext.logInfo(logger, "[END Step 9 - Generated Program for Query Form '{}' END]\n", compilationResult.getQueryForm());
			

			deALSContext.logError(logger, "No program generated for query form");
			throw new CompilerException("No program generated for query form");
		}

		if (compilationResult.getCompiledProgram() != null)
			compilationResult.getCompiledProgram().delete();

		return new ProgramGenerationResult(queryForm, message);
	}
}