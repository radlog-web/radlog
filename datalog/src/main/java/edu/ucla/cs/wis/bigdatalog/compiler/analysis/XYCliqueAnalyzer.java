package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenElsePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateBase;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerInt;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.ArithmeticOperation;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYRuleType;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class XYCliqueAnalyzer {
	private static Logger logger = LoggerFactory.getLogger(XYCliqueAnalyzer.class.getName());
	
	public static int XY_STAGE_POSITION = 0;
	public static String XY_SUCCESSOR_SYMBOL = "+";
	public static int XY_SUCCESSOR_VALUE = 1;
	
	protected DeALSContext 				deALSContext;
	protected Module module;
	protected Stack<PrimedPredicateNode>	primedPredicateNodeStack;
	protected List<PrimedPredicateNode> 	primedPredicateNodes;
	protected List<PrimedPredicateNode>	visitedPrimedPredicateNodes;
	
	public XYCliqueAnalyzer(DeALSContext deALSContext, Module module) {
		this(deALSContext);
		this.module 						= module;
	}
	
	public XYCliqueAnalyzer(DeALSContext deALSContext) {
		this.deALSContext					= deALSContext;		
		this.primedPredicateNodes 			= new ArrayList<>();
		this.primedPredicateNodeStack 		= new Stack<>();
		this.visitedPrimedPredicateNodes 	= new ArrayList<>();
	}
	
	public boolean isPrimedRecursive(BasicClique clique, List<Rule> xRules, List<Rule> yRules,
			List<Rule> copyRules, List<Rule> deleteRules, List<PredicateBase> sortedPrimedPredicates) {
		this.deALSContext.logTrace(logger, "Entering isPrimedRecursive");
		this.deALSContext.logTrace(logger, "{}", clique.toString());

		boolean status = false;

		this.primedPredicateNodes.clear();

		this.generatePrimedDependencyGraphFromRules(clique, xRules, false);
		this.generatePrimedDependencyGraphFromRules(clique, yRules, true);
		this.generatePrimedDependencyGraphFromRules(clique, copyRules, true);
		this.generatePrimedDependencyGraphFromRules(clique, deleteRules, true);

		if (!this.findPrimedRecursion()) {
			this.sortPrimedPredicateList(sortedPrimedPredicates);
			status = true;
		}

		this.primedPredicateNodes.clear();

		this.deALSContext.logTrace(logger, "{}", clique.toString());
		this.deALSContext.logTrace(logger, "Exiting isPrimedRecursive with status = {}", status);

		return status;
	}

	private void generatePrimedDependencyGraphFromRules(BasicClique clique, List<Rule> rules, boolean isYRule) {
		Predicate head;
		PrimedPredicateNode headPrimedPredicateNode; 

		for (Rule rule : rules) {
			head = rule.getHead();
			headPrimedPredicateNode = this.getPrimedPredicateNode(head.getPredicateName(), head.getArity());
			if (headPrimedPredicateNode == null) {
				headPrimedPredicateNode = new PrimedPredicateNode(head.getPredicateName(), head.getArity());
				this.primedPredicateNodes.add(headPrimedPredicateNode);
			}

			this.generatePrimedDependencyGraphFromLiterals(clique, rule.getBody(), headPrimedPredicateNode, isYRule);
		}
	}

	private void generatePrimedDependencyGraphFromLiterals(BasicClique clique, List<Predicate> literals, 
			PrimedPredicateNode headPrimedPredicateNode, boolean isYRule) {
		
		PrimedPredicateNode literalPrimedPredicateNode;

		for (Predicate literal : literals) {
			if (literal instanceof IfThenElsePredicate) {
				IfThenElsePredicate iteLiteral = (IfThenElsePredicate) literal;

				this.generatePrimedDependencyGraphFromLiterals(clique, iteLiteral.getIfLiterals(), headPrimedPredicateNode, isYRule);
				this.generatePrimedDependencyGraphFromLiterals(clique, iteLiteral.getThenLiterals(), headPrimedPredicateNode, isYRule);
				this.generatePrimedDependencyGraphFromLiterals(clique, iteLiteral.getElseLiterals(), headPrimedPredicateNode, isYRule);
			} else if (literal instanceof IfThenPredicate) {
				IfThenPredicate itLiteral = (IfThenPredicate) literal;

				this.generatePrimedDependencyGraphFromLiterals(clique, itLiteral.getIfLiterals(), headPrimedPredicateNode, isYRule);
				this.generatePrimedDependencyGraphFromLiterals(clique, itLiteral.getThenLiterals(), headPrimedPredicateNode, isYRule);
			} else if (clique.getDerivedPredicate(literal.getPredicateName(), literal.getArity()) != null) {
				// For the case of y rule, if the stage argument is not simple variable,
				// we can assume that it is the successor functor since we have passed the
				// first phase of checking
				if (!isYRule || !literal.getArgument(XY_STAGE_POSITION).isVariable()) {					
					if ((literalPrimedPredicateNode = this.getPrimedPredicateNode(literal.getPredicateName(), literal.getArity())) != null) {
						headPrimedPredicateNode.addDependentNode(literalPrimedPredicateNode);
					} else {
						literalPrimedPredicateNode = new PrimedPredicateNode(literal.getPredicateName(),literal.getArity());
						this.primedPredicateNodes.add(literalPrimedPredicateNode);
					}
				}
			}
		}
	}

	private boolean findPrimedRecursion() {
		this.deALSContext.logTrace(logger, "Entering findPrimedRecursion");

		boolean status = false;

		this.primedPredicateNodeStack.clear();

		for (PrimedPredicateNode primedPredicateNode : this.primedPredicateNodes) {
			if (!primedPredicateNode.visited())
				status = this.findPrimedRecursion2(primedPredicateNode);
		}

		this.primedPredicateNodeStack.clear();

		this.deALSContext.logTrace(logger, "Exiting findPrimedRecursion with status = {}", status);
		
		return status;
	}

	private boolean findPrimedRecursion2(PrimedPredicateNode primedPredicateNode) {
		this.deALSContext.logTrace(logger, "Entering findPrimedRecursion2");
		this.deALSContext.logTrace(logger, primedPredicateNode.toString());
		
		boolean 			status = false;
		PrimedPredicateNode	dependentPrimedPredicateNode;
		
		primedPredicateNode.setVisited();

		this.primedPredicateNodeStack.push(primedPredicateNode);

		for (AnalysisNode<PredicateBase> node : primedPredicateNode.getDependentNodes()) {
			dependentPrimedPredicateNode = (PrimedPredicateNode)node;
			
			if (dependentPrimedPredicateNode.visited()) {
				if (this.primedPredicateNodeStack.contains(dependentPrimedPredicateNode)) {
					status = true;
					break;
				}
			} else {
				status = this.findPrimedRecursion2(dependentPrimedPredicateNode);
			}
		}

		this.primedPredicateNodeStack.pop();

		this.deALSContext.logTrace(logger, primedPredicateNode.toString());
		this.deALSContext.logTrace(logger, "Exiting findPrimedRecursion2 with status = {}", status);

		return status;
	}

	private void sortPrimedPredicateList(List<PredicateBase> sortedPrimedPredicates) {
		this.visitedPrimedPredicateNodes.clear();

		// We visit all nodes and gather all their dependent nodes
		for (PrimedPredicateNode primedPredicateNode : this.primedPredicateNodes) {
			if (!this.visitedPrimedPredicateNodes.contains(primedPredicateNode))
				this.visitPrimedPredicate(primedPredicateNode);
		}

		this.visitedPrimedPredicateNodes.clear();

		// We re-visit all  nodes and sort the  nodes according to their dependency order
		for (PrimedPredicateNode primedPredicateNode : this.primedPredicateNodes) {
			if (!this.visitedPrimedPredicateNodes.contains(primedPredicateNode))
				this.sortPrimedPredicates(primedPredicateNode);
		}

		//System.out.println("Visited ordering: ");
		// For each of the sorted  nodes, we create a sorted list of pred names and arity
		for (PrimedPredicateNode primedPredicateNode : this.visitedPrimedPredicateNodes) {
			//APS fixed bug on 3/8/2013
			//node = (PrimedPredicateNode) this.primedPredicateNodes.getNthElement(i);
			sortedPrimedPredicates.add(new PredicateBase(primedPredicateNode.getPredicateName(), 
					primedPredicateNode.getArity()));
			//System.out.println(primedPredicateNode);
		}

		this.visitedPrimedPredicateNodes.clear();
	}

	private void visitPrimedPredicate(PrimedPredicateNode primedPredicateNode) {
		PrimedPredicateNode dependentPrimedPredicateNode;

		for (int i = 0; i < primedPredicateNode.getNumberOfDependentNodes(); i++) {
			dependentPrimedPredicateNode = (PrimedPredicateNode)primedPredicateNode.getDependentNode(i);

			// If not visited, we visit it
			if (!this.visitedPrimedPredicateNodes.contains(dependentPrimedPredicateNode))
				this.visitPrimedPredicate(dependentPrimedPredicateNode);

			// Add all children of dependent  nodes to  node
			for (int j = 0; j < dependentPrimedPredicateNode.getNumberOfDependentNodes(); j++)
				primedPredicateNode.addDependentNode(dependentPrimedPredicateNode.getDependentNode(j));
		}

		this.visitedPrimedPredicateNodes.add(primedPredicateNode);
	}

	private void sortPrimedPredicates(PrimedPredicateNode primedPredicateNode) {
		int currentMaxIndex = -1;

		for (AnalysisNode<PredicateBase> dependentPrimedPredicateNode : primedPredicateNode.getDependentNodes()) {
			int position = 0;
			// If any of the dependent  node is in the visited/sorted list, we remember the largest index
			for (PrimedPredicateNode visitedPrimedPredicateNode : this.visitedPrimedPredicateNodes) {
				if (((PrimedPredicateNode)dependentPrimedPredicateNode) == visitedPrimedPredicateNode) {
					if (position > currentMaxIndex) {
						currentMaxIndex = position;
						break;
					}
				}
				position++;
			}
		}

		// Once the large index for the right-most dependent  node has been identified,
		// we insert the  node after, i.e. to the right of, the dependent  node
		this.visitedPrimedPredicateNodes.add(currentMaxIndex + 1, primedPredicateNode);
	}
	
	private PrimedPredicateNode getPrimedPredicateNode(String predicateName, int arity) {
		for (PrimedPredicateNode primedPredicateNode : this.primedPredicateNodes) {
			if (primedPredicateNode.getPredicateName().equals(predicateName) 
					&& (arity == primedPredicateNode.getArity())) {
				return primedPredicateNode;
			}
		}

		return null;
	}
	
	public boolean isXYClique(BasicClique basicClique, List<Rule> exitRules, List<Rule> xRules, 
			List<Rule> yRules, List<Rule> copyRules, List<Rule> deleteRules, List<PredicateBase> sortedPrimedPredicates) {
		
		this.deALSContext.logTrace(logger, "Entering isXYClique");
		
		boolean isXYClique = false;

		if (isXYClique2(basicClique, exitRules, xRules, yRules, copyRules, deleteRules) 
				&& (exitRules.size() > 0) 
				&& hasXYCommonConstantInExitRules(basicClique, exitRules)
				&& this.isXYPrimedClique(basicClique, xRules, yRules, copyRules, deleteRules, sortedPrimedPredicates))
			isXYClique = true;

		this.deALSContext.logTrace(logger, "Exiting isXYClique");
		
		return isXYClique;
	}

	private boolean isXYClique2(BasicClique basicClique, List<Rule> exitRules, List<Rule> xRules, 
			List<Rule> yRules, List<Rule> copyRules, List<Rule> deleteRules) {
		this.deALSContext.logTrace(logger, "Entering isXYClique2");
		
		boolean isXYClique = true;

		for (DerivedPredicate derivedPredicate : basicClique.getDerivedPredicates()) {
			for (Rule rule : derivedPredicate.getRules()) {
				switch (findXYRuleType(rule, basicClique)) {
				case NONE:
					isXYClique = false;
					break;
				case EXIT_RULE:
					exitRules.add(rule);
					break;
				case X_RULE:
					xRules.add(rule);
					break;
				case Y_RULE:
					yRules.add(rule);
					break;
				case COPY_RULE:
					copyRules.add(rule);
					break;
				case DELETE_RULE:
					deleteRules.add(rule);
					break;
				}
			}
			if (!isXYClique)
				break;
		}

		this.deALSContext.logTrace(logger, "Exiting isXYClique2");

		return isXYClique;
	}
	
	private boolean isXYClique(Predicate literal) {
		this.deALSContext.logTrace(logger, "Entering isXYClique");
		this.deALSContext.logTrace(logger, literal.toString());

		BasicClique basicClique = this.module.getBasicClique(literal.getPredicateName(), literal.getArity());
		boolean status = false;
		if (basicClique != null)
			status = this.isXYClique(basicClique);
			
		this.deALSContext.logTrace(logger, "Exiting isXYClique with status = {}", status);
		
		return status;
	}
	
	public boolean isXYClique(BasicClique basicClique) {
		this.deALSContext.logTrace(logger, "Entering isXYClique");
		this.deALSContext.logTrace(logger, basicClique.toString());

		List<Rule> 		exitRules = new ArrayList<>();
		List<Rule> 		xRules = new ArrayList<>();
		List<Rule> 		yRules = new ArrayList<>();
		List<Rule> 		copyRules = new ArrayList<>();
		List<Rule> 		deleteRules = new ArrayList<>();
		List<PredicateBase> 	sortedPrimedPredicates = new ArrayList<>();

		boolean status = this.isXYClique(basicClique, exitRules, xRules, yRules, copyRules, deleteRules, sortedPrimedPredicates);

		exitRules.clear();
		xRules.clear();
		yRules.clear();
		copyRules.clear();
		deleteRules.clear();
		sortedPrimedPredicates.clear();

		this.deALSContext.logTrace(logger, "Exiting isXYClique with status = {}", status);
		
		return status;
	}
	
	public List<BasicClique> mergeSubordinateXYCliques(QueryForm queryForm) {
		List<BasicClique> cliques = this.module.getCliques();
		List<BasicClique> updatedCliques = new ArrayList<>();
		// merge subordinate XY Cliques into parent Clique
		for (BasicClique clique : cliques) {
			// if a clique is xy clique and is used by another xy clique push it inside the clique 
			boolean exclude = false;
			if (this.isXYClique(clique)) {				
				boolean matchesQueryForm = false;
				for (DerivedPredicate dp : clique.getDerivedPredicates()) {
					if (dp.getPredicateName().equals(queryForm.getPredicateName()) 
							&& dp.getArity() == queryForm.getArity())
						matchesQueryForm = true;
				}
				
				// if query form is calling this clique its not subordinate
				if (!matchesQueryForm) {
					List<BasicClique> xyCallers = getXYCallerXYCliques(cliques, clique);
					if (xyCallers.size() == 1) {
						BasicClique caller = xyCallers.get(0);
						// merge clique into its xy caller
						for (DerivedPredicate dp : clique.getDerivedPredicates())
							caller.addDerivedPredicate(dp);

						exclude = true;
					}
				}
			}
			
			if (!exclude)
				updatedCliques.add(clique);
		}
		return updatedCliques;
	}	
	
	private List<BasicClique> getXYCallerXYCliques(List<BasicClique> cliques, BasicClique xyClique) {
		String predicateName = xyClique.getDerivedPredicate(0).getPredicateName();
		int arity = xyClique.getDerivedPredicate(0).getArity();
		List<BasicClique> callers = new ArrayList<>();
		for (BasicClique clique : cliques) {
			if (clique == xyClique)continue;
			
			if (this.isXYClique(clique)) {
				int timesCalledCounter = 0;
				for (DerivedPredicate dp : clique.getDerivedPredicates()) {
					for (Rule r : dp.getRules()) {
						for (Predicate literal : r.getBody()) {
							if (literal.getPredicateName().equals(predicateName) 
									&& literal.getArity() == arity)
								timesCalledCounter++;
						}
					}
				}
				if (timesCalledCounter > 0)
					callers.add(clique);
			}
		}
		return callers;
	}

	private XYRuleType findXYRuleType(Rule rule, BasicClique basicClique) {
		this.deALSContext.logTrace(logger, "Entering findXYRuleType");
		this.deALSContext.logTrace(logger, "{}", rule.toString());

		int				numberOfIdenticalsWithSimpleStage = 0;
		int				numberOfLiteralsWithSimpleStage = 0;
		int				numberOfLiteralsWithComplexStage = 0;
		int				numberOfDeletionNegatedLiterals = 0;
		CompilerVariableList	headNonStageVariables = new CompilerVariableList();
		CompilerVariableList	nonStageVariables = new CompilerVariableList();

		Pair<XYRuleType, CompilerVariable> retvalPair = findXYHeadType(rule.getHead());
		XYRuleType xyRuleType = retvalPair.getFirst();
		CompilerVariable stageVariable = retvalPair.getSecond();

		getNonStageVariables(rule.getHead(), headNonStageVariables);

		for (Predicate literal : rule.getBody()) {
			Pair<XYRuleType, Integer[]> retvalPair2 = findXYLiteralType(rule.getHead(), literal, basicClique, stageVariable, 
					xyRuleType, numberOfIdenticalsWithSimpleStage, numberOfLiteralsWithSimpleStage,
					numberOfLiteralsWithComplexStage, numberOfDeletionNegatedLiterals, headNonStageVariables, nonStageVariables);
			
			xyRuleType = retvalPair2.getFirst();
			numberOfIdenticalsWithSimpleStage = retvalPair2.getSecond()[0];
			numberOfLiteralsWithSimpleStage = retvalPair2.getSecond()[1];
			numberOfLiteralsWithComplexStage = retvalPair2.getSecond()[2];
			numberOfDeletionNegatedLiterals = retvalPair2.getSecond()[3];
			
			// stop if we find we're not examining an XYClique
			if (xyRuleType == XYRuleType.NONE)
				break;
		}

		if (xyRuleType == XYRuleType.Y_RULE) {
			// Check for copy and deletion rule here
			if ((numberOfIdenticalsWithSimpleStage == 1)
					&& (!Utilities.intersects(headNonStageVariables, nonStageVariables))) {
				if (numberOfDeletionNegatedLiterals >= 1)
					xyRuleType = XYRuleType.DELETE_RULE;
				else
					xyRuleType = XYRuleType.COPY_RULE;
			} else if (numberOfLiteralsWithSimpleStage <= 0) {
				if (numberOfLiteralsWithComplexStage <= 0) {
					xyRuleType = XYRuleType.NONE;
				} else {
					// 	original comment - It should complain that this rule be rewritten as X-rule
					
					// APS said on 3/7/2013 - we allow rewritten rules to be Y-rules, even though they look like poorly written X-rules
					//   example : head(I + 1, X) <- literal1(I + 1, X), literal2(I + 1, X).
					if (!rule.isResultOfRewrite())
						xyRuleType = XYRuleType.NONE;
				}
			}
		}

		headNonStageVariables.clear();
		nonStageVariables.clear();

		this.deALSContext.logTrace(logger, "Exiting findXYRuleType with {}", xyRuleType.name());

		return xyRuleType;
	}

	private static Pair<XYRuleType, CompilerVariable> findXYHeadType(Predicate head) {		
		CompilerVariable 			stageVariable = null;
		XYRuleType 			xyRuleType;
		CompilerTypeBase 	stageArgument = head.getArgument(XY_STAGE_POSITION);

		if (stageArgument.isGround()) {
			xyRuleType = XYRuleType.EXIT_RULE;
		} else if (stageArgument.isVariable()) {
			xyRuleType = XYRuleType.X_RULE;
			stageVariable = (CompilerVariable) stageArgument;
		} else if ((stageArgument.getType() == CompilerType.ARITHMETIC_EXPRESSION) 
				&& ((CompilerArithmeticExpression)stageArgument).isBinary()) {
			//CompilerFunctor successor = (CompilerFunctor) stageArgument;
			CompilerArithmeticExpression successor = (CompilerArithmeticExpression)stageArgument; 
			xyRuleType = XYRuleType.NONE;

			//if (successor.getFunctorName().equals(XY_SUCCESSOR_SYMBOL)
			if (successor.getOperation() == ArithmeticOperation.ADDITION) {
				if (successor.getArgument1().isVariable()) {
					if (CompilerType.isInteger(successor.getArgument2().getType()) 
							&& (((CompilerInt)successor.getArgument2()).getValue() == XY_SUCCESSOR_VALUE)) {
						xyRuleType = XYRuleType.Y_RULE;
						stageVariable = (CompilerVariable) successor.getArgument1();
					}
				} else if (successor.getArgument2().isVariable()) {
					if (CompilerType.isInteger(successor.getArgument1().getType())
							&& (((CompilerInt)successor.getArgument1()).getValue() == XY_SUCCESSOR_VALUE)) {
						xyRuleType = XYRuleType.Y_RULE;
						stageVariable = (CompilerVariable) successor.getArgument2();
					}
				}
			}
		} else {
			xyRuleType = XYRuleType.NONE;
		}

		return new Pair<>(xyRuleType, stageVariable);
	}

	private static boolean hasNegatedLiteralForDeleteRule(Predicate literal, CompilerVariableList headNonStageVariables, 
			CompilerVariable stageVariable) {
		boolean status = false;

		if (literal.isNegative()) {
			CompilerVariableList negatedLiteralVariableList = new CompilerVariableList();
			Utilities.getVariables(literal, negatedLiteralVariableList);

			status = true;
			for (int i = 1; i < negatedLiteralVariableList.size(); i++) {
				CompilerVariable var = negatedLiteralVariableList.get(i);

				status = (var == stageVariable || var.isAnonymous() 
						|| headNonStageVariables.contains(var));
				
				if (!status)
					break;
			}
		}
		return status;
	}

	private Pair<XYRuleType, Integer[]> findXYLiteralType(Predicate head, Predicate literal, 
			BasicClique basicClique, CompilerVariable stageVariable, XYRuleType xyRuleType, 
			int numberOfIdenticalsWithSimpleStage,
			int numberOfLiteralsWithSimpleStage,
			int numberOfLiteralsWithComplexStage,
			int numberOfDeletionNegatedLiterals,
			CompilerVariableList headNonStageVariables,
			CompilerVariableList nonStageVariables) {

		this.deALSContext.logTrace(logger, "Entering findXYLiteralType for {}", literal.toString());
						
		 /*@DATALOGFS APS 3/25/2013*/
		boolean isAccessReadTable = false;
		boolean isAccessFSReadTable = false;
		boolean isAggregate = false;
		boolean isFSAggregate = false;
		boolean isChoice = false;
		if (literal instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)literal;
			isAccessReadTable = bip.isReadAggregate();
			isAccessFSReadTable = bip.isReadAggregateFS();
			isAggregate = bip.isAggregate();
			isFSAggregate = bip.isFSAggregate();
			isChoice = bip.isChoice();
		}

		if (((literal.isDerived() || isAccessReadTable || isAccessFSReadTable || isAggregate || isFSAggregate)
				&& (basicClique.getDerivedPredicate(literal.getPredicateName(), literal.getArity()) != null))
				|| isChoice
				|| isXYClique(literal)) {
			switch (xyRuleType)
			{
			case EXIT_RULE:
			{
				// Exit rule can not have recursive literals
				xyRuleType = XYRuleType.NONE;
			}
			break;

			case X_RULE:
			{
				if ((!isSimpleStageArgument(literal, stageVariable)) ||
						hasIllegalStageVariableInNonStageArguments(literal, stageVariable))
					xyRuleType = XYRuleType.NONE;
			}
			break;

			case Y_RULE:
			{
				if (hasIllegalStageVariableInNonStageArguments(literal, stageVariable)) {
					xyRuleType = XYRuleType.NONE;
				} else if (hasIdenticalLiteral(head, literal, stageVariable)) {
					numberOfIdenticalsWithSimpleStage++;
					numberOfLiteralsWithSimpleStage++;					
				} else if (isSimpleStageArgument(literal, stageVariable)) {
					// we should allow derived negative literal. -- HW
					if (hasNegatedLiteralForDeleteRule(literal, headNonStageVariables, stageVariable)) {
						numberOfDeletionNegatedLiterals++;
					} else {
						numberOfLiteralsWithSimpleStage++;
						getNonStageVariables(literal, nonStageVariables);
					}
				} else if (isComplexStageArgument(literal, stageVariable)) {					
					// we should allow derived negative literal. -- HW
					if (hasNegatedLiteralForDeleteRule(literal, headNonStageVariables, stageVariable)) {
						numberOfDeletionNegatedLiterals++;                        
					} else {
						getNonStageVariables(literal, nonStageVariables);						
					}
					numberOfLiteralsWithComplexStage++;
				} else {
					xyRuleType = XYRuleType.NONE;					
				}
			}
			break;

			case NONE:
				// Should not happen
				break;
			}
		} else if (hasIllegalStageVariableInAllArguments(literal, stageVariable)) {
			xyRuleType = XYRuleType.NONE;
		} else if (hasNegatedLiteralForDeleteRule(literal, headNonStageVariables, stageVariable)) {
			numberOfDeletionNegatedLiterals++;
		} else {
			Utilities.getVariables(literal, nonStageVariables);
		}

		this.deALSContext.logTrace(logger, "Exiting findXYLiteralType with {}", xyRuleType.name());

		Integer[] ints = new Integer[] {numberOfIdenticalsWithSimpleStage, numberOfLiteralsWithSimpleStage, 
				numberOfLiteralsWithComplexStage, numberOfDeletionNegatedLiterals};

		return new Pair<> (xyRuleType, ints);
	}

	private static boolean hasIdenticalLiteral(Predicate head, Predicate literal, CompilerVariable stageVariable) {
		if (isSimpleStageArgument(literal, stageVariable)
				&& head.getPredicateName().equals(literal.getPredicateName())
				&& (head.getArity() == literal.getArity())) {
			CompilerTypeList headArguments = head.getArguments();

			// Skip the first argument - it is the xy stage argument
			for (int i = 1; i < headArguments.size(); i++) {
				if (!Utilities.isMatch(headArguments.get(i), literal.getArgument(i))) {
					return false;
				}
			}
			
			return true;
		}

		return false;
	}

	private static void getNonStageVariables(Predicate literal, CompilerVariableList nonStageVariables) {
		CompilerTypeList arguments = literal.getArguments();

		// Skip the first argument
		for (int i = 1; i < arguments.size(); i++)
			Utilities.getVariables(arguments.get(i), nonStageVariables);
	}

	private static boolean isSimpleStageArgument(Predicate literal, CompilerVariable stageVariable) {
		if (stageVariable == literal.getArgument(XY_STAGE_POSITION))
			return true;
		
		if (literal.isBuiltIn() 
				&& (((BuiltInPredicate)literal).isChoice()) 
				&& literal.getArgument(XY_STAGE_POSITION).isFunctor()) {
			CompilerFunctor func = (CompilerFunctor) literal.getArgument(XY_STAGE_POSITION);
			if ((func.getArity() == 1) && (func.getArgument(0) == stageVariable))
				return true;
		}
		
		return false;
	}

	// this method looks to see if the literal has a I + 1 stage argument 
	private static boolean isComplexStageArgument(Predicate literal, CompilerVariable stageVariable) {
		CompilerTypeBase stageArgument = literal.getArgument(XY_STAGE_POSITION);

		/*if (stageArgument.isFunctor()) {
			CompilerFunctor successor = (CompilerFunctor)stageArgument;

			if ((successor.getFunctorName().equals(XY_SUCCESSOR_SYMBOL)) 
					&& (successor.getArity() == 2)) {
				if (successor.getArgument(0) == stageVariable){
					if ((successor.getArgument(1).getType() == CompilerType.COMPILER_INTEGER) 
							&& (((CompilerInteger)successor.getArgument(1)).getValue() == XY_SUCCESSOR_VALUE))
						return true;
				} else if (successor.getArgument(1) == stageVariable) {
					if ((successor.getArgument(0).getType() == CompilerType.COMPILER_INTEGER) 
							&& (((CompilerInteger)successor.getArgument(0)).getValue() == XY_SUCCESSOR_VALUE))
						return true;
				}
			}
		}*/
		
		if ((stageArgument.getType() == CompilerType.ARITHMETIC_EXPRESSION) 
				&& ((CompilerArithmeticExpression)stageArgument).isBinary()) {
			CompilerArithmeticExpression successor = (CompilerArithmeticExpression)stageArgument;
			if (successor.getOperation() == ArithmeticOperation.ADDITION) {
				if (successor.getArgument1() == stageVariable){
					if (CompilerType.isInteger(successor.getArgument2().getType()) 
							&& (((CompilerInt)successor.getArgument2()).getValue() == XY_SUCCESSOR_VALUE))
						return true;
				} else if (successor.getArgument2() == stageVariable) {
					if (CompilerType.isInteger(successor.getArgument1().getType()) 
							&& (((CompilerInt)successor.getArgument1()).getValue() == XY_SUCCESSOR_VALUE))
						return true;
				}
			}
		}

		return false;
	}

	private static boolean hasIllegalStageVariableInArguments(Predicate literal, CompilerVariable stageVariable, int startIndex) { 
		for (int i = startIndex; i < literal.getArity(); i++) {
			CompilerVariableList xyVariableList = new CompilerVariableList();
			
			Utilities.getVariables(literal.getArgument(i), xyVariableList);

			if (xyVariableList.contains(stageVariable))
				return true;
		}
		return false;
	}

	private static boolean hasIllegalStageVariableInNonStageArguments(Predicate literal, CompilerVariable stageVariable) {
		return hasIllegalStageVariableInArguments(literal, stageVariable, XY_STAGE_POSITION + 1);
	}

	private static boolean hasIllegalStageVariableInAllArguments(Predicate literal, CompilerVariable stageVariable) {
		return hasIllegalStageVariableInArguments(literal, stageVariable, XY_STAGE_POSITION);
	}

	private boolean hasXYCommonConstantInExitRules(BasicClique basicClique, List<Rule> exitRules) {
		this.deALSContext.logTrace(logger, "Entering hasXYCommonConstantInExitRules");
		this.deALSContext.logTrace(logger, "{}", basicClique.toString());

		
		boolean				status = true;
		CompilerTypeBase 	stageArgument;
		CompilerTypeBase 	stageConstant = null;

		for (Rule rule : exitRules) {
			stageArgument = rule.getHead().getArgument(XY_STAGE_POSITION);

			if (!stageArgument.isGround())
				status = false;
			else if (stageConstant == null)
				stageConstant = stageArgument;
			else if (!(stageConstant.equals(stageArgument)))
				status = false;
			
			if (!status)
				break;
		}

		this.deALSContext.logTrace(logger, "{}", basicClique.toString());
		this.deALSContext.logTrace(logger, "Exiting hasXYCommonConstantInExitRules with status = {}", status);
		
		return status;
	}

	private boolean isXYPrimedClique(BasicClique basicClique, List<Rule> xRules,
			List<Rule> yRules, List<Rule> copyRules,
			List<Rule> deleteRules, List<PredicateBase> sortedPrimedPredicates) {
		
		this.deALSContext.logTrace(logger, "Entering isXYPrimedClique");
		this.deALSContext.logTrace(logger, "{}", basicClique.toString());

		boolean status = isPrimedRecursive(basicClique, xRules, yRules, copyRules, deleteRules, sortedPrimedPredicates);

		this.deALSContext.logTrace(logger, "{}", basicClique.toString());
		this.deALSContext.logTrace(logger, "Exiting isXYPrimedClique with status = {}", status);

		return status;
	}

}
