package edu.ucla.cs.wis.bigdatalog.compiler.rewriting;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class Rewriter {
	private static Logger logger = LoggerFactory.getLogger(Rewriter.class.getName());

	public static final String UNIQUE_VARIABLE_PREFIX = "$rw$var_";
	public static final String DUMMY_PREDICATE_NAME_PREFIX = "$$_";
	public static final String MAGIC_PREDICATE_NAME_PREFIX = "$$_magic_";
	public static final String LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX = "$$_linear_supplementary_magic_";
	public static final String NON_LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX = "$$_non_linear_supplementary_magic_";
	public static final String GENERAL_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX = "$$_general_supplementary_magic_";
	public static final String REVERSE_MAGIC_PREDICATE_NAME_PREFIX = "$$_reverse_magic_";
	
	protected DeALSContext			deALSContext;
	protected List<CliqueBase> 	cliqueList;
	protected List<CliqueBase> 	localRewrittenCliqueList;
	protected List<CliqueBase> 	localNonRewrittenCliqueList;
	protected int 					variableCount;

	public Rewriter(DeALSContext deALSContext) {
		this.deALSContext 					= deALSContext;
		this.cliqueList 					= new ArrayList<>();
		this.localRewrittenCliqueList 		= null;
		this.localNonRewrittenCliqueList 	= null;
	}

	public PCGOrNode rewriteQueryForm(PCGOrNode orNode, List<CliqueBase> rewrittenCliqueList) {
		this.deALSContext.logTrace(logger, "Entering rewriteQueryForm for {}", orNode.toString());
		
		this.localRewrittenCliqueList		= rewrittenCliqueList;
		this.localNonRewrittenCliqueList	= new ArrayList<>();
		this.variableCount 					= 1;
		/*Compressor compressor 				= new Compressor(this);

		compressor.compressQueryForm(orNode);

		if (Runtime.isDebugEnabled()) {
			orNode.resetIsDescribed();
			this.deALSContext.logDebug(logger, "After first compressQueryForm: {}\n", orNode.toStringTree());
		}
		
		compressor.compressQueryForm(orNode);

		if (Runtime.isDebugEnabled()) {
			orNode.resetIsDescribed();
			this.deALSContext.logDebug(logger, "After second compressQueryForm: {}\n", orNode.toStringTree());
		}
		 */
		if (this.deALSContext.getConfiguration().compareProperty("deals.compiler.rewriter", "on"))
			this.rewriteOrNode(orNode);

		new FSRuleRewriter(this.deALSContext).optimize(orNode);
		
		this.deALSContext.logTrace(logger, "Exiting rewriteQueryForm for {}", orNode.toString());

		return orNode;
	}

	protected void rewriteOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering rewriteOrNode for {}", orNode.toString());
				
		if (orNode.isRecursive()) {
			this.rewriteClique(orNode);
		} else { 
			boolean rewrite = false;
			if (orNode.getPredicate().isDerived()) {
				rewrite = true;
			} else {
				if (orNode.getPredicate() instanceof BuiltInPredicate) {
					BuiltInPredicate bip = (BuiltInPredicate)orNode.getPredicate();
						
					if (bip.isAggregate() 
							|| bip.isFSAggregate()
							|| bip.isIfThenElse() 
							|| bip.isIfThen() 
							|| bip.isReadAggregate() 
							|| bip.isReadAggregateFS() /*APS 3/25/2013*/) {
						rewrite = true;
					}
				}
			}
			
			if (rewrite) {
				for (PCGOrNodeChild node : orNode.getChildren())
					this.rewriteAndNode((PCGAndNode)node);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting rewriteOrNode for {}", orNode.toString());
	}

	protected void rewriteAndNode(PCGAndNode andNode) {
		this.deALSContext.logTrace(logger, "Entering rewriteAndNode for {}", andNode.toString());
				
		for (PCGOrNode orNode : andNode.getChildren())
			this.rewriteOrNode(orNode);

		this.deALSContext.logTrace(logger, "Exiting rewriteAndNode for {}", andNode.toString());
	}

	protected void rewriteNonCliqueOrNodes(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering rewriteNonCliqueOrNodes");

		for (CliquePredicate cliquePredicate : clique.getCliquePredicates()) {
			for (PCGAndNode andNode : cliquePredicate.getExitRules())
				this.rewriteAndNode(andNode);
			
			// rewrite or nodes that aren't recursive with this clique
			for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
				for (PCGOrNode orNode : andNode.getChildren()) {
					if (!orNode.isRecursiveLiteral(clique))
						this.rewriteOrNode(orNode);
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting rewriteNonCliqueOrNodes");
	}

	protected void rewriteClique(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering rewriteClique for {}", orNode.toString());
					
		RewritingMethodType	rewritingMethod;
		Clique clique;
		CliqueBase baseClique = orNode.getBaseClique();

		if (this.cliqueList.contains(baseClique)) {
			orNode.setRewritingMethod(baseClique.getRewritingMethod());
		} else {
			this.cliqueList.add(baseClique);

			switch (baseClique.getType()) 
			{
			case CLIQUE:
			{
				rewritingMethod = orNode.getRewritingMethod();
				clique = (Clique) baseClique;

				if (rewritingMethod == RewritingMethodType.UNKNOWN) {
					rewritingMethod = this.decideRewritingMethod(clique);
					orNode.setRewritingMethod(rewritingMethod);
				}

				clique.setRewritingMethod(rewritingMethod);
				
				this.deALSContext.logInfo(logger, "[Rewriting Strategy for Clique {} is {}]", clique.getCliqueId(), rewritingMethod.toString());						
				
				this.rewriteNonCliqueOrNodes(clique);

				switch (rewritingMethod)
				{
				case TRIVIAL_PHASE1 :
				{
					this.rewriteTrivialPhaseOne(orNode);
					this.localRewrittenCliqueList.add(clique);
				}
				break;
				case TRIVIAL_PHASE2 :
				{
					this.rewriteTrivialPhaseTwo(orNode);
					this.localRewrittenCliqueList.add(clique);
				}
				break;
				case LINEAR_MAGIC :
				{
					this.rewriteLinearMagic(orNode);
					this.localRewrittenCliqueList.add(clique);
				}
				break;
				case NON_LINEAR_MAGIC :
				{
					this.rewriteNonLinearMagic(orNode);
					this.localRewrittenCliqueList.add(clique);
				}
				break;
				case GENERALIZED_MAGIC :
				{
					this.rewriteGeneralizedMagic(orNode);
					this.localRewrittenCliqueList.add(clique);
				}
				break;
				case SEMI_NAIVE:
				case NAIVE:
				case UNKNOWN:
				default :
				{
					this.localNonRewrittenCliqueList.add(clique);
				}
				break;
				}

				this.deALSContext.logTrace(logger, "{} with {}", orNode.toString(), clique.toString());
			}
			break;
			case XY_CLIQUE:
			{
			}
			break;

			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting rewriteClique for {}", orNode.toString());
	}

	protected RewritingMethodType isTrivialPhaseOne(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering isTrivialPhaseOne");
			
		RewritingMethodType rewritingMethod = RewritingMethodType.UNKNOWN;

		// can not be mutually recursive
		if (clique.getNumberOfCliquePredicates() == 1) {
			CliquePredicate cliquePredicate = clique.getCliquePredicate(0);
			
			rewritingMethod = RewritingMethodType.TRIVIAL_PHASE1;

			for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
				if (andNode.getBindingPattern().allFree()) {
					if (clique.isLinearRecursive())
						rewritingMethod = RewritingMethodType.SEMI_NAIVE;
					else
						if (clique.isNonLinearRecursive())
							rewritingMethod = RewritingMethodType.NAIVE;
				} else {
					for (PCGOrNode orNode : andNode.getChildren()) {
						// bound arguments must match
						if (orNode.isRecursiveLiteral(clique) 
								&& (!Utilities.hasSameBoundArguments(andNode, orNode))) {
							rewritingMethod = RewritingMethodType.UNKNOWN;
							break;
						}
					}
				}

				if (rewritingMethod != RewritingMethodType.TRIVIAL_PHASE1)
					break;
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting isTrivialPhaseOne with rewritingMethod = {}", rewritingMethod);
				
		return rewritingMethod;
	}

	protected RewritingMethodType isTrivialPhaseTwo(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering isTrivialPhaseTwo");
		
		RewritingMethodType rewritingMethod = RewritingMethodType.UNKNOWN;
						
		// can not be mutually recursive
		if (clique.getNumberOfCliquePredicates() == 1 && clique.isLinearRecursive()) {
			CliquePredicate cliquePredicate = clique.getCliquePredicate(0);

			rewritingMethod = RewritingMethodType.TRIVIAL_PHASE2;

			for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
				if (andNode.getBindingPattern().allFree()) {
					rewritingMethod = RewritingMethodType.SEMI_NAIVE;
				} else {
					int count = 0;
					for (PCGOrNode orNode : andNode.getChildren()) {
						// recursive literal has to be the right most
						// free arguments must match
						if (orNode.isRecursiveLiteral(clique) 
								&&  ((count + 1) != andNode.getNumberOfChildren() 
								|| (!Utilities.hasSameFreeArguments(andNode, orNode)))) {
							rewritingMethod = RewritingMethodType.UNKNOWN;
							break;
						}
						count++;
					}
				}

				if (rewritingMethod != RewritingMethodType.TRIVIAL_PHASE2)
					break;
			}
		}

		this.deALSContext.logTrace(logger, "Exiting isTrivialPhaseTwo with rewritingMethod = {}", rewritingMethod);
				
		return rewritingMethod;
	}

	/* If the binding are all free, no reason to do magic, we might as well do semi-naive
	 *  The chain-rule ensures that if the recursive head is not all-free, the
	 *  recursive literal can not be all free. Otherwise, we won't do magic
	 *  e.g.    p_ff <- ... p_bf ...
	 *          p_bf <- ... p_ff ...    <== fail chain-rule
	 *  will fail magic test  because if we have p_ff in head, other recursive rules
	 *  with p_ff in body will fail chain-rule
	 * Thus, we can assume a clique with p_ff in rule head will fail magic test */
	protected RewritingMethodType isLinearMagic(PCGAndNode recursiveHead, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering isLinearMagic for {}", recursiveHead.toString());
		
		RewritingMethodType rewritingMethod = RewritingMethodType.SEMI_NAIVE;

		if (!recursiveHead.getBindingPattern().allFree()) {
			CompilerVariableList chainVariableList = new CompilerVariableList();
			CompilerVariableList literalBoundVariableList = new CompilerVariableList();

			Utilities.getBoundVariables(recursiveHead.getArguments(), recursiveHead.getBindingPattern(), chainVariableList);

			for (PCGOrNode orNode : recursiveHead.getChildren()) {
				literalBoundVariableList.clear();
				Utilities.getBoundVariables(orNode.getArguments(), orNode.getBindingPattern(), literalBoundVariableList);

				if (orNode.isRecursiveLiteral(clique)) {
					if (!literalBoundVariableList.isEmpty() 
							&& Utilities.isSubset(literalBoundVariableList, chainVariableList))
						rewritingMethod = RewritingMethodType.LINEAR_MAGIC;
					break;
				}
				if (Utilities.intersects(literalBoundVariableList, chainVariableList))
					Utilities.getVariables(orNode.getArguments(), chainVariableList);
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting isLinearMagic for {} with rewritingMethod = {}", recursiveHead.toString(), rewritingMethod);

		return rewritingMethod;
	}

	protected RewritingMethodType isNonLinearMagic(PCGAndNode recursiveHead, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering isNonLinearMagic");
		
		RewritingMethodType rewritingMethod = RewritingMethodType.UNKNOWN;

		if (!recursiveHead.getBindingPattern().allFree()) {
			CompilerVariableList chainVariableList = new CompilerVariableList();
			CompilerVariableList literalBoundVariableList = new CompilerVariableList();

			Utilities.getBoundVariables(recursiveHead.getArguments(), recursiveHead.getBindingPattern(), chainVariableList);

			rewritingMethod = RewritingMethodType.NON_LINEAR_MAGIC;

			for (PCGOrNode orNode : recursiveHead.getChildren()) {				
				literalBoundVariableList.clear();
				Utilities.getBoundVariables(orNode.getArguments(), orNode.getBindingPattern(),literalBoundVariableList);

				// do not update chain-set of recursive literal because we assume it is invisible
				if (orNode.isRecursiveLiteral(clique)) {
					if (Utilities.intersects(literalBoundVariableList, chainVariableList)) {
						rewritingMethod = RewritingMethodType.UNKNOWN;
						break;
					}
				} else {
					if (Utilities.intersects(literalBoundVariableList, chainVariableList))
						Utilities.getVariables(orNode.getArguments(), chainVariableList);
				}
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting isNonLinearMagic with rewritingMethod = {}", rewritingMethod);

		return rewritingMethod;
	}

	protected RewritingMethodType isGeneralizedMagic(PCGAndNode recursiveHead, Clique clique) {		
		this.deALSContext.logTrace(logger, "Entering isGeneralizedMagic");
		
		RewritingMethodType rewritingMethod = RewritingMethodType.NAIVE;

		if (!recursiveHead.getBindingPattern().allFree()) {
			CompilerVariableList chainVariableList = new CompilerVariableList();
			CompilerVariableList literalBoundVariableList = new CompilerVariableList();

			Utilities.getBoundVariables(recursiveHead.getArguments(), recursiveHead.getBindingPattern(), chainVariableList);

			rewritingMethod = RewritingMethodType.GENERALIZED_MAGIC;

			for (PCGOrNode orNode : recursiveHead.getChildren()) {
				literalBoundVariableList.clear();
				Utilities.getBoundVariables(orNode.getArguments(), orNode.getBindingPattern(), literalBoundVariableList);

				// we update the chain-set here
				if (orNode.isRecursiveLiteral(clique)) {
					if (Utilities.intersects(literalBoundVariableList, chainVariableList)) {
						Utilities.getVariables(orNode.getArguments(), chainVariableList);
					} else {
						rewritingMethod = RewritingMethodType.NAIVE;
						break;
					}
				} else {
					if (Utilities.intersects(literalBoundVariableList, chainVariableList))
						Utilities.getVariables(orNode.getArguments(), chainVariableList);
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting isGeneralizedMagic with rewritingMethod = {}", rewritingMethod);
		
		return rewritingMethod;
	}

	protected RewritingMethodType getMagicRewritingType(Clique clique, RewritingMethodType rewritingMethod) {
		this.deALSContext.logTrace(logger, "Entering getMagicRewritingType");
		
		RewritingMethodType newRewritingMethod = RewritingMethodType.UNKNOWN;

		for (CliquePredicate cliquePredicate : clique.getCliquePredicates()) {

			for (PCGAndNode orNode : cliquePredicate.getRecursiveRules()) {

				switch (rewritingMethod)
				{
				case LINEAR_MAGIC:
					newRewritingMethod = this.isLinearMagic(orNode, clique);
					break;
				case NON_LINEAR_MAGIC:
					newRewritingMethod = this.isNonLinearMagic(orNode, clique);
					break;
				case GENERALIZED_MAGIC:
					newRewritingMethod = this.isGeneralizedMagic(orNode, clique);
					break;
				default:
					break;
				}

				if (newRewritingMethod != rewritingMethod)
					break;
			}

			if (newRewritingMethod != rewritingMethod)
				break;
		}
		
		this.deALSContext.logTrace(logger, "Exiting isGeneralizedMagic with rewritingMethod = {}", newRewritingMethod);

		return newRewritingMethod;
	}

	/* Current Procedure for Deciding Rewriting Strategy - Pending Revision
	 *
	 * Trivial Phase 1
	 *  1. same recursive predicate in head and literal
	 *     - including predicate name, arity and binding pattern
	 *  2. bound arguments in recursive head and recursive literal are identical
	 *     - including functors - we do pattern match but variables must be identical
	 *     - must be positionally identical
	 *  3. has some bound arguments in the recursive head
	 *  4. no mutual recursion
	 *
	 * Trivial Phase 2
	 *  1. same recursive predicate in head and literal
	 *     - including predicate name, arity and binding pattern
	 *  2. free arguments in recursive head and recursive literal are identical
	 *     - including functors - we do pattern match but variables must be identical
	 *     - must be positionally identical
	 *  3. linear recursion
	 *  4. has some bound arguments in the recursive head
	 *  5. must be the right most predicate
	 *  6. no mutual recursion
	 *
	 * Linear Magic
	 *  1. linear recursion
	 *  2. all bound variables in the recursive literal must be chained to the bound variables from the head
	 *  3. has some bound arguments in the recursive head
	 *  4. If any one of the recursive rule fails the chain requirements, we default to Semi-Naive ???
	 *
	 * Non-Linear Magic
	 *  1. non-linear recursion
	 *  2. all bound variables in the recursive literal must be chained to the bound
	 *      variables from the head
	 *  3. has some bound arguments in the recursive head
	 *  4. we pretend that the recursive literal does not use binding from previous recursive literals
	 *
	 * Generalized Magic
	 *  1. non-linear recursion
	 *  2. some bound variables in the recursive literal must be chained to the bound variables from the head
	 *  3. has some bound arguments in the recursive head
	 *
	 * Semi-Naive or Do Nothing
	 *  1. linear recursion
	 *  2. any linear recursion that can not be handled by trivial phase one or two or linear magic
	 *
	 * Naive or Do Nothing
	 *  1. non-linear recursion
	 *  2. any non-linear recursion that can not be handled by trivial phase one or non-linear or generalized magic
	 */
	protected RewritingMethodType decideLinearRecursiveRewritingMethod(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering decideLinearRecursiveRewritingMethod");
		
		RewritingMethodType rewritingMethod = this.isTrivialPhaseTwo(clique);
	
		if (rewritingMethod == RewritingMethodType.UNKNOWN)
			rewritingMethod = this.isTrivialPhaseOne(clique);
	
		if (rewritingMethod == RewritingMethodType.UNKNOWN)
			rewritingMethod = this.getMagicRewritingType(clique, RewritingMethodType.LINEAR_MAGIC);
	
		if (rewritingMethod == RewritingMethodType.UNKNOWN)
			rewritingMethod = RewritingMethodType.SEMI_NAIVE;
		
		this.deALSContext.logTrace(logger, "Exiting decideLinearRecursiveRewritingMethod with rewritingMethod = {}", rewritingMethod);

		return rewritingMethod;
	}

	protected RewritingMethodType decideNonLinearRecursiveRewritingMethod(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering decideNonLinearRecursiveRewritingMethod");
		
		RewritingMethodType rewritingMethod = this.isTrivialPhaseOne(clique);

		if (rewritingMethod == RewritingMethodType.UNKNOWN)
			rewritingMethod = this.getMagicRewritingType(clique, RewritingMethodType.NON_LINEAR_MAGIC);

		if (rewritingMethod == RewritingMethodType.UNKNOWN)
			rewritingMethod = this.getMagicRewritingType(clique, RewritingMethodType.GENERALIZED_MAGIC);

		if (rewritingMethod == RewritingMethodType.UNKNOWN)
			rewritingMethod = RewritingMethodType.NAIVE;

		this.deALSContext.logTrace(logger, "Exiting decideNonLinearRecursiveRewritingMethod with rewritingMethod = {}", rewritingMethod);
		
		return rewritingMethod;
	}

	protected RewritingMethodType decideRewritingMethod(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering decideRewritingMethod");

		RewritingMethodType rewritingMethod = RewritingMethodType.UNKNOWN;

		clique.determineRecursiveType();

		if (clique.isLinearRecursive())
			rewritingMethod = this.decideLinearRecursiveRewritingMethod(clique);
		else if (clique.isNonLinearRecursive())
			rewritingMethod = this.decideNonLinearRecursiveRewritingMethod(clique);

		this.deALSContext.logTrace(logger, "Exiting decideRewritingMethod with rewritingMethod = {}", rewritingMethod);

		return rewritingMethod;
	}

	protected void createGeneralSupplementaryMagicRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering createGeneralSupplementaryMagicRules");
		
		for (int i = 0; i < cliquePredicate.getNumberOfRecursiveRules(); i++) {
			int	numberOfRecursiveLiterals = 0;
			int beginLiteralPosition = 0;
			int	endLiteralPosition = 0;
			int	previousEndLiteralPosition = 0;
			int	ruleNumber = i + 1;
			PCGAndNode recursiveHead = cliquePredicate.getRecursiveRule(i);
			PCGOrNode previousRecursiveLiteral = null;

			for (int j = 0; j < recursiveHead.getNumberOfChildren(); j++) {
				PCGOrNode literal = recursiveHead.getChild(j);

				if (literal.isRecursiveLiteral(clique) && !Utilities.hasSameBoundArguments(recursiveHead, literal)) {
					CompilerVariableList variableList = new CompilerVariableList();
					CliquePredicate supplementaryMagicCliquePredicate = null;
					PCGAndNode 		supplementaryMagicHead;
					PCGOrNode 		magicLiteral;
					PCGOrNode		supplementaryMagicLiteral = null;

					previousEndLiteralPosition = endLiteralPosition;
					endLiteralPosition = j;
					numberOfRecursiveLiterals++;

					// step 1: create new supplementary magic_head and cliquePredicate
					Pair<PCGAndNode, CliquePredicate> retvalPair = this.createGeneralSupplementaryMagicCliquePredicateAndNode(clique, literal, recursiveHead, ruleNumber,
							numberOfRecursiveLiterals, endLiteralPosition, variableList);

					supplementaryMagicHead = retvalPair.getFirst();
					supplementaryMagicCliquePredicate = retvalPair.getSecond();

					// step 2: if it is the first magic predicate, we generate magic literal
					//	  else supplementary magic literal
					if (numberOfRecursiveLiterals == 1) {
						// step 2: create new magicLiteral with bound arguments of recursiveHead
						magicLiteral = this.createMagicOrNode(recursiveHead, variableList);
						magicLiteral.setAsRecursive();
						magicLiteral.setBaseClique(clique);
						clique.addParent(magicLiteral);
						supplementaryMagicHead.addChild(magicLiteral);
					} else {
						supplementaryMagicLiteral = this.createGeneralSupplementaryMagicOrNode(previousRecursiveLiteral,
								recursiveHead,
								ruleNumber,
								numberOfRecursiveLiterals - 1,
								previousEndLiteralPosition,
								variableList);
						supplementaryMagicLiteral.setAsRecursive();
						supplementaryMagicLiteral.setBaseClique(clique);
						clique.addParent(supplementaryMagicLiteral);
						supplementaryMagicHead.addChild(supplementaryMagicLiteral);
					}

					// step 3: copy all or nodes between previous and current recursive literal to the supplementary magic head
					for (int k = beginLiteralPosition; k < endLiteralPosition; k++) {
						PCGOrNode leftLiteral = recursiveHead.getChild(k);
						PCGOrNode leftLiteralCopy = leftLiteral.copyTree(variableList);

						if (leftLiteral.isRecursiveLiteral(clique))
							clique.addParent(leftLiteralCopy);

						supplementaryMagicHead.addChild(leftLiteralCopy);
					}

					// step 4: attach the new and node to the clique
					supplementaryMagicCliquePredicate.addRecursiveRule(supplementaryMagicHead);

					beginLiteralPosition = endLiteralPosition;
					previousRecursiveLiteral = literal;
				}
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting createGeneralSupplementaryMagicRules");
	}

	protected void createGeneralMagicRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering createGeneralMagicRules");
		
		for (int i = 0; i < cliquePredicate.getNumberOfRecursiveRules(); i++) {
			int ruleNumber = i + 1;
			int numberOfRecursiveLiterals = 0;
			PCGAndNode recursiveHead = cliquePredicate.getRecursiveRule(i);

			for (int j = 0; j < recursiveHead.getNumberOfChildren(); j++) {
				PCGOrNode literal = recursiveHead.getChild(j);

				if (literal.isRecursiveLiteral(clique) &&
						!Utilities.hasSameBoundArguments(recursiveHead, literal)) {
					CompilerVariableList variableList = new CompilerVariableList();
					int recursiveLiteralPosition = j;
					numberOfRecursiveLiterals++;

					CliquePredicate magicCliquePredicate = this.createMagicCliquePredicate(clique, literal);
					PCGAndNode magicHead = this.createMagicAndNode(literal, variableList);
					PCGOrNode supplementaryMagicLiteral = this.createGeneralSupplementaryMagicOrNode(literal,
							recursiveHead,
							ruleNumber,
							numberOfRecursiveLiterals,
							recursiveLiteralPosition,
							variableList);
					supplementaryMagicLiteral.setAsRecursive();
					supplementaryMagicLiteral.setBaseClique(clique);
					clique.addParent(supplementaryMagicLiteral);
					magicHead.addChild(supplementaryMagicLiteral);
					magicCliquePredicate.addRecursiveRule(magicHead);
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting createGeneralMagicRules");
	}

	protected void modifyGeneralMagicRecursiveRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering modifyGeneralMagicRecursiveRules");
		
		for (int i = 0; i < cliquePredicate.getNumberOfRecursiveRules(); i++) {
			int numberOfRecursiveLiterals = 0;
			int lastRecursiveLiteralPosition = 0;
			int ruleNumber = i + 1;
			PCGAndNode recursiveHead = cliquePredicate.getRecursiveRule(i);
			PCGOrNode recursiveLiteral = null;
			PCGOrNode literal;
			PCGOrNode supplementaryMagicLiteral;

			// step 1: find the last recursive literal that DOES NOT have the
			//         identical bound arguments as the recursive head
			for (int j = 0; j < recursiveHead.getNumberOfChildren(); j++) {
				literal = recursiveHead.getChild(j);

				if (literal.isRecursiveLiteral(clique) &&
						!Utilities.hasSameBoundArguments(recursiveHead, literal)) {
					lastRecursiveLiteralPosition = j;
					recursiveLiteral = literal;
					numberOfRecursiveLiterals++;
				}
			}

			// If there does not exist any recursive predicate with either different predicate name,
			//  or arity or binding or bound arguments, no supplementary node is added to the modified
			//  recursive rules - We need to verify this with Sergio to determine the actual algorithm
			//  Somehow, the algorithm he specified does not cover this case !!
			if (recursiveLiteral != null) {
				// step 2: create the supplementary magic literal based on the last recursive literal
				supplementaryMagicLiteral = this.createGeneralSupplementaryMagicOrNode(recursiveLiteral,
						recursiveHead,
						ruleNumber,
						numberOfRecursiveLiterals,
						lastRecursiveLiteralPosition);
				supplementaryMagicLiteral.setAsRecursive();
				supplementaryMagicLiteral.setBaseClique(clique);
				clique.addParent(supplementaryMagicLiteral);

				// step 3: destroy all predicate left of last recursive literal
				for (int j = lastRecursiveLiteralPosition - 1; j >= 0; j--)
					recursiveHead.removeChild(j).delete();
				
				// step 4: put supplementary magic literal as the left most literal
				recursiveHead.prependChild(supplementaryMagicLiteral);
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting modifyGeneralMagicRecursiveRules");
	}

	protected void rewriteGeneralizedMagic(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering rewriteGeneralizedMagic for {}", orNode.toString());

		if (orNode.getBindingPattern().allFree()) {
			this.deALSContext.logError(logger, "cannot apply magic set technique.  all-free predicate found in generalized magic rewriting");
			throw new CompilerException("cannot apply magic set technique.  all-free predicate found in generalized magic rewriting");
		}
		
		Clique clique = (Clique)orNode.getBaseClique();

		for (int i = 0; i < clique.getNumberOfCliquePredicates(); i++) {
			CliquePredicate cliquePredicate = clique.getCliquePredicate(i);

			this.deALSContext.logInfo(logger, "Rewriting Clique Predicate for {}", cliquePredicate.toString());
			
			if (!cliquePredicate.isRewritten()) {
				cliquePredicate.setRewritten();

				this.createGeneralMagicRules(clique, cliquePredicate);

				this.createGeneralSupplementaryMagicRules(clique, cliquePredicate);

				this.modifyLinearMagicExitRules(clique, cliquePredicate);

				this.modifyGeneralMagicRecursiveRules(clique, cliquePredicate);
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting rewriteGeneralizedMagic for {}", orNode.toString());
	}

	protected void createLinearMagicRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering createLinearMagicRules");
		
		CompilerVariableList variableList;
		int recursiveLiteralPosition;
		PCGOrNode recursiveLiteral;
		CliquePredicate magicCliquePredicate;
		PCGAndNode magicHead;
		PCGOrNode magicLiteral;
		PCGOrNode literal;

		for (PCGAndNode recursiveHead : cliquePredicate.getRecursiveRules()) {
			variableList = new CompilerVariableList();
			recursiveLiteralPosition = Rewriter.getFirstRecursiveLiteralPosition(recursiveHead, clique);
			recursiveLiteral = recursiveHead.getChild(recursiveLiteralPosition);

			magicCliquePredicate = this.createMagicCliquePredicate(clique, recursiveLiteral);

			// step 1: create new magicHead with bound arguments of recursiveLiteral
			magicHead = this.createMagicAndNode(recursiveLiteral, variableList);

			// step 2: create new magicLiteral with bound arguments of recursiveHead
			magicLiteral = this.createMagicOrNode(recursiveHead, variableList);
			magicLiteral.setAsRecursive();
			magicLiteral.setBaseClique(clique);
			clique.addParent(magicLiteral);
			magicHead.addChild(magicLiteral);		// always the first

			// step 3: link all or nodes before recursive literal to the magic head
			for (int j = 0; j < recursiveLiteralPosition; j++) {
				literal = recursiveHead.getChild(j);
				magicHead.addChild(literal.copyTree(variableList));
			}

			// step 4: attach the new and node to the clique
			magicCliquePredicate.addRecursiveRule(magicHead);
		}
		
		this.deALSContext.logTrace(logger, "Exiting createLinearMagicRules");
	}

	// we need to do supplementary magic for linear recursion for the following reason:
	// Given a recursive program:  p <- e.   p <- a, p, b.
	// Performing magic rewrite:  $m.   m <- m, a.   p <- m, e.   p <- m, a, p, b.
	// Traditional supplementary:  $m.  sm <- m, a.  m <- sm.   p <- m, e.  p <- sm, p, b.
	// This is too expensive in terms of space since we have one extra recursive relation.
	// So, we use another form of supplementary:
	//   $m.  m <- m, a.  sm <- m, a.   p <- m, e.  p <- sm, p, b.
	// Notice that all results in sm are always sharable in each iteration of fixpoint computation.
	// Here, even though we do an extra join in the sm, we saved all results in the supplementary
	//  and thus, in each iteration, we do not need to recompute the sm results.
	// However, we want to optimize further by switching the order of sm and p as follows:
	//   $m.  m <- m, a.  sm <- m, a.   p <- m, e.  p <- p, sm, b.
	// Since p, i.e. delta p is presumed to be small. Since sm will have some bound values,
	//  we build indices for the sm relation.
	// This is also better than the following:
	//   $m.  m <- m, a.  p <- m, e.  p <- p, m, a, b.
	//  since we have to join m & a for every delta p, which is less efficient.
	protected void createLinearSupplementaryMagicRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering createLinearSupplementaryMagicRules");
		
		CompilerVariableList	variableList;
		PCGAndNode 		recursiveHead;
		int 			recursiveLiteralPosition;
		CliquePredicate supplementaryMagicCliquePredicate = null;
		PCGAndNode 		supplementaryMagicHead;
		PCGOrNode 		magicLiteral;
		PCGOrNode 		literal;

		for (int i = 0; i < cliquePredicate.getNumberOfRecursiveRules(); i++) {
			variableList = new CompilerVariableList();
			recursiveHead = cliquePredicate.getRecursiveRule(i);
			recursiveLiteralPosition = Rewriter.getFirstRecursiveLiteralPosition(recursiveHead, clique);

			// step 1: create new supplementary magic_head and cliquePredicate
			String 	predicateName = recursiveHead.getPredicateName();
			Binding 		binding = recursiveHead.getBindingPattern();
			String 	supplementaryMagicPredicateName 
				= Rewriter.createLinearSupplementaryMagicPredicateName(predicateName, binding, i + 1, 1);

			Pair<PCGAndNode, CliquePredicate> retvalPair 
				= this.createCommonSupplementaryMagicCliquePredicateAndNode(clique, recursiveHead, 
					recursiveLiteralPosition, variableList, supplementaryMagicPredicateName);	
			
			supplementaryMagicHead = retvalPair.getFirst();
			supplementaryMagicCliquePredicate = retvalPair.getSecond();

			// step 2: create new magicLiteral with bound arguments of recursiveHead
			magicLiteral = this.createMagicOrNode(recursiveHead, variableList);
			magicLiteral.setAsRecursive();
			magicLiteral.setBaseClique(clique);
			clique.addParent(magicLiteral);
			supplementaryMagicHead.addChild(magicLiteral);

			// step 3: link all or nodes before recursive literal to the magic head
			for (int j = 0; j < recursiveLiteralPosition; j++) {
				literal = recursiveHead.getChild(j);
				supplementaryMagicHead.addChild(literal.copyTree(variableList));
			}

			// step 4: attach the new and node to the clique
			supplementaryMagicCliquePredicate.addExitRule(supplementaryMagicHead);
		}
		
		this.deALSContext.logTrace(logger, "Exiting createLinearSupplementaryMagicRules");
	}

	protected void modifyLinearMagicRecursiveRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering modifyLinearMagicRecursiveRules");
		
		PCGAndNode recursiveRule;
		int recursiveLiteralPosition;
		PCGOrNode supplementaryMagicLiteral;

		for (int i = 0; i < cliquePredicate.getNumberOfRecursiveRules(); i++) {
			recursiveRule = cliquePredicate.getRecursiveRule(i);
			//if (recursiveRule.getNumberOfChildren() == 1)
			//	if (recursiveRule.getChild(0).getPredicateName().startsWith(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX))
			//		continue;
			recursiveLiteralPosition = Rewriter.getFirstRecursiveLiteralPosition(recursiveRule, clique);

			// step 1: create new supplementary magic literal
			//supplementaryMagicLiteral = this.createLinearSupplementaryMagicOrNode(recursiveHead, i+1, recursiveLiteralPosition);			
			String predicateName = recursiveRule.getPredicateName();
			Binding	binding = recursiveRule.getBindingPattern();
			String supplementaryMagicPredicateName 
				= Rewriter.createLinearSupplementaryMagicPredicateName(predicateName, binding, i+1, 1);

			supplementaryMagicLiteral = this.createCommonSupplementaryMagicOrNode(recursiveRule, 
													recursiveLiteralPosition, supplementaryMagicPredicateName);			
			supplementaryMagicLiteral.setAsRecursive();
			supplementaryMagicLiteral.setBaseClique(clique);
			clique.addParent(supplementaryMagicLiteral);

			// step 2: eliminate all literals to the left hand side of the recursive literal
			for (int j = (recursiveLiteralPosition - 1); j >= 0; j--)
				recursiveRule.removeChild(j).delete();

			// step 3: put supplementary literal after recursive literal
			recursiveRule.insertChild(1, supplementaryMagicLiteral);
		}
		
		this.deALSContext.logTrace(logger, "Exiting modifyLinearMagicRecursiveRules");
	}

	protected void modifyLinearMagicExitRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering modifyLinearMagicExitRules");
		
		PCGOrNode magicLiteral;
		List<PCGOrNode> equalityList = new ArrayList<>();

		// construct magic literal from bound arguments of the exitHead
		for (PCGAndNode exitHead : cliquePredicate.getExitRules()) {
			// We rewrite the head by replacing those bound argument with complex term 
			// with a unique variable so that they are sharable with the magic literal
			equalityList.clear();
			this.extractComplexTermsFromPcgAndNode(exitHead, equalityList);

			magicLiteral = this.createMagicOrNode(exitHead);
			magicLiteral.setAsRecursive();
			magicLiteral.setBaseClique(clique);
			clique.addParent(magicLiteral);

			exitHead.prependChild(magicLiteral);

			// Place the equality after the magic literal
			for (int j = equalityList.size() - 1; j >= 0; j--)
				exitHead.insertChild(1, equalityList.get(j));
		}

		equalityList.clear();
		
		this.deALSContext.logTrace(logger, "Exiting modifyLinearMagicExitRules");
	}

	protected void rewriteLinearMagic(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering rewriteLinearMagic for {}", orNode.toString());

		if (orNode.getBindingPattern().allFree()) {
			this.deALSContext.logError(logger, "Cannot apply magic set technique.  all-free predicate found in magic rewriting");			
			throw new CompilerException("Cannot apply magic set technique.  all-free predicate found in magic rewriting");
		}
		
		Clique clique = (Clique)orNode.getBaseClique();
		CliquePredicate cliquePredicate;
		
		for (int i = 0; i < clique.getNumberOfCliquePredicates(); i++) {
			cliquePredicate = clique.getCliquePredicate(i);

			this.deALSContext.logInfo(logger, "Rewriting Clique Predicate for {}", clique.toString());
			
			if (!cliquePredicate.isRewritten()) {
				cliquePredicate.setRewritten();
				
				this.createLinearMagicRules(clique, cliquePredicate);
				
				this.deALSContext.logInfo(logger, "After createLinearMagicRules()");
				this.deALSContext.logInfo(logger, "{}", clique.toString());

				this.createLinearSupplementaryMagicRules(clique, cliquePredicate);
				
				this.deALSContext.logInfo(logger, "After createLinearSupplementaryMagicRules() ");
				this.deALSContext.logInfo(logger, "{}", clique.toString());

				this.modifyLinearMagicExitRules(clique, cliquePredicate);
				
				this.deALSContext.logInfo(logger, "After modifyLinearMagicExitRules() ");
				this.deALSContext.logInfo(logger, "{}", clique.toString());

				this.modifyLinearMagicRecursiveRules(clique, cliquePredicate);

				this.deALSContext.logInfo(logger, "After modifyLinearMagicRecursiveRules() ");
				this.deALSContext.logInfo(logger, "{}", clique.toString());
			}
		}

		if (this.deALSContext.isTraceEnabled())
			this.deALSContext.logTrace(logger, "Exiting rewriteLinearMagic for {}", orNode.toString());
	}

	protected void createNonLinearMagicRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering createNonLinearMagicRules");
		
		for (PCGAndNode recursiveHead : cliquePredicate.getRecursiveRules()) {
			for (int j = 0; j < recursiveHead.getNumberOfChildren(); j++) {
				CompilerVariableList variableList = new CompilerVariableList();
				PCGOrNode literal = recursiveHead.getChild(j);

				// we generate magic iff the bound arguments in the recursive literal are not
				//  identical to that in the recursive head
				if (literal.isRecursiveLiteral(clique) && 
						!Utilities.hasSameBoundArguments(recursiveHead, literal)) {
					CliquePredicate magicCliquePredicate = this.createMagicCliquePredicate(clique, literal);

					// step 1: create new magic_head with bound arguments of recursiveliterals
					PCGAndNode magicHead = this.createMagicAndNode(literal, variableList);

					// step 2: create new magicLiteral with bound arguments of recursiveHead
					PCGOrNode magicLiteral = this.createMagicOrNode(recursiveHead, variableList);
					magicLiteral.setAsRecursive();
					magicLiteral.setBaseClique(clique);
					clique.addParent(magicLiteral);
					magicHead.addChild(magicLiteral);		// always the first

					// step 3: link all non-self-recursive literal before the current recursive literal to the magic head
					PCGOrNode leftLiteral;
					for (int k = 0; k < j; k++) {
						leftLiteral = recursiveHead.getChild(k);

						if (!(leftLiteral.isRecursive() && leftLiteral.getChild(0).equals(clique)))
							magicHead.addChild(leftLiteral.copyTree(variableList));
					}

					// step 4: attach the new and node to the clique predicate
					magicCliquePredicate.addRecursiveRule(magicHead);
				}
			}
		}
	
		this.deALSContext.logTrace(logger, "Exiting createNonLinearMagicRules");
	}

	// we need to do supplementary magic for non-linear recursion for the following reason:
	// Given a recursive program:  p <- e.   p <- a, p, b, p, c.
	//   $m.  m <- m, a.  m <- m, a, b. p <- m, e.  p <- m, a, p, b, p, c.
	// Note that the join of m & a in the last rule is always repeated in each iteration.
	// So, we create supplementary for that join, i.e.
	//   $m.  m <- m, a.  m <- m, a, b. sm <- m, a. p <- m, e.  p <- sm, p, b, p, c.
	// Notice that all results in sm are always sharable in each iteration of fixpoint computation
	// since the join m & a is always free-free.
	// Here, even though we do an extra join in the sm, we saved all results in the supplementary
	//  and thus, in each iteration, we do not need to recompute the sm results.
	protected void createNonLinearSupplementaryMagicRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering createNonLinearSupplementaryMagicRules");
		
		CompilerVariableList variableList;
		PCGAndNode recursiveHead;
		int recursiveLiteralPosition;
		CliquePredicate supplementaryMagicCliquePredicate = null;
		PCGAndNode supplementaryMagicHead;
		PCGOrNode supplementaryMagicLiteral;

		for (int i = 0; i < cliquePredicate.getNumberOfRecursiveRules(); i++) {
			variableList = new CompilerVariableList();
			recursiveHead = cliquePredicate.getRecursiveRule(i);
			recursiveLiteralPosition = Rewriter.getFirstRecursiveLiteralPosition(recursiveHead, clique);

			// step 1: create new supplementary magic_head and cliquePredicate
			String predicateName = recursiveHead.getPredicateName();
			Binding binding = recursiveHead.getBindingPattern();  
			String supplementaryMagicPredicateName = Rewriter.createNonLinearSupplementaryMagicPredicateName(predicateName, binding, i+1, 1);

			Pair<PCGAndNode, CliquePredicate> retvalPair = this.createCommonSupplementaryMagicCliquePredicateAndNode(clique, recursiveHead, 
					recursiveLiteralPosition, variableList, supplementaryMagicPredicateName);
						
			supplementaryMagicHead = retvalPair.getFirst();
			supplementaryMagicCliquePredicate = retvalPair.getSecond();

			// step 2: create new magicLiteral with bound arguments of recursiveHead
			supplementaryMagicLiteral = this.createMagicOrNode(recursiveHead, variableList);
			supplementaryMagicLiteral.setAsRecursive();
			supplementaryMagicLiteral.setBaseClique(clique);
			clique.addParent(supplementaryMagicLiteral);
			supplementaryMagicHead.addChild(supplementaryMagicLiteral);

			// step 3: link all or nodes before recursive literal to the magic head
			for (int j = 0; j < recursiveLiteralPosition; j++)
				supplementaryMagicHead.addChild(recursiveHead.getChild(j).copyTree(variableList));

			// step 4: attach the new and node to the clique
			supplementaryMagicCliquePredicate.addExitRule(supplementaryMagicHead);
		}
		
		this.deALSContext.logTrace(logger, "Exiting createNonLinearSupplementaryMagicRules");
	}

	protected void modifyNonLinearMagicRecursiveRules(Clique clique, CliquePredicate cliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering modifyNonLinearMagicRecursiveRules");
		
		int recursiveLiteralPosition;
		PCGOrNode supplementaryMagicLiteral;

		int i = 0;
		for (PCGAndNode recursiveHead : cliquePredicate.getRecursiveRules()) {
			recursiveLiteralPosition = Rewriter.getFirstRecursiveLiteralPosition(recursiveHead, clique);

			// step 1: create new supplementary magic literal
			String predicateName = recursiveHead.getPredicateName();
			Binding binding = recursiveHead.getBindingPattern();
			String supplementaryMagicPredicateName = Rewriter.createNonLinearSupplementaryMagicPredicateName(predicateName, binding, i+1, 1);

			supplementaryMagicLiteral = this.createCommonSupplementaryMagicOrNode(recursiveHead, recursiveLiteralPosition, supplementaryMagicPredicateName);						
			supplementaryMagicLiteral.setAsRecursive();
			supplementaryMagicLiteral.setBaseClique(clique);
			clique.addParent(supplementaryMagicLiteral);

			// step 2: eliminate all literal to the left hand side of the recursive literal
			for (int j = recursiveLiteralPosition - 1; j >= 0; j--)
				recursiveHead.removeChild(j).delete();
			
			// step 3: put supplementary literal before recursive literal
			recursiveHead.insertChild(1, supplementaryMagicLiteral);
			i++;
		}
		
		this.deALSContext.logTrace(logger, "Exiting modifyNonLinearMagicRecursiveRules");
	}

	protected void modifyNonLinearMagicExitRules(Clique clique, CliquePredicate cliquePredicate) {
		// this routine is identical in the case of linear magic
		this.modifyLinearMagicExitRules(clique, cliquePredicate);
	}

	protected void rewriteNonLinearMagic(PCGOrNode orNode) {	  
		this.deALSContext.logTrace(logger, "Entering rewriteNonLinearMagic for {}", orNode.toString());
		
		if (orNode.getBindingPattern().allFree()) {
			this.deALSContext.logError(logger, "cannot apply magic set technique.  all-free predicate found in magic rewriting");
			throw new CompilerException("cannot apply magic set technique.  all-free predicate found in magic rewriting");
		}

		Clique clique = (Clique) orNode.getChild(0);
		CliquePredicate cliquePredicate; 
			
		for (int i = 0; i < clique.getNumberOfCliquePredicates(); i++) {
			cliquePredicate = clique.getCliquePredicate(i);
			
			this.deALSContext.logInfo(logger, "Rewriting Clique Predicate for {}", cliquePredicate.toString());			

			if (!cliquePredicate.isRewritten()) {
				cliquePredicate.setRewritten();

				this.createNonLinearMagicRules(clique, cliquePredicate);

				this.createNonLinearSupplementaryMagicRules(clique, cliquePredicate);

				this.modifyNonLinearMagicExitRules(clique, cliquePredicate);

				this.modifyNonLinearMagicRecursiveRules(clique, cliquePredicate);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting rewriteNonLinearMagic for {}", orNode.toString());
	}

	protected void modifyTrivialPhaseOneRecursiveRules(Clique clique, CliquePredicate newCliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering modifyTrivialPhaseOneRecursiveRules");
		
		String				newPredicateName = newCliquePredicate.getPredicateName();
		Binding				newBinding = newCliquePredicate.getBindingPattern();
		CompilerVariableList 		headBoundVariableList = new CompilerVariableList();
		CompilerVariableList	 	literalVariableList = new CompilerVariableList();
		CompilerVariableList		variableList;
		boolean				isConstantPushing;
		PCGAndNode			andNode;
		Binding				headBinding;
		CompilerTypeList	headArguments;
		CompilerTypeList 	headFreeArguments;
		PCGAndNode			newAndNode;
		PCGOrNode			orNode;
		PCGOrNode			newOrNode;
		CompilerTypeList	literalArguments;
		Binding				literalBinding;
		CompilerTypeList	literalFreeArguments;
		String				headPredicateName;
		CompilerTypeList 	headBoundArguments;
		PCGOrNode			newMagicOrNode;
		String				magicPredicateName;

		for (int i = 0; i < newCliquePredicate.getNumberOfRecursiveRules(); i++) {
			variableList = new CompilerVariableList();

			// step 1: create new and node with free arguments
			isConstantPushing = false;
			andNode = newCliquePredicate.getRecursiveRule(i);
			headBinding = andNode.getBindingPattern();
			headArguments = andNode.getArguments();
			headFreeArguments = Utilities.getFreeArguments(headArguments, headBinding).copy(variableList);

			headBoundVariableList.clear();
			Utilities.getBoundVariables(headArguments, headBinding, headBoundVariableList);

			// we generate all bound variables from the head
			newAndNode = new PCGAndNode(newPredicateName, headFreeArguments);
			newAndNode.setBindingPattern(newBinding);

			// step 2: link the new and node to the rest of the or nodes
			for (int j = 0; j < andNode.getNumberOfChildren(); j++) {
				orNode = andNode.getChild(j);

				if (orNode.isRecursiveLiteral(clique)) {
					// rewrite recursive node
					literalArguments = orNode.getArguments();
					literalBinding = orNode.getBindingPattern();
					literalFreeArguments = Utilities.getFreeArguments(literalArguments, literalBinding).copy(variableList);

					newOrNode = new PCGOrNode(newPredicateName, literalFreeArguments);
					newOrNode.setBindingPattern(newBinding);
					newOrNode.setAsRecursive();
					newOrNode.setBaseClique(clique);
					clique.substituteParent(orNode,newOrNode);
				} else {
					// we determine that we need to do more explicit constant pushing in this rule
					//  if there are other non-recursive literal that uses those bound variables from the head
					if (!isConstantPushing) {
						literalVariableList.clear();
						Utilities.getVariables(orNode, literalVariableList);

						if (Utilities.intersects(headBoundVariableList, literalVariableList))
							isConstantPushing = true;
					}

					newOrNode = orNode.copyTree(variableList);
				}

				newAndNode.addChild(newOrNode);
				orNode.delete();
			}

			// step 3: create magic or node - we utilize the special meaning of magic
			//	 for constant pushing later on iff any of the variables in the
			//   bound arguments are used in any non-recursive literal
			if (isConstantPushing) {
				headPredicateName = andNode.getPredicateName();
				headBoundArguments = Utilities.getBoundArguments(headArguments, headBinding).copy(variableList);

				magicPredicateName = Rewriter.createMagicPredicateName(headPredicateName, headBinding);
				newMagicOrNode = new PCGOrNode(magicPredicateName, headBoundArguments);
				// this is non-recursive
				newMagicOrNode.clearBindingPattern();
				newAndNode.prependChild(newMagicOrNode);
			}

			// step 4: attach the new and node to new clique predicate
			newCliquePredicate.setRecursiveRule(i, newAndNode);
		}
		
		this.deALSContext.logTrace(logger, "Exiting modifyTrivialPhaseOneRecursiveRules");
	}

	protected void modifyTrivialPhaseOneExitRules(CliquePredicate newCliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering modifyTrivialPhaseOneExitRules");
		
		// We generate new recursive predicate such that we drop the bound arguments
		// Delete all exit rules and add modified exit rules instead
		// We can be sure that new dummy rule is needed because the binding pattern
		//  for the recursive head can not be all-free

		String 				newPredicateName = newCliquePredicate.getPredicateName();
		Binding				newBinding = newCliquePredicate.getBindingPattern();
		CompilerVariableList 		variableList;
		PCGAndNode			andNode;
		String				predicateName;
		Binding				binding;
		CompilerTypeList	arguments;
		CompilerTypeList 	boundArguments;
		CompilerTypeList 	freeArguments;
		PCGAndNode			newAndNode;
		String				magicPredicateName;
		PCGOrNode			newMagicOrNode;
		PCGOrNode			newOrNode;

		for (int i = 0; i < newCliquePredicate.getNumberOfExitRules(); i++) {
			variableList = new CompilerVariableList();
			andNode = newCliquePredicate.getExitRule(i);
			predicateName = andNode.getPredicateName();
			binding = andNode.getBindingPattern();
			arguments = andNode.getArguments();
			boundArguments = Utilities.getBoundArguments(arguments,binding).copy(variableList);
			freeArguments = Utilities.getFreeArguments(arguments,binding).copy(variableList);

			// step 1: create new and node with free arguments
			newAndNode = new PCGAndNode(newPredicateName, freeArguments);
			newAndNode.setBindingPattern(newBinding);

			// step 2: create magic or node - we utilize the special meaning of magic for constant pushing later on
			magicPredicateName = Rewriter.createMagicPredicateName(predicateName, binding);
			newMagicOrNode = new PCGOrNode(magicPredicateName,boundArguments);
			newMagicOrNode.clearBindingPattern();
			// this is non-recursive
			newAndNode.addChild(newMagicOrNode);

			// step 3: link the new and node to the rest of the or nodes
			for (PCGOrNode orNode : andNode.getChildren()) {
				newOrNode = orNode.copyTree(variableList);
				newAndNode.addChild(newOrNode);
				orNode.delete();
			}

			// step 4: attach the new and node to the clique
			newCliquePredicate.setExitRule(i, newAndNode);
		}
		
		this.deALSContext.logTrace(logger, "Exiting modifyTrivialPhaseOneExitRules");
	}

	protected void createDummyTrivialPhaseOneRule(Clique clique, CliquePredicate cliquePredicate, CliquePredicate newCliquePredicate)  {
		this.deALSContext.logTrace(logger, "Entering createDummyTrivialPhaseOneRule");
		// Assuming exit rules in new clique predicate has NOT been modified yet

		String 				predicateName = cliquePredicate.getPredicateName();
		Binding				binding = cliquePredicate.getBindingPattern();
		CompilerTypeList 	andNodeArgs = new CompilerTypeList();
		CompilerTypeList 	orNodeArgs = new CompilerTypeList();
		CompilerVariable			variable;
		PCGAndNode			dummyAndNode;
		PCGOrNode			dummyOrNode;

		for (int i = 0; i < cliquePredicate.getArity(); i++) {
			if (binding.getBinding(i) == BindingType.FREE) {
				variable = this.generateUniqueVariable();
				orNodeArgs.add(variable);
			} else {
				variable = CompilerVariable.generateAnonymousVariable();
			}
			andNodeArgs.add(variable);
		}

		PCGAndNode typedRule = null;
		// assign types to andNodeArgs from cliquePredicate
		if (newCliquePredicate.getExitRules().size() > 0)
			typedRule = newCliquePredicate.getExitRule(0);
		else if (newCliquePredicate.getRecursiveRules().size() > 0)
			typedRule = newCliquePredicate.getRecursiveRule(0);
		
		if (typedRule != null) {
			for (int i = 0; i < andNodeArgs.size(); i++) {
				if (typedRule.getArgument(i).isVariable()) {
					((CompilerVariable)andNodeArgs.get(i)).setDataType(((CompilerVariable)typedRule.getArgument(i)).getDataType());
				} else if (typedRule.getArgument(i).isList()) {
					((CompilerVariable)andNodeArgs.get(i)).setDataType(DataType.LIST);
				}
			}
		}
		
		dummyAndNode = new PCGAndNode(predicateName, andNodeArgs);
		dummyAndNode.setBindingPattern(binding);

		dummyOrNode = new PCGOrNode(newCliquePredicate.getPredicateName(), orNodeArgs);
		dummyOrNode.clearBindingPattern();
		dummyOrNode.setAsRecursive();
		dummyOrNode.setBaseClique(clique);
		clique.addParent(dummyOrNode);

		// this will ensure that this non-recursive or node will have information about
		//  whether the clique it is pointing to is sharable or not
		dummyOrNode.setRewritingMethod(RewritingMethodType.TRIVIAL_PHASE1);
		dummyAndNode.addChild(dummyOrNode);

		cliquePredicate.addExitRule(dummyAndNode);
		
		this.deALSContext.logTrace(logger, "Exiting createDummyTrivialPhaseOneRule");
	}

	protected void rewriteTrivialPhaseOne(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering rewriteTrivialPhaseOne for {}", orNode.toString());
				
		Clique clique = (Clique)orNode.getChild(0);
		
		// If trivial phase one rewriting strategy is chosen,
		// this also implicitly implies no mutual recursion
		CliquePredicate cliquePredicate = clique.getCliquePredicate(orNode.getPredicateName(), 
				orNode.getArity(), orNode.getBindingPattern());

		// the first condition is somewhat artificial, it is added because sometimes
		//  the magic clique predicate may not be generated
		if (cliquePredicate == null)
			throw new CompilerException("can not find clique predicate in left linear rewriting");

		String predicateName = cliquePredicate.getPredicateName();
		Binding binding = cliquePredicate.getBindingPattern();
		int numberOfFreeArgs = binding.getNumberOfFreeArguments();

		if (binding.allFree()) {
			this.deALSContext.logError(logger, "cannot apply rewriting technique.  all-free clique predicate found in trivial phase one rewriting");
			throw new CompilerException("cannot apply rewriting technique.  all-free clique predicate found in trivial phase one rewriting");
		}
		
		if (!cliquePredicate.isRewritten()) {
			cliquePredicate.setRewritten();

			// create new predicate name for recursive predicate with dropped bound arguments
			String newPredicateName = Rewriter.createDummyPredicateName(predicateName, binding);

			// create new clique predicate with all free binding
			Binding allFreeBinding = new Binding(numberOfFreeArgs, BindingType.FREE);
			CliquePredicate newCliquePredicate = this.createCliquePredicate(clique, newPredicateName, numberOfFreeArgs, allFreeBinding);

			if (binding.allBound()) {
				PCGAndNode 	recursiveRule;
				PCGOrNode  	recursiveLiteral;
				int 		recursiveLiteralPosition;						

				// drop all recursive rules
				for (int i = cliquePredicate.getNumberOfRecursiveRules() - 1; i >= 0; i--) {
					recursiveRule = cliquePredicate.removeRecursiveRule(i);
					Pair<Integer, Integer> retvalPair = Rewriter.getRecursiveLiteralPosition(recursiveRule, clique);
					recursiveLiteralPosition = retvalPair.getFirst();
					recursiveLiteral = recursiveRule.getChild(recursiveLiteralPosition);
					clique.removeParent(recursiveLiteral);
					recursiveRule.delete();
				}
			} else {
				// copy recursive rules
				for (int i = cliquePredicate.getNumberOfRecursiveRules() - 1; i >= 0; i--)
					newCliquePredicate.addRecursiveRule(cliquePredicate.removeRecursiveRule(i));
			}

			// copy exit rules
			for (int i = cliquePredicate.getNumberOfExitRules() - 1; i >= 0; i--)
				newCliquePredicate.addExitRule(cliquePredicate.removeExitRule(i));

			// Since we are doing trivial phase one, we can assume that there is only one new magic clique predicate
			this.createMagicCliquePredicate(clique, predicateName, binding);

			this.createDummyTrivialPhaseOneRule(clique, cliquePredicate, newCliquePredicate);

			if (!binding.allBound())
				this.modifyTrivialPhaseOneRecursiveRules(clique, newCliquePredicate);

			this.modifyTrivialPhaseOneExitRules(newCliquePredicate);
		}

		this.deALSContext.logTrace(logger, "Exiting rewriteTrivialPhaseOne for {}", orNode.toString());
	}

	protected void modifyTrivialPhaseTwoExitRules(Clique clique, CliquePredicate newCliquePredicate) {
		this.deALSContext.logTrace(logger, "Entering modifyTrivialPhaseTwoExitRules");
		
		// We generate new recursive predicate such that we drop the bound arguments
		// Delete all exit rules and add modified exit rules instead
		// We can be sure that new dummy rule is needed because the binding pattern
		//  for the recursive head can not be all-free

		String				newPredicateName 	= newCliquePredicate.getPredicateName();
		Binding				newBinding 			= newCliquePredicate.getBindingPattern();
		CompilerVariableList 		variableList;
		Binding				binding;
		CompilerTypeList	arguments, boundArguments, freeArguments;
		PCGAndNode			andNode, newAndNode;
		String				predicateName, magicPredicateName;
		PCGOrNode			newOrNode, newMagicOrNode;

		for (int i = 0; i < newCliquePredicate.getNumberOfExitRules(); i++) {
			variableList = new CompilerVariableList();
			andNode = newCliquePredicate.getExitRule(i);
			predicateName = andNode.getPredicateName();
			binding = andNode.getBindingPattern();
			arguments = andNode.getArguments();
			boundArguments = Utilities.getBoundArguments(arguments,binding).copy(variableList);
			freeArguments = Utilities.getFreeArguments(arguments,binding).copy(variableList);

			// step 1: create new and node with free arguments
			newAndNode = new PCGAndNode(newPredicateName, freeArguments);
			newAndNode.setBindingPattern(newBinding);

			// step 2: create magic or node - we utilize the special meaning of magic for constant pushing later on
			magicPredicateName = Rewriter.createMagicPredicateName(predicateName, binding);
			newMagicOrNode = new PCGOrNode(magicPredicateName, boundArguments);
			newMagicOrNode.clearBindingPattern();
			newMagicOrNode.setAsRecursive();
			newMagicOrNode.setBaseClique(clique);
			clique.addParent(newMagicOrNode);
			newAndNode.addChild(newMagicOrNode);

			// we have to specify the rewriting method for this magic or-node because it becomes
			//  the top node of the clique since the original recursive rules will be dropped
			//  in the trivial phase two rewriting
			newMagicOrNode.setRewritingMethod(RewritingMethodType.TRIVIAL_PHASE2);

			// step 3: link the new andnode to the rest of the ornodes
			for (PCGOrNode orNode : andNode.getChildren()) {
				newOrNode = orNode.copyTree(variableList);
				newAndNode.addChild(newOrNode);				
				orNode.delete();
			}

			// step 4: attach the new and node to the clique
			newCliquePredicate.setExitRule(i, newAndNode);
		}
		
		this.deALSContext.logTrace(logger, "Exiting modifyTrivialPhaseTwoExitRules");
	}

	protected void createTrivialPhaseTwoMagicRules(Clique clique, CliquePredicate cliquePredicate, 
			CliquePredicate magicCliquePredicate) {

		this.deALSContext.logTrace(logger, "Entering createTrivialPhaseTwoMagicRules");
		
		CompilerVariableList	variableList;
		int 			recursiveLiteralPosition;
		PCGAndNode		magicHead;
		PCGOrNode		recursiveLiteral, magicLiteral;

		for (PCGAndNode recursiveHead : cliquePredicate.getRecursiveRules()) {
			variableList = new CompilerVariableList();
			Pair<Integer, Integer> retvalPair = Rewriter.getRecursiveLiteralPosition(recursiveHead, clique);
			recursiveLiteralPosition = retvalPair.getFirst();
			recursiveLiteral = recursiveHead.getChild(recursiveLiteralPosition);

			// step 1: create new magic_head with bound arguments of recursive_literals
			magicHead = this.createMagicAndNode(recursiveLiteral, variableList);

			// step 2: create new magicLiteral with bound arguments of recursiveHead
			magicLiteral = this.createMagicOrNode(recursiveHead, variableList);
			magicLiteral.setAsRecursive();
			magicLiteral.setBaseClique(clique);
			clique.addParent(magicLiteral);
			magicHead.addChild(magicLiteral);		// always the first

			// step 3: link all or nodes before recursive literal to the magic head
			for (int j = 0; j < recursiveLiteralPosition; j++) 
				magicHead.addChild(recursiveHead.getChild(j).copyTree(variableList));
			
			// step 4: attach the new and node to the clique
			magicCliquePredicate.addRecursiveRule(magicHead);

			clique.removeParent(recursiveLiteral);
		}

		this.deALSContext.logTrace(logger, "Exiting createTrivialPhaseTwoMagicRules");
	}

	protected void createDummyTrivialPhaseTwoRule(Clique clique, CliquePredicate cliquePredicate, 
			CliquePredicate newCliquePredicate) {
		
		this.deALSContext.logTrace(logger, "Entering createDummyTrivialPhaseTwoRule");
		
		// Assuming exit rules in new clique predicate has NOT been modified yet
		String 				predicateName 		= cliquePredicate.getPredicateName();
		String 				newPredicateName 	= newCliquePredicate.getPredicateName();
		Binding				binding				= cliquePredicate.getBindingPattern();
		CompilerTypeList 	andNodeArgs 		= new CompilerTypeList();
		CompilerTypeList 	orNodeArgs 			= new CompilerTypeList();
		CompilerVariable			variable;

		for (int i = 0; i < cliquePredicate.getArity(); i++) {
			if (binding.getBinding(i) == BindingType.FREE) {
				variable = this.generateUniqueVariable();
				orNodeArgs.add(variable);
			} else {
				variable = CompilerVariable.generateAnonymousVariable();
			}

			andNodeArgs.add(variable);
		}

		PCGAndNode dummyAndNode = new PCGAndNode(predicateName, andNodeArgs);
		dummyAndNode.setBindingPattern(binding);

		PCGOrNode dummyOrNode = new PCGOrNode(newPredicateName, orNodeArgs);
		dummyOrNode.clearBindingPattern();
		dummyOrNode.setAsRecursive();
		dummyOrNode.setBaseClique(clique);
		clique.addParent(dummyOrNode);

		// this will ensure that this non-recursive or node will have information about
		//  whether the clique it is pointing to is sharable or not
		dummyOrNode.setRewritingMethod(RewritingMethodType.TRIVIAL_PHASE2);
		dummyAndNode.addChild(dummyOrNode);

		cliquePredicate.addExitRule(dummyAndNode);
		
		this.deALSContext.logTrace(logger, "Exiting createDummyTrivialPhaseTwoRule");
	}

	protected void rewriteTrivialPhaseTwo(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering rewriteTrivialPhaseTwo for {}", orNode.toString());
				
		Clique clique = (Clique) orNode.getChild(0);
		//PCGAndNode	recursiveRule;

		CliquePredicate cliquePredicate = clique.getCliquePredicate(orNode.getPredicateName(), 
				orNode.getArity(), orNode.getBindingPattern());

		if (cliquePredicate == null) {
			this.deALSContext.logError(logger, "Can not find clique predicate. Can not continue trivial phase two rewriting");
			throw new CompilerException("Can not find clique predicate. Can not continue trivial phase two rewriting");	
		}
			
		String predicateName = cliquePredicate.getPredicateName();
		Binding binding = cliquePredicate.getBindingPattern();
		int numberOfFreeArgs = binding.getNumberOfFreeArguments();

		if (binding.allFree()) {
			this.deALSContext.logError(logger, "Can not apply rewriting technique.  All-free clique predicate found in trivial phase two rewriting");			
			throw new CompilerException("Can not apply rewriting technique.  All-free clique predicate found in trivial phase two rewriting");
		}
		
		if (!cliquePredicate.isRewritten()) {
			cliquePredicate.setRewritten();

			// create new predicate name for recursive predicate with dropped bound arguments
			String newPredicateName = Rewriter.createDummyPredicateName(predicateName, binding);

			// create new clique predicate with all free binding
			Binding allFreeBinding = new Binding(numberOfFreeArgs, BindingType.FREE);
			CliquePredicate newCliquePredicate 
				= this.createCliquePredicate(clique,newPredicateName, numberOfFreeArgs, allFreeBinding);

			// Since we are doing trivial phase two, we can assume that there is only one new magic
			//  clique predicate
			CliquePredicate magicCliquePredicate = this.createMagicCliquePredicate(clique, predicateName, binding);

			this.createTrivialPhaseTwoMagicRules(clique, cliquePredicate, magicCliquePredicate);

			// drop all recursive rules
			for (int i = cliquePredicate.getNumberOfRecursiveRules() - 1; i >= 0; i--)
				cliquePredicate.removeRecursiveRule(i).delete();
			
			// copy exit rules
			for (int i = cliquePredicate.getNumberOfExitRules() - 1; i >= 0; i--)
				newCliquePredicate.addExitRule(cliquePredicate.removeExitRule(i));

			this.createDummyTrivialPhaseTwoRule(clique, cliquePredicate, newCliquePredicate);

			this.modifyTrivialPhaseTwoExitRules(clique,newCliquePredicate);
		}

		this.deALSContext.logTrace(logger, "Exiting rewriteTrivialPhaseTwo for {}", orNode.toString());
	}

	protected CompilerVariable generateUniqueVariable() {
		return new CompilerVariable(UNIQUE_VARIABLE_PREFIX + this.variableCount++);
	}

	protected static String createDummyPredicateName(String predicateName, Binding binding) {
		// binding should be all free arguments???		
		return DUMMY_PREDICATE_NAME_PREFIX + predicateName + "_" + binding.createBindingString();
	}

	protected static String createMagicPredicateName(String predicateName, Binding binding) {
		// binding should be all bound arguments???
		return MAGIC_PREDICATE_NAME_PREFIX + predicateName + "_" + binding.createBindingString();
	}

	protected static String createSupplementaryMagicPredicateName(String supplementaryPredicatePrefix, String predicateName,
			Binding binding, int ruleNumber, int numberOfRecursiveLiterals) {

		// create magic predicate name for recursive predicate with bound arguments
		String supplementaryMagicPredicateName;
		String bindingString = binding.createBindingString();

		if ((ruleNumber == 0) || (numberOfRecursiveLiterals == 0))
			supplementaryMagicPredicateName = supplementaryPredicatePrefix + predicateName + "_" + bindingString;
		else
			supplementaryMagicPredicateName = supplementaryPredicatePrefix + ruleNumber + "_" + numberOfRecursiveLiterals + "_" + predicateName + "_" + bindingString;

		return supplementaryMagicPredicateName;
	}

	protected static String createLinearSupplementaryMagicPredicateName(String predicateName,
			Binding binding, int ruleNumber, int numberOfRecursiveLiterals) {
		// This is used for supplementary nodes that needs materialization
		// which is used primarily in linear recursive clique that uses magic rewrite
		return Rewriter.createSupplementaryMagicPredicateName(LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX, predicateName, binding, ruleNumber, numberOfRecursiveLiterals);
	}

	protected static String createNonLinearSupplementaryMagicPredicateName(String predicateName,
			Binding binding, int ruleNumber, int numberOfRecursiveLiterals) {
		// This is used for supplementary nodes that needs materialization
		// which is used primarily in linear recursive clique that uses magic rewrite
		return Rewriter.createSupplementaryMagicPredicateName(NON_LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX, predicateName, binding, ruleNumber, numberOfRecursiveLiterals);
	}

	protected static String createGeneralSupplementaryMagicPredicateName(String predicateName, Binding binding, int ruleNumber, int numberOfRecursiveLiterals) {
		return Rewriter.createSupplementaryMagicPredicateName(GENERAL_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX, predicateName, binding, ruleNumber, numberOfRecursiveLiterals);
	}

	protected CliquePredicate createCliquePredicate(Clique clique, String predicateName, int arity, Binding binding) {
		this.deALSContext.logTrace(logger, "Entering createCliquePredicate");
		
		CliquePredicate cliquePredicate = clique.getCliquePredicate(predicateName, arity, binding);

		if (cliquePredicate == null) {
			cliquePredicate = new CliquePredicate(predicateName, arity);
			cliquePredicate.setBindingPattern(binding);
			cliquePredicate.setRewritten(); 

			clique.addCliquePredicate(cliquePredicate);
		}
		
		this.deALSContext.logTrace(logger, "Exiting createCliquePredicate");

		return cliquePredicate;
	}

	protected CliquePredicate createMagicCliquePredicate(Clique clique, String predicateName, Binding binding) {
		this.deALSContext.logTrace(logger, "Entering createMagicCliquePredicate");
		
		String magicPredicateName = Rewriter.createMagicPredicateName(predicateName, binding);
		int magicArity = binding.getNumberOfBoundArguments();
		Binding magicBinding = new Binding(magicArity, BindingType.FREE);
		CliquePredicate	magicCliquePredicate = clique.getCliquePredicate(magicPredicateName, magicArity, magicBinding);

		if (magicCliquePredicate == null) {
			magicCliquePredicate = new CliquePredicate(magicPredicateName, magicArity);
			magicCliquePredicate.clearBindingPattern();
			magicCliquePredicate.setRewritten(); 

			clique.addCliquePredicate(magicCliquePredicate);
		}

		this.deALSContext.logTrace(logger, "Exiting createMagicCliquePredicate");
		
		return magicCliquePredicate;
	}

	protected CliquePredicate createMagicCliquePredicate(Clique clique, PCGOrNode recursiveLiteral) {		
		String predicateName = recursiveLiteral.getPredicateName();
		Binding binding = recursiveLiteral.getBindingPattern();		
		return this.createMagicCliquePredicate(clique, predicateName, binding);
	}

	protected PCGAndNode createMagicAndNode(PCGOrNode recursiveLiteral, CompilerVariableList variableList) {
		this.deALSContext.logTrace(logger, "Entering createMagicAndNode");
		
		String predicateName = recursiveLiteral.getPredicateName();
		Binding binding = recursiveLiteral.getBindingPattern();
		CompilerTypeList arguments = recursiveLiteral.getArguments();
		CompilerTypeList boundArguments = Utilities.getBoundArguments(arguments, binding);
		String magicPredicateName = Rewriter.createMagicPredicateName(predicateName, binding);
		CompilerTypeList magicArgs = boundArguments.copy(variableList);
		
		PCGAndNode magicAndNode = new PCGAndNode(magicPredicateName, magicArgs);
		magicAndNode.clearBindingPattern();
		
		this.deALSContext.logTrace(logger, "Exiting createMagicAndNode");
		
		return magicAndNode;
	}

	protected PCGOrNode createMagicOrNode(PCGAndNode recursiveHead, CompilerVariableList variableList) {
		this.deALSContext.logTrace(logger, "Entering createMagicOrNode");
		
		Binding binding = recursiveHead.getBindingPattern();
		CompilerTypeList boundArguments = Utilities.getBoundArguments(recursiveHead.getArguments(), binding);
		String magicPredicateName = Rewriter.createMagicPredicateName(recursiveHead.getPredicateName(), binding);
		CompilerTypeList magicArgs = boundArguments.copy(variableList);
		
		PCGOrNode magicOrNode = new PCGOrNode(magicPredicateName, magicArgs);
		magicOrNode.clearBindingPattern();
		
		this.deALSContext.logTrace(logger, "Exiting createMagicOrNode");

		return magicOrNode;
	}

	protected PCGOrNode createMagicOrNode(PCGAndNode recursiveHead) {
		this.deALSContext.logTrace(logger, "Entering createMagicOrNode");
		
		Binding binding = recursiveHead.getBindingPattern();
		String magicPredicateName = Rewriter.createMagicPredicateName(recursiveHead.getPredicateName(), binding);
		CompilerTypeList magicArgs = Utilities.getBoundArguments(recursiveHead.getArguments(), binding);
		
		PCGOrNode magicOrNode = new PCGOrNode(magicPredicateName, magicArgs);
		magicOrNode.clearBindingPattern();
		
		this.deALSContext.logTrace(logger, "Exiting createMagicOrNode");

		return magicOrNode;
	}

	protected void extractComplexTermsFromPcgAndNode(PCGAndNode andNode, List<PCGOrNode> equalityList) {
		this.deALSContext.logTrace(logger, "Entering extractComplexTermsFromPcgAndNode");
		
		CompilerVariable variable;
		PCGOrNode equality;
		Binding equalityBinding;  

		int count = 0;
		for (CompilerTypeBase argument : andNode.getArguments()) {
			if (andNode.getBinding(count) == BindingType.BOUND &&
					(argument.isFunctor() || argument.isList())) {
				variable = this.generateUniqueVariable();
				variable.setDataType(argument.getDataType());
				andNode.overwriteNthArgument(count, variable);

				equalityBinding = new Binding(2);
				equalityBinding.setBinding(0, BindingType.BOUND);
				equalityBinding.setBinding(1, BindingType.FREE);

				CompilerTypeList arguments = new CompilerTypeList();
				arguments.add(variable);
				arguments.add(argument);

				equality = new PCGOrNode(BuiltInPredicate.EQUALITY_PREDICATE_NAME, arguments, BuiltInPredicateType.BINARY);
				equality.setBindingPattern(equalityBinding);
				
				equalityList.add(equality);
			}
			count++;
		}
		
		this.deALSContext.logTrace(logger, "Exiting extractComplexTermsFromPcgAndNode");
	}

	/*************************************************************
	 * create supplementary magic arguments
	 * - the arguments of the supplementary magic predicate is as follows:
	 *
	 *   Let i be the index of the recursive literal
	 *   B = Bound(head) = bound variables in the head
	 *   F = Free(head)  = free variables in the head
	 *   R = Variables of literal[1] .. literal[i]
	 *   L = Variables of literal[i] .. literal[N]
	 *   S = variables or arguments in supplementary magic
	 *     = B + ( R intersect F + L )
	 **************************************************************/
	protected CompilerTypeList createSupplementaryMagicArguments(PCGAndNode recursiveHead, int recursiveLiteralPosition) {
		this.deALSContext.logTrace(logger, "Entering createSupplementaryMagicArguments for {} where recursiveLiteralPosition = {}", 
					recursiveHead.toString(), recursiveLiteralPosition);

		PCGOrNode 			literal;
		CompilerVariable 			variable;
		Binding 			binding = recursiveHead.getBindingPattern();
		CompilerTypeList	arguments = recursiveHead.getArguments();
		CompilerVariableList		localVariables = new CompilerVariableList();
		CompilerVariableList		usedVariables = new CompilerVariableList();

		for (int i = 0; i < recursiveLiteralPosition; i++) {
			literal = recursiveHead.getChild(i);
			Utilities.getVariables(literal, localVariables);

			if (this.deALSContext.isDebugEnabled()) {
				this.deALSContext.logDebug(logger, i + ": " + literal.toString());
				this.deALSContext.logDebug(logger, "localVariables = ");
				this.deALSContext.logDebug(logger, localVariables.toString());
			}
		}

		if (this.deALSContext.isDebugEnabled()) {
			this.deALSContext.logDebug(logger, "final localVariables = "); 
			this.deALSContext.logDebug(logger, localVariables.toString());
		}
		
		for (int i = recursiveLiteralPosition; i < recursiveHead.getNumberOfChildren(); i++)   {
			literal = recursiveHead.getChild(i);
			Utilities.getVariables(literal, usedVariables);

			if (this.deALSContext.isDebugEnabled()) {
				this.deALSContext.logDebug(logger, i + ": " + literal.toString());
				this.deALSContext.logDebug(logger, "usedVariables = ");
				this.deALSContext.logDebug(logger, usedVariables.toString());
			}
		}

		Utilities.getFreeVariables(arguments, binding, usedVariables);

		if (this.deALSContext.isDebugEnabled()) {
			this.deALSContext.logDebug(logger, "final usedVariables = ");
			this.deALSContext.logDebug(logger, usedVariables.toString());
		}
		
		CompilerVariableList supplementaryVariables = Utilities.getIntersectingVariables(localVariables, usedVariables);

		// eliminate anonymous variables
		for (int i = supplementaryVariables.size() - 1; i >= 0; i--) {
			variable = supplementaryVariables.get(i);

			if (variable.isAnonymous())
				supplementaryVariables.remove(i);	
		}

		// this keeps the bound variables from the head at the front of the argument list
		CompilerVariableList boundVariablesInHead = new CompilerVariableList();
		// but we keep anonymous variables from the bound arguments of the head
		Utilities.getBoundVariables(arguments, binding, boundVariablesInHead);
		Utilities.getBoundVariables(arguments, binding, supplementaryVariables);
		// merge into a single list with the boundVariablesInHead at front
		supplementaryVariables.prependList(boundVariablesInHead);

		if (this.deALSContext.isDebugEnabled()) {
			this.deALSContext.logDebug(logger, "final supplementaryVariables = ");
			this.deALSContext.logDebug(logger, supplementaryVariables.toString());
		}
		
		this.deALSContext.logTrace(logger, "Exiting createSupplementaryMagicArguments for {} where recursiveLiteralPosition = {}", recursiveHead.toString(), recursiveLiteralPosition);
		
		return supplementaryVariables.toCompilerTypeList();
	}

	protected Pair<PCGAndNode, CliquePredicate> createCommonSupplementaryMagicCliquePredicateAndNode(Clique clique,
			PCGAndNode recursiveHead, int recursiveLiteralPosition, CompilerVariableList variableList,
			String supplementaryMagicPredicateName) {

		this.deALSContext.logTrace(logger, "Entering createCommonSuppMagicCliquePredicateAndNode");
		
		CompilerTypeList supplementaryMagicVariables 
			= this.createSupplementaryMagicArguments(recursiveHead, recursiveLiteralPosition);
		CompilerTypeList supplementaryMagicArguments = supplementaryMagicVariables.copy(variableList);
		int supplementaryMagicArity = supplementaryMagicArguments.size();
		Binding supplementaryMagicBinding = new Binding(supplementaryMagicArity, BindingType.FREE);
		PCGAndNode supplementaryMagicAndNode = new PCGAndNode(supplementaryMagicPredicateName, supplementaryMagicArguments);
		supplementaryMagicAndNode.clearBindingPattern();
		
		CliquePredicate supplementaryMagicCliquePredicate = clique.getCliquePredicate(supplementaryMagicPredicateName,
				supplementaryMagicArity,
				supplementaryMagicBinding);
		
		if (supplementaryMagicCliquePredicate == null) {
			supplementaryMagicCliquePredicate = new CliquePredicate(supplementaryMagicPredicateName, supplementaryMagicArity);
			supplementaryMagicCliquePredicate.clearBindingPattern();
			supplementaryMagicCliquePredicate.setRewritten(); 

			clique.addCliquePredicate(supplementaryMagicCliquePredicate);
		}
		
		this.deALSContext.logTrace(logger, "Exiting createCommonSuppMagicCliquePredicateAndNode");

		return new Pair<>(supplementaryMagicAndNode, supplementaryMagicCliquePredicate);
	}

	protected PCGOrNode createCommonSupplementaryMagicOrNode(PCGAndNode recursiveHead, int recursiveLiteralPosition,
			String supplementaryMagicPredicateName) {

		this.deALSContext.logTrace(logger, "Entering createCommonSupplementaryMagicOrNode for {}", recursiveHead.toString());
		
		CompilerTypeList supplementaryMagicArguments 
			= this.createSupplementaryMagicArguments(recursiveHead, recursiveLiteralPosition);
		PCGOrNode supplementaryMagicOrNode = new PCGOrNode(supplementaryMagicPredicateName, supplementaryMagicArguments);
		supplementaryMagicOrNode.clearBindingPattern();

		this.deALSContext.logTrace(logger, "Exiting createCommonSupplementaryMagicOrNode for {}", recursiveHead.toString());
		
		return supplementaryMagicOrNode;
	}

	protected Pair<PCGAndNode, CliquePredicate> createGeneralSupplementaryMagicCliquePredicateAndNode(Clique clique, 
			PCGOrNode recursiveLiteral, PCGAndNode recursiveHead, int ruleNumber, int numberOfRecursiveLiterals, 
			int recursiveLiteralPosition, CompilerVariableList variableList) {

		String supplementaryMagicPredicateName 
			= Rewriter.createGeneralSupplementaryMagicPredicateName(recursiveLiteral.getPredicateName(), 
					recursiveLiteral.getBindingPattern(), ruleNumber, numberOfRecursiveLiterals);
		CompilerTypeList supplementaryMagicVariables 
			= this.createSupplementaryMagicArguments(recursiveHead, recursiveLiteralPosition);
		CompilerTypeList supplementaryMagicArguments = supplementaryMagicVariables.copy(variableList);
		int supplementaryMagicArity = supplementaryMagicArguments.size();
		Binding supplementaryMagicBinding = new Binding(supplementaryMagicArity, BindingType.FREE);
		PCGAndNode supplementaryMagicAndNode = new PCGAndNode(supplementaryMagicPredicateName, supplementaryMagicArguments);
		supplementaryMagicAndNode.clearBindingPattern();

		CliquePredicate supplementaryMagicCliquePredicate = clique.getCliquePredicate(supplementaryMagicPredicateName,
																supplementaryMagicArity, supplementaryMagicBinding);

		if (supplementaryMagicCliquePredicate == null) {
			supplementaryMagicCliquePredicate = new CliquePredicate(supplementaryMagicPredicateName, supplementaryMagicArity);
			supplementaryMagicCliquePredicate.clearBindingPattern();
			supplementaryMagicCliquePredicate.setRewritten(); 

			clique.addCliquePredicate(supplementaryMagicCliquePredicate);
		}

		return new Pair<>(supplementaryMagicAndNode, supplementaryMagicCliquePredicate);
	}

	protected PCGOrNode createGeneralSupplementaryMagicOrNode(PCGOrNode recursiveLiteral, PCGAndNode recursiveHead,
			int ruleNumber, int numberOfRecursiveLiterals, int recursiveLiteralPosition, CompilerVariableList variableList) {

		String supplementaryMagicPredicateName 
			= Rewriter.createGeneralSupplementaryMagicPredicateName(recursiveLiteral.getPredicateName(), 
					recursiveLiteral.getBindingPattern(), ruleNumber, numberOfRecursiveLiterals);
		CompilerTypeList supplementaryMagicVariables 
			= this.createSupplementaryMagicArguments(recursiveHead, recursiveLiteralPosition);
		CompilerTypeList supplementaryMagicArguments = supplementaryMagicVariables.copy(variableList);

		PCGOrNode supplementaryMagicOrNode = new PCGOrNode(supplementaryMagicPredicateName, supplementaryMagicArguments);
		supplementaryMagicOrNode.clearBindingPattern();

		return supplementaryMagicOrNode;
	}

	protected PCGOrNode createGeneralSupplementaryMagicOrNode(PCGOrNode recursiveLiteral, PCGAndNode recursiveHead,
			int ruleNumber, int numberOfRecursiveLiterals, int recursiveLiteralPosition) {

		String supplementaryMagicPredicateName 
			= Rewriter.createGeneralSupplementaryMagicPredicateName(recursiveLiteral.getPredicateName(), 
					recursiveLiteral.getBindingPattern(), ruleNumber, numberOfRecursiveLiterals);
		CompilerTypeList supplementaryMagicVariables 
			= this.createSupplementaryMagicArguments(recursiveHead, recursiveLiteralPosition);
		CompilerTypeList supplementaryMagicArguments = supplementaryMagicVariables;

		PCGOrNode supplementaryMagicOrNode = new PCGOrNode(supplementaryMagicPredicateName, supplementaryMagicArguments);
		supplementaryMagicOrNode.clearBindingPattern();

		return supplementaryMagicOrNode;
	}

	protected static Pair<Integer, Integer> getRecursiveLiteralPosition(PCGAndNode andNode, Clique clique) {  
		int	recursiveLiteralPosition = 0;
		int numberOfRecursiveLiterals = 0;
		int count = 0;
		
		for (PCGOrNode orNode : andNode.getChildren()) {
			// check if it is a local recursive predicate
			if (orNode.isRecursiveLiteral(clique)) {
				numberOfRecursiveLiterals++;
				recursiveLiteralPosition = count;
			}
			count++;
		}

		return new Pair<>(recursiveLiteralPosition, numberOfRecursiveLiterals);
	}

	protected static int getFirstRecursiveLiteralPosition(PCGAndNode andNode, Clique clique) {  
		int	recursiveLiteralPosition = 0;

		for (PCGOrNode orNode : andNode.getChildren()) {
			// check if it is a local recursive predicate
			if (orNode.isRecursiveLiteral(clique))
				return recursiveLiteralPosition;

			recursiveLiteralPosition++;
		}

		return recursiveLiteralPosition;
	}
}
