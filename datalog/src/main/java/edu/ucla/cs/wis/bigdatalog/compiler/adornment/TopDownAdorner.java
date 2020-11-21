package edu.ucla.cs.wis.bigdatalog.compiler.adornment;

import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.RewritingMethodType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYClique;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYCliquePredicate;
import edu.ucla.cs.wis.bigdatalog.exception.AdornerException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

	/* This class applied top down adornment to determine which rewriting techniques to apply to the PCG.
	 * APS 6/11/2013*/
public class TopDownAdorner {
	private static Logger logger = LoggerFactory.getLogger(TopDownAdorner.class.getName());

	private DeALSContext		deALSContext;
	private List<CliqueBase> 	localCliqueList;

	public TopDownAdorner(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	
	public PCGOrNode adornQueryForm(PCGOrNode orNode, List<CliqueBase> cliqueList) {
		this.deALSContext.logTrace(logger, "Entering adornQueryForm for {}", orNode.toString());
		
		this.localCliqueList = cliqueList;
		TopDownAdorner.setQueryFormBindingPattern(orNode);

		CompilerVariableList boundVariableList = new CompilerVariableList();
		CompilerVariableList conflictVariableList = new CompilerVariableList();

		this.adornOrNode(orNode, boundVariableList, conflictVariableList);

		this.deALSContext.logTrace(logger, "{}", orNode.toString());
		this.deALSContext.logTrace(logger, "Exiting adornQueryForm");

		return orNode;
	}

	private static void setQueryFormBindingPattern(PCGOrNode orNode) {		
		orNode.clearBindingPattern();
		
		int position = 0;
		for (CompilerTypeBase argument : orNode.getArguments()) {
			if (argument.isGround())
				orNode.setBindingPattern(position, BindingType.BOUND);
			else
				orNode.setBindingPattern(position, BindingType.FREE);
			position++;
		}
	}

	/*************************************************************
	 * setPcgOrNodeBindingPattern(PCGOrNode orNode, PCGAndNode andNode, 
	 *	                      VariableList boundVariableList,
	 *	                      VariableList conflictVariableList)
	 *
	 * This routine does 4 things:
	 *  1. set binding for the orNode
	 *  2. it updates the boundVariableList with all variables in orNode
	 *  3. it updates the conflictVariableList for negation and deletion
	 *  4. it checks for safety
	 **************************************************************/
	private static void setOrNodeBindingPattern(PCGOrNode orNode, PCGAndNode andNode,  
			CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList) {

		CompilerVariableList variableList = new CompilerVariableList();
		CompilerVariableList argumentVariableList = new CompilerVariableList();

		orNode.clearBindingPattern();

		int position = 0;
		for (CompilerTypeBase argument : orNode.getArguments()) {
			argumentVariableList.clear();
			Utilities.getVariables(argument, argumentVariableList);

			if (Utilities.allVariablesBound(argumentVariableList, boundVariableList))
				orNode.setBindingPattern(position, BindingType.BOUND);
			else
				orNode.setBindingPattern(position, BindingType.FREE);

			Utilities.mergeVariableLists(variableList, argumentVariableList);
			position++;
		}

		// Safety Condition: ensure that no variable in conflict set is used
		//for (Variable variable : variableList) {
		for (int i = 0; i < variableList.size(); i++) {
			if (conflictVariableList.contains(variableList.get(i)))
				new AdornerException("Unsafe variable " + variableList.get(i).toString() + " used in " + 
						orNode.toStringAsPredicate() + " of the following rule:\n" + andNode.toStringAsRule());
		}

		// Safety Condition: ensure that all free variables in negation or deletion are in the conflict set
		if (orNode.getPredicate().isNegative() /*|| orNode.getPredicate().isDelete()*/) {
			for (int i = 0; i < variableList.size(); i++) {
				if (!boundVariableList.contains(variableList.get(i)))
					conflictVariableList.add(variableList.get(i));
			}
		}
	}

	/*************************************************************
	 * setPcgAndNodeBindingPattern(PCGAndNode andNode, PCGOrNode orNode)
	 *
	 * This routine sets binding for the andNode such that
	 * no binding will pass through the set group arguments
	 **************************************************************/
	private static void setAndNodeBindingPattern(PCGAndNode andNode, PCGOrNode orNode) {
		Binding binding = andNode.getBindingPattern();

		int position = 0;
		for (CompilerTypeBase argument : andNode.getArguments()) {
			if (argument.containsAnyAggregate())
				binding.setBinding(position, BindingType.FREE);
			else
				binding.setBinding(position, orNode.getBinding(position));
			position++;
		}
	}

	private void adornOrNode(PCGOrNode orNode, CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList) {
		this.deALSContext.logTrace(logger, "Entering adornOrNode for {}", orNode.toString());
		
		if (orNode.isRecursive()) {
			this.adornClique(orNode);
		} else if (orNode.getPredicate().isBuiltIn()) {
			this.adornBuiltInOrNode(orNode, boundVariableList, conflictVariableList);
		} else if (orNode.getPredicate().isDerived()) { 
			for (PCGOrNodeChild andNode : orNode.getChildren()) {
				TopDownAdorner.setAndNodeBindingPattern((PCGAndNode)andNode, orNode);
				this.adornAndNode((PCGAndNode)andNode);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting adornOrNode for {}", orNode.toString());
	}

	private void adornAndNode(PCGAndNode andNode) {
		this.deALSContext.logTrace(logger, "Entering adornAndNode for {}", andNode.toString());
		
		CompilerVariableList boundVariableList = new CompilerVariableList();
		CompilerVariableList freeHeadVariableList = new CompilerVariableList();
		CompilerVariableList conflictVariableList = new CompilerVariableList();

		Utilities.getFreeVariables(andNode.getArguments(), andNode.getBindingPattern(), freeHeadVariableList);
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(), boundVariableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			TopDownAdorner.setOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList);
			this.adornOrNode(orNode, boundVariableList, conflictVariableList);
			Utilities.getVariables(orNode, boundVariableList);
		}

		// Safety Condition: all free variables in the head must be covered by bound variables 
		//                   in the head or by variables in the literals this also includes anonymous variables
		if (!Utilities.isSubset(freeHeadVariableList, boundVariableList))
			new AdornerException("Free variables in head are not covered in the following unsafe rule:\n" + andNode.toStringAsRule());

		// Safety Condition: ensure that no variable in conflict set is used
		//for (Variable variable : freeHeadVariableList) {
		for (int i = 0; i < freeHeadVariableList.size(); i++) {
			if (conflictVariableList.contains(freeHeadVariableList.get(i)))
				new AdornerException("Unsafe variable " + freeHeadVariableList.get(i).toString() + " used in head of rule:\n" + andNode.toStringAsRule());
		}

		this.deALSContext.logTrace(logger, "Exiting adornAndNode for {}", andNode.toString());
	}

	private static void setIfThenElseAndNodeBindingPattern(PCGOrNode orNode, PCGAndNode ifAndNode, 
			PCGAndNode thenAndNode, PCGAndNode elseAndNode) {
		// Assuming that all arguments of both andNode and orNode are variables
		CompilerTypeList orNodeArguments = orNode.getArguments();
		int index = 0;

		ifAndNode.clearBindingPattern();
		thenAndNode.clearBindingPattern();

		int position = 0;
		for (CompilerTypeBase term : ifAndNode.getArguments()) {
			// if we have the argument in the or node, get its binding pattern from the or node
			if ((index = orNodeArguments.getPosition(term)) > -1)
				ifAndNode.setBindingPattern(position, orNode.getBinding(index));
			else if (term.isBound())
				ifAndNode.setBindingPattern(position, BindingType.BOUND);
			else
				ifAndNode.setBindingPattern(position, BindingType.FREE);
			position++;
		}

		position = 0;
		for (CompilerTypeBase term : thenAndNode.getArguments()) {
			if ((index = orNodeArguments.getPosition(term)) > -1)
				thenAndNode.setBindingPattern(position, orNode.getBinding(index));
			else if (term.isBound())
				thenAndNode.setBindingPattern(position, BindingType.BOUND);
			else if (ifAndNode.getArguments().contains(term))
				thenAndNode.setBindingPattern(position, BindingType.BOUND);
			else
				thenAndNode.setBindingPattern(position, BindingType.FREE);
			
			position++;
		}

		if (elseAndNode != null) {
			elseAndNode.clearBindingPattern();

			position = 0;
			for (CompilerTypeBase term : elseAndNode.getArguments()) {
				if ((index = orNodeArguments.getPosition(term)) > -1)
					elseAndNode.setBindingPattern(position, orNode.getBinding(index));
				else if (term.isBound())
					elseAndNode.setBindingPattern(position, BindingType.BOUND);
				else
					elseAndNode.setBindingPattern(position, BindingType.FREE);

				position++;
			}
		}
	}

	private void adornBuiltInOrNode(PCGOrNode orNode, CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList) {
		this.deALSContext.logTrace(logger, "Entering adornBuiltInOrNode for {}", orNode.toString());
		
		switch (orNode.getBuiltInPredicateType())
		{
			case BINARY: {
				if (orNode.getPredicateName().equals(BuiltInPredicate.EQUALITY_PREDICATE_NAME)) {
					if (orNode.getBinding(0) == BindingType.FREE && orNode.getBinding(1) == BindingType.FREE)
						throw new AdornerException("Unsafe equality - at least one argument must be bound. OrNode = " + orNode.toStringAsPredicate());
				} else {
					if (orNode.getBinding(0) == BindingType.FREE || orNode.getBinding(1) == BindingType.FREE)
						throw new AdornerException("Unsafe evaluable predicate - both arguments must be bound. OrNode = " + orNode.toStringAsPredicate());
				}
			}
			break;
	
			case CHOICE: {
				//if (orNode.getBinding(0) == BindingType.FREE || orNode.getBinding(1) == BindingType.FREE)
				if (!orNode.getBindingPattern().allBound())
					throw new AdornerException("Unsafe choice predicate - all arguments must be bound. OrNode = " + orNode.toStringAsPredicate());
			}
			break;
	
			case IFTHEN:	  
			case IFTHENELSE: {
				this.adornIfThenElseNode(orNode, boundVariableList, conflictVariableList);
			}
			break;
			
			case AGGREGATE:
			case AGGREGATE_FS:
			case READ_USER_DEFINED_AGGREGATE_FS:	// APS 3/25/2013 @DATALOGFS
			case READ_USER_DEFINED_AGGREGATE:
			case SORT: 
			case TOPK: {	     		
				for (PCGOrNodeChild andNode : orNode.getChildren()) {
					((PCGAndNode)andNode).setBindingPattern(orNode.getBindingPattern());
					this.adornAndNode((PCGAndNode)andNode);
				}
			}
			break;
	
			case TRUE:
			case FALSE:
			case SINGLE:
			case MULTI:
			case GENERIC:
			case UNKNOWN:
			default:
				break;
		}

		this.deALSContext.logTrace(logger, "Exiting adornBuiltInOrNode for {}", orNode.toString());
	}

	private void adornIfThenElseNode(PCGOrNode orNode, CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList) {
		this.deALSContext.logTrace(logger, "Entering adornIfThenElseNode for {}", orNode.toString());
		
		CompilerVariableList boundVariableList1 = boundVariableList.copyList();
		CompilerVariableList boundVariableList2 = boundVariableList.copyList();
		CompilerVariableList conflictVariableList1 = conflictVariableList.copyList();
		CompilerVariableList conflictVariableList2 = conflictVariableList.copyList();

		if (orNode.isIfThenElseNode()) {
			PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0); 
			PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1); 
			PCGAndNode elseAndNode = (PCGAndNode) orNode.getChild(2);

			TopDownAdorner.setIfThenElseAndNodeBindingPattern(orNode, ifAndNode, thenAndNode, elseAndNode);

			PCGOrNode lastIfOrNode = ifAndNode.getLastChild();
			PCGOrNode lastThenOrNode = thenAndNode.getLastChild();
			PCGOrNode lastElseOrNode = elseAndNode.getLastChild();

			if (lastIfOrNode.isFalseNode() || lastThenOrNode.isFalseNode() || lastElseOrNode.isFalseNode()) {
				// no binding out of if-then-else
				this.adornDummyAndNode(ifAndNode, boundVariableList1, conflictVariableList1);

				// all free variables in IF are conflict to ELSE
				Utilities.getDifferenceVariables(boundVariableList1, boundVariableList, conflictVariableList2);

				this.adornDummyAndNode(thenAndNode, boundVariableList1, conflictVariableList1);
				this.adornDummyAndNode(elseAndNode, boundVariableList2, conflictVariableList2);

				// Safety Condition: ensure that all free variables in if-then-else are in the conflict set
				Utilities.getDifferenceVariables(boundVariableList1, boundVariableList, conflictVariableList);
				Utilities.getDifferenceVariables(boundVariableList2, boundVariableList, conflictVariableList);
			} else if (lastIfOrNode.isFalseNode() || lastThenOrNode.isFalseNode()) {
				// binding passed out of if-then-else is ELSE
				this.adornDummyAndNode(ifAndNode, boundVariableList1, conflictVariableList1);

				// Safety Condition: ensure that all free variables in if are in the conflict set
				Utilities.getDifferenceVariables(boundVariableList1, boundVariableList, conflictVariableList);

				// all free variables in IF are conflict to ELSE
				Utilities.getDifferenceVariables(boundVariableList1, boundVariableList, conflictVariableList2);

				this.adornDummyAndNode(thenAndNode, boundVariableList1, conflictVariableList1);
				this.adornDummyAndNode(elseAndNode, boundVariableList, conflictVariableList2);
			} else if (lastElseOrNode.isFalseNode()) {
				// binding passed out of if-then-else is IF union THEN
				this.adornDummyAndNode(ifAndNode, boundVariableList, conflictVariableList1);

				// all free variables in IF are conflict to ELSE. Note the reverse
				Utilities.getDifferenceVariables(boundVariableList, boundVariableList1, conflictVariableList2);

				this.adornDummyAndNode(thenAndNode, boundVariableList, conflictVariableList1);
				this.adornDummyAndNode(elseAndNode, boundVariableList2, conflictVariableList2);
			} else {
				// binding passed out of if-then-else is the intersection of
				//  ( IF union THEN ) intersect ELSE
				this.adornDummyAndNode(ifAndNode, boundVariableList1, conflictVariableList1);

				// all free variables in IF are conflict to ELSE
				Utilities.getDifferenceVariables(boundVariableList1, boundVariableList, conflictVariableList2);

				this.adornDummyAndNode(thenAndNode, boundVariableList1, conflictVariableList1);
				this.adornDummyAndNode(elseAndNode, boundVariableList2, conflictVariableList2);

				boundVariableList.clear();
				Utilities.getIntersectingVariables(boundVariableList1, boundVariableList2, boundVariableList);

				// Safety Condition: ensure that all free variables in if-then-else are in the conflict set
				Utilities.getDifferenceVariables(boundVariableList1, boundVariableList, conflictVariableList);
				Utilities.getDifferenceVariables(boundVariableList2, boundVariableList, conflictVariableList);
			}
		} else if (orNode.isIfThenNode())  {
			// no binding passed out of if-then
			PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0); 
			PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1); 

			TopDownAdorner.setIfThenElseAndNodeBindingPattern(orNode, ifAndNode, thenAndNode, null);      

			this.adornDummyAndNode(ifAndNode, boundVariableList1, conflictVariableList1);
			this.adornDummyAndNode(thenAndNode, boundVariableList1, conflictVariableList1);

			// Safety Condition: ensure that all free variables in if-then are in the conflict set
			Utilities.getDifferenceVariables(boundVariableList1, boundVariableList, conflictVariableList);
		}

		this.deALSContext.logTrace(logger, "Exiting adornIfThenElseNode for {}", orNode.toString());
	}

	private void adornDummyAndNode(PCGAndNode andNode, CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList) {
		this.deALSContext.logTrace(logger, "Entering adornDummyAndNode for {}", andNode.toString());
				
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(), boundVariableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			TopDownAdorner.setOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList);
			this.adornOrNode(orNode, boundVariableList, conflictVariableList);
			Utilities.getVariables(orNode, boundVariableList);
		}

		this.deALSContext.logTrace(logger, "Exiting adornDummyAndNode for {}", andNode.toString());
	}

	/*************************************************************
	 * adornLinearClique(PCGOrNode orNode, Clique clique)
	 *
	 * Upon completion of the adornment of a clique, all clique predicate with NO_BINDING should disappear.
	 * Some new clique predicates may be generated.  Cliques are shared when it is first extracted
	 *  (i.e. based on their predicate name and arity)
	 *  Here, we do something even more intelligent such that we share cliques for those clients (i.e. orNodes) that
	 *  have the same binding pattern. However, since we may generate new adorned rules, we could
	 *  potentially create rules that are not recursive any more and in fact, more than one clique may be split from this
	 *  clique, thus, upon completion of the whole adornment of the extracted pcg, the set of adorned cliques (represented here
	 *   as localCliqueList) have to be REORGANIZED. Look at Compiler.doCompile() for more detail.
	 **************************************************************/
	private void adornLinearClique(PCGOrNode orNode, Clique clique) {
		if (this.deALSContext.isTraceEnabled()) {
			orNode.resetIsDescribed();
			this.deALSContext.logTrace(logger, "Entering adornLinearClique for {}", orNode.toStringTree());
		}
		
		String 			predicateName 	= orNode.getPredicateName();
		int 			arity 			= orNode.getArity();
		Binding 		binding 		= orNode.getBindingPattern();
		CliquePredicate cliquePredicate;
		
		if (this.isCliqueAdorned(clique)) {
			// we have already adorned this clique but we may not have the right binding adorned
			 cliquePredicate = clique.getCliquePredicate(predicateName, arity, binding);

			// check if the clique predicate has been generated
			if (cliquePredicate == null) {
				// We simple get any clique predicate regardless of the binding
				cliquePredicate = clique.getCliquePredicate(predicateName, arity);

				if (cliquePredicate != null) {
					// @DATALOGFS
					// APS changed 3/18/2013 to account for getting an unbound cliquepredicate even when the clique was already adorned.
					// not necessary to create a new clique predicate - it messes things up
					if (cliquePredicate.getBindingPattern().hasNoBinding()) {
						this.adornLinearCliquePredicate(binding, cliquePredicate, clique);
					} else {
						CliquePredicate newCliquePredicate = cliquePredicate.copy();
						clique.addCliquePredicate(newCliquePredicate);

						this.adornLinearCliquePredicate(binding, newCliquePredicate, clique);
					}
				} else {
					this.deALSContext.logError(logger, "can not find clique predicate in adorner");
					throw new CompilerException("can not find clique predicate in adorner");
				}
			}
			// else we have generate the clique predicate with that binding, that we reach a closure
			//   for the adornment process
		} else {
			// we have not adorned this clique so far, so remember it
			this.localCliqueList.add(clique);

			// We simple get the clique predicate regardless of the binding
			// This is the original clique predicate
			// APS changed 3/18/2013 to account for multiple clique predicates with the same name and arity
			cliquePredicate = clique.getCliquePredicate(predicateName, arity);

			if (cliquePredicate == null) {
				this.deALSContext.logError(logger, "can not find clique predicate in adorner");
				throw new CompilerException("can not find clique predicate in adorner");
			}
				
			this.adornLinearCliquePredicate(binding, cliquePredicate, clique);
		}

		if (this.deALSContext.isTraceEnabled()) {
			orNode.resetIsDescribed();
			this.deALSContext.logTrace(logger, "Exiting adornLinearClique for {}", orNode.toStringTree());
		}
	}

	private void adornLinearCliquePredicate(Binding binding, CliquePredicate cliquePredicate, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering adornLinearCliquePredicate for binding = {} and clique predicate = {}", binding.toString(), cliquePredicate.toString());
				
		cliquePredicate.setBindingPattern(binding);

		for (PCGAndNode andNode : cliquePredicate.getExitRules()) {
			andNode.setBindingPattern(binding);
			this.adornAndNode(andNode);
		}

		for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
			andNode.setBindingPattern(binding);
			this.adornLinearCliqueAndNode(andNode, clique);
		}

		this.deALSContext.logTrace(logger, "{}", cliquePredicate.toString());
		this.deALSContext.logTrace(logger, "Exiting adornLinearCliquePredicate for binding = {}", binding.toString());
	}

	/*************************************************************
	 * adornLinearCliqueAndNode(PCGAndNode andNode, Clique clique)
	 *
	 * Given a recursive rule that fails the chain-rule, we will generate
	 *  adornment for a new clique predicate with all free binding.
	 * We can be sure that this will be the last clique predicate generated
	 *  for clique predicate with this predicate name and arity because
	 *  when we do adornment for a recursive rule with all free arguments
	 *  in the head, it will DEFINITELY fail the chain-rule.
	 *  Thus, when we generate the adornment for the clique predicate with 
	 *  all free binding again, it will terminate the adornment because
	 *  clique predicate with all free arguments has already been generated
	 **************************************************************/
	private void adornLinearCliqueAndNode(PCGAndNode andNode, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering adornLinearCliqueAndNode for {}", andNode.toString());
				
		CompilerVariableList boundVariableList = new CompilerVariableList();
		CompilerVariableList freeHeadVariableList = new CompilerVariableList();
		CompilerVariableList conflictVariableList = new CompilerVariableList();
		CompilerVariableList chainVariableList = new CompilerVariableList();

		String predicateName;
		int arity;
		Binding binding;
		CliquePredicate cliquePredicate;

		Utilities.getFreeVariables(andNode.getArguments(), andNode.getBindingPattern(), freeHeadVariableList);
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(),  boundVariableList);
		chainVariableList.appendList(boundVariableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			if (orNode.isRecursiveLiteral(clique)) {
				predicateName = orNode.getPredicateName();
				arity = orNode.getArity();

				if (!TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, chainVariableList)) {
					// if chain-rule fails, we will try to generate clique_predicate with all free arguments
					binding = new Binding(arity);
					binding.setAsAllFreeBinding();
					orNode.setBindingPattern(binding);

					this.deALSContext.logTrace(logger, "** FAIL chain-rule **");
				} else {
					binding = orNode.getBindingPattern().copy();
				}

				// Update the bound variable list
				Utilities.getVariables(orNode, boundVariableList);

				// we determine if we need to continue the adornment in this routine
				cliquePredicate = this.generateCliquePredicate(predicateName, arity, binding, clique);

				if (cliquePredicate != null)
					this.adornLinearCliquePredicate(binding, cliquePredicate, clique);
			} else{
				TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, chainVariableList);

				this.adornOrNode(orNode, boundVariableList, conflictVariableList);

				// Update the bound variable list
				Utilities.getVariables(orNode, boundVariableList);
			}
		}

		// Safety Condition: all free variables in the head must be covered by bound variables in the head or body 
		//                   this also includes anonymous variables
		if (!Utilities.isSubset(freeHeadVariableList, boundVariableList))
			new AdornerException("Free variables in head are not covered in the following unsafe rule:\n" + andNode.toStringAsRule());

		this.deALSContext.logTrace(logger, "Exiting adornLinearCliqueAndNode for {}", andNode.toString());
	}

	/*************************************************************
	 * adornNonLinearClique(PCGOrNode orNode, Clique clique)
	 *
	 * Upon completion of the adornment of a clique, all clique predicate with NO_BINDING should disappear.
	 * Some new clique predicates may be generated.  As we all know, cliques are shared when first extracted
	 *  (i.e. based on their predicate name and arity)
	 *  Here, we do something even more intelligent such that we share cliques for those clients (i.e. orNodes) that
	 *  have the same binding pattern.  However, since we may generate new adorned rules, we could
	 *  potentially create rules that are not recursive any more and in fact, more than one clique may be split from this
	 *  clique, thus, upon completion of the whole adornment of the extracted pcg, the set of adorned cliques (represented here
	 *  as localCliqueList) have to be REORGANIZED.
	 * Look at Compiler.doCompile() for more 
	 * 
	 * We use a two-phase adornment process for non-linear magic. The phase 1 is to determine if it satisfies the chain-rule
	 *  while making minimal adornment such that we only propagate adornment in the local recursive literal.
	 * If it is determined that non-linear magic satisfies the chain-rule, then we do the actual adornment in the second phase
	 **************************************************************/
	private void adornNonLinearClique(PCGOrNode orNode, Clique clique) {
		if (this.deALSContext.isTraceEnabled()) {
			orNode.resetIsDescribed();
			this.deALSContext.logTrace(logger, "Entering adornNonLinearClique for {}", orNode.toStringTree());
		}

		Pair<RewritingMethodType, Clique> retval = this.adornDecideNonLinearRewritingMethod(orNode, clique); 
		RewritingMethodType rewritingMethod = retval.getFirst();
		Clique newClique = retval.getSecond();

		newClique = this.adornNonLinearWithRewritingMethod(orNode, clique, newClique, rewritingMethod);

		if (newClique != null) {
			orNode.setChild(0, newClique);

			// make sure that the new clique has a new parent
			newClique.addParent(orNode);

			// make sure that the old clique loses a parent
			clique.removeParent(orNode);

			// remember that we may have a newly adorned clique
			this.localCliqueList.add(newClique);

			// we delay deletion of this clique because there may be other literal
			//  still pointing to this. Remember, this is a unique list
			//this.local_deleted_cliqueList.addElement(clique);

			// We should remove 'clique' from local_cliqueList. -- Natraj
			this.localCliqueList.remove(clique);
		}

		if (this.deALSContext.isTraceEnabled()) {
			orNode.resetIsDescribed();
			this.deALSContext.logTrace(logger, "Exiting adornNonLinearClique for {}", orNode.toStringTree());			
		}
	}

	private Pair<RewritingMethodType, Clique> adornDecideNonLinearRewritingMethod(PCGOrNode orNode, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering adornDecideNonLinearRewritingMethod for {}", clique.toString());
		
		RewritingMethodType rewritingMethod;
		Clique newClique = null;
		Binding binding = orNode.getBindingPattern();

		if (binding.allFree()) {
			rewritingMethod = RewritingMethodType.NAIVE;
		} else  {
			newClique = clique.copy();

			if (this.adornNonLinearTrivialPhaseOneCliquePredicate(orNode, newClique, PhaseType.PHASE1)) {
				this.deALSContext.logTrace(logger, "********** non-linear trivial phase one phase 1 succeeds ****************");

				rewritingMethod = RewritingMethodType.TRIVIAL_PHASE1;
			} else {
				// the new clique may have contaminated, so we need to clear the binding for phase 2
				newClique.clearBinding();

				if (this.adornNonLinearMagicCliquePredicate(orNode, newClique, PhaseType.PHASE1)) {
					this.deALSContext.logTrace(logger, "********** non-linear magic phase 1 succeeds ****************");

					rewritingMethod = RewritingMethodType.NON_LINEAR_MAGIC;
				} else {
					newClique = clique.copy();

					if (this.adornNonLinearGeneralMagicCliquePredicate(orNode, newClique, PhaseType.PHASE1)) {
						this.deALSContext.logTrace(logger, "********** non-linear generalized magic phase 1 succeeds ****************");

						rewritingMethod = RewritingMethodType.GENERALIZED_MAGIC;
					} else {
						newClique = null;
						rewritingMethod = RewritingMethodType.NAIVE;
					}
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting adornDecideNonLinearRewritingMethod for {}", clique.toString());
				
		return new Pair<> (rewritingMethod, newClique);
	}

	private Clique adornNonLinearWithRewritingMethod(PCGOrNode orNode, Clique clique, Clique newClique, RewritingMethodType rewritingMethod) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearWithRewritingMethod for {}", orNode.toString());
		if (newClique != null)
			this.deALSContext.logTrace(logger, newClique.toString());

		orNode.setRewritingMethod(rewritingMethod);

		switch (rewritingMethod)
		{
		case TRIVIAL_PHASE1: {
			this.deALSContext.logTrace(logger, "********** begin non-linear trivial phase one phase 2 ****************");

			newClique.clearBinding();

			if (!this.adornNonLinearTrivialPhaseOneCliquePredicate(orNode, newClique, PhaseType.PHASE2))
				throw new CompilerException("can not adorn non-linear clique in adorner");
			
			this.deALSContext.logTrace(logger, "********** non-linear trivial phase one phase 2 succeeds ****************");
		}
		break;
		case NON_LINEAR_MAGIC: {
			this.deALSContext.logTrace(logger, "********** begin non-linear magic phase 2 ****************");

			// the newClique may have contaminated, so we need to clear the binding for phase 2
			newClique.clearBinding();

			if (!this.adornNonLinearMagicCliquePredicate(orNode, newClique, PhaseType.PHASE2))
				throw new CompilerException("can not adorn non-linear clique in adorner");
			
			this.deALSContext.logTrace(logger, "********** non-linear magic phase 2 succeeds ****************");
		}
		break;
		case GENERALIZED_MAGIC: {
			this.deALSContext.logTrace(logger, "********** begin non-linear generalized magic phase 2 ****************");

			// the newClique may have contaminated, so we need to clear the binding for phase 2
			newClique.clearBinding();

			if (!this.adornNonLinearGeneralMagicCliquePredicate(orNode, newClique, PhaseType.PHASE2))
				throw new CompilerException("can not adorn non-linear clique in adorner");

			this.deALSContext.logTrace(logger, "********** non-linear generalized magic phase 2 succeeds ****************");
		}
		break;
		case NAIVE: {
			String 		predicateName = orNode.getPredicateName();
			int	 		arity = orNode.getArity();
			Binding 	binding = orNode.getBindingPattern();
			
			newClique = this.findAdornedClique(predicateName, arity, binding);

			// the code can be shared but eventually the results can not be shared
			if (newClique == null) {
				// we also know that one with all-free binding is sharable
				binding = new Binding(arity, BindingType.FREE);
				newClique = this.findAdornedClique(predicateName, arity, binding);

				if (newClique == null) {
					newClique = clique.copy();

					this.adornNonLinearNaiveCliquePredicate(orNode, newClique);
				}
			}
			// else we have generated the clique predicate with that binding, that we reach a closure
			//  for the adornment process
		}
		break;
		case TRIVIAL_PHASE2:
		case LINEAR_MAGIC:
		case SEMI_NAIVE:
		case UNKNOWN:
		default : {
			throw new CompilerException("invalid rewriting method for non-linear clique in adorner");
		}
		}

		this.deALSContext.logTrace(logger, "{}", newClique.toString());
		this.deALSContext.logTrace(logger, "Exiting adornNonLinearWithRewritingMethod for {}", orNode.toString());

		return newClique;
	}


	private boolean adornNonLinearTrivialPhaseOneCliquePredicate(PCGOrNode orNode, Clique clique, PhaseType phase) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearTrivialPhaseOneCliquePredicate for {}", orNode.toString());
		
		boolean isNonLinearTrivialPhaseOne = true;
		Binding binding = orNode.getBindingPattern();

		// if the binding is all-free, we might as well do naive-evaluation
		if (binding.allFree()) {
			isNonLinearTrivialPhaseOne = false;
		} else {
			// we determine if we need to continue the adornment in this routine
			CliquePredicate cliquePredicate = this.generateCliquePredicate(orNode.getPredicateName(), 
					orNode.getArity(), binding, clique);

			if (cliquePredicate != null) {
				cliquePredicate.setBindingPattern(binding);

				for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
					andNode.setBindingPattern(binding);
					isNonLinearTrivialPhaseOne = this.adornNonLinearTrivialPhaseOneAndNode(andNode, clique, phase);

					if (!isNonLinearTrivialPhaseOne)
						break;
				}

				if (phase == PhaseType.PHASE2 && isNonLinearTrivialPhaseOne) {
					for (PCGAndNode andNode : cliquePredicate.getExitRules()) {
						andNode.setBindingPattern(binding);
						this.adornAndNode(andNode);
					}
				}
			}
			// else we reach closure
		}

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearTrivialPhaseOneCliquePredicate for {}", orNode.toString());
		
		return isNonLinearTrivialPhaseOne;
	}

	private boolean adornNonLinearTrivialPhaseOneAndNode(PCGAndNode andNode, Clique clique, PhaseType phase) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearTrivialPhaseOneAndNode for {}", andNode.toString());
		
		CompilerVariableList boundVariableList = new CompilerVariableList();
		CompilerVariableList freeHeadVariableList = new CompilerVariableList();
		CompilerVariableList conflictVariableList = new CompilerVariableList();
		CompilerVariableList chainVariableList = new CompilerVariableList();
		boolean isNonLinearTrivialPhaseOne = false;
		
		Utilities.getFreeVariables(andNode.getArguments(), andNode.getBindingPattern(), freeHeadVariableList);
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(), boundVariableList);
		chainVariableList.appendList(boundVariableList);

		boolean allVisited = true;
		for (PCGOrNode orNode : andNode.getChildren()) {
			TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, chainVariableList);

			if (orNode.isRecursiveLiteral(clique)){
				if (!Utilities.hasSameBoundArguments(andNode, orNode)) {
					this.deALSContext.logTrace(logger, "** FAIL same bound argument rule **");

					if (phase == PhaseType.PHASE2)
						throw new CompilerException("can not fail same-bound_argument-rule in non-linear trivial phase one phase 2 in adorner");
					allVisited = false;
					break;
				}
			}
			else if (phase == PhaseType.PHASE2) {
				this.adornOrNode(orNode, boundVariableList, conflictVariableList);
			}

			// Update the bound variable list
			Utilities.getVariables(orNode, boundVariableList);
		}

		if (allVisited) {
			isNonLinearTrivialPhaseOne = true;

			// Safety Condition: all free variables in the head must be covered by bound variables in the head or body
			//                   this also includes anonymous variables
			if (!Utilities.isSubset(freeHeadVariableList, boundVariableList))
				new AdornerException("Free variables in head are not covered in the following unsafe rule:\n" + andNode.toStringAsRule());
		}

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearTrivialPhaseOneAndNode for {}", andNode.toString());
		
		return isNonLinearTrivialPhaseOne;
	}

	private boolean adornNonLinearMagicCliquePredicate(PCGOrNode orNode, Clique clique, PhaseType phase) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearMagicCliquePredicate for {}", orNode.toString());
		
		boolean isNonLinearMagic = true;
		Binding binding = orNode.getBindingPattern();

		// if the binding is all-free, we might as well do naive-evaluation
		if (binding.allFree()) {
			isNonLinearMagic = false;
		} else {
			// we determine if we need to continue the adornment in this routine
			CliquePredicate cliquePredicate = this.generateCliquePredicate(orNode.getPredicateName(), 
					orNode.getArity(), binding, clique);

			if (cliquePredicate != null) {
				cliquePredicate.setBindingPattern(binding);

				for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
					andNode.setBindingPattern(binding);
					isNonLinearMagic = this.adornNonLinearMagicAndNode(andNode, clique, phase);

					if (!isNonLinearMagic)
						break;
				}

				if (isNonLinearMagic && phase == PhaseType.PHASE2) {
					for (PCGAndNode andNode : cliquePredicate.getExitRules()) {
						andNode.setBindingPattern(binding);
						this.adornAndNode(andNode);
					}
				}
			}
			// else we reach closure
		}

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearMagicCliquePredicate for {}", orNode.toString());
		
		return isNonLinearMagic;
	}

	/*************************************************************
	 * boolean adornNonLinearMagicAndNode(PCGAndNode andNode, Clique clique, AdPhaseType phase)
	 *
	 * Given a recursive rule that fails the chain-rule, we will generate
	 *  adornment for a new clique predicate with all free binding.
	 * We can be sure that this will be the last clique predicate generated
	 *  for clique predicate with this predicate name and arity because
	 *  when we do adornment for a recursive rule with all free arguments
	 *  in the head, it will DEFINITELY fails the chain-rule.
	 *  Thus, when we generate the adornment for the clique predicate with 
	 *  all free binding again, it will terminate the adornment because
	 *  clique predicate with all free arguments has already been generated
	 **************************************************************/
	private boolean adornNonLinearMagicAndNode(PCGAndNode andNode, Clique clique, PhaseType phase) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearMagicAndNode for {}", andNode.toString());
		
		boolean hasMore = true;
		boolean isNonLinearMagic = false;
		CompilerVariableList boundVariableList = new CompilerVariableList();
		CompilerVariableList freeHeadVariableList = new CompilerVariableList();
		CompilerVariableList conflictVariableList = new CompilerVariableList();
		CompilerVariableList chainVariableList = new CompilerVariableList();

		Utilities.getFreeVariables(andNode.getArguments(), andNode.getBindingPattern(), freeHeadVariableList);
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(), boundVariableList);
		chainVariableList.appendList(boundVariableList);

		while (hasMore) {
			int count = 0;
			for (PCGOrNode orNode : andNode.getChildren()) {
				if (orNode.isRecursive() && (clique == orNode.getChild(0))) {
					// we assume all previous recursive literals do not exist in checking the chain-rule
					// assuming it does not update the boundVariableList and the chainVariableList
					//  so that we can use the same chain variable list throughout
					// the chainVariableList does not include variables from previous recursive literal
					if (!TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, 
							chainVariableList, SetBindingOption.BOUND)) {
						if (phase == PhaseType.PHASE1) {
							// if chain-rule fails, we return with failure
							this.deALSContext.logTrace(logger, "** FAIL chain-rule **");

							hasMore = false;
							break;
						} else if (phase == PhaseType.PHASE2) {
							// we can not fail chain-rule in second phase 
							// even though we allow propagation of bound variables in phase 2
							//  the chain-rule should not fail if it succeeds in phase 1
							//  because we may get more binding in chainVariableList with
							//  binding from previous recursive literals
							throw new CompilerException("can not fail chain-rule in non-linear magic phase 2 in adorner");
						}
					} else if (!this.adornNonLinearMagicCliquePredicate(orNode, clique, phase)) {
						hasMore = false;
						break;
					}
				} else {
					TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, chainVariableList);

					// we only propgate the adornment in phase 2
					if (phase == PhaseType.PHASE2)
						this.adornOrNode(orNode, boundVariableList, conflictVariableList);
				}

				// Update the bound variable list
				Utilities.getVariables(orNode, boundVariableList);
				count++;
			}

			if (count >= andNode.getNumberOfChildren()) {
				hasMore = false;
				isNonLinearMagic = true;

				// Safety Condition: all free variables in the head must be covered by bound variables in the head or body 
				//                   this also includes anonymous variables
				if (!Utilities.isSubset(freeHeadVariableList, boundVariableList))
					new AdornerException("Free variables in head are not covered in the following unsafe rule:\n" + andNode.toStringAsRule());
			}
		}

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearMagicAndNode for {} with status = {}", andNode.toString(), isNonLinearMagic);
		
		return isNonLinearMagic;
	}

	private boolean adornNonLinearGeneralMagicCliquePredicate(PCGOrNode orNode, Clique clique, PhaseType phase) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearGenMagicCliquePredicate for {}", orNode.toString());
				
		boolean isNonLinearGeneralMagic = true;
		Binding binding = orNode.getBindingPattern();

		// if the binding is all-free, we might as well do naive-evaluation
		if (binding.allFree()) {
			isNonLinearGeneralMagic = false;
		} else {
			// we determine if we need to continue the adornment in this routine
			CliquePredicate cliquePredicate = this.generateCliquePredicate(orNode.getPredicateName(), 
					orNode.getArity(), binding, clique);

			if (cliquePredicate != null) {
				cliquePredicate.setBindingPattern(binding);

				for (PCGAndNode andNode : cliquePredicate.getRecursiveRules())  {
					andNode.setBindingPattern(binding);
					isNonLinearGeneralMagic = this.adornNonLinearGeneralMagicAndNode(andNode, clique, phase);

					if (!isNonLinearGeneralMagic)
						break;
				}

				if (isNonLinearGeneralMagic && phase == PhaseType.PHASE2)
					for (PCGAndNode andNode : cliquePredicate.getExitRules()) {
						andNode.setBindingPattern(binding);
						this.adornAndNode(andNode);
					}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearGenMagicCliquePredicate for {} with status = {}", orNode.toString(), isNonLinearGeneralMagic);
		
		return isNonLinearGeneralMagic;
	}


	/*************************************************************
	 * boolean adornNonLinearGenMagicAndNode(PCGAndNode andNode, Clique clique, AdPhaseType phase)
	 *
	 * Given a recursive rule that fails the chain-rule, we will generate
	 *  adornment for a new clique predicate with all free binding.
	 * We can be sure that this will be the last clique predicate generated
	 *  for clique predicate with this predicate name and arity because
	 *  when we do adornment for a recursive rule with all free arguments
	 *  in the head, it will DEFINITELY fail the chain-rule.
	 *  Thus, when we generate the adornment for the clique predicate with 
	 *  all free binding again, it will terminate the adornment because
	 *  clique predicate with all free arguments has already been generated
	 **************************************************************/
	private boolean adornNonLinearGeneralMagicAndNode(PCGAndNode andNode, Clique clique, PhaseType phase) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearGeneralMagicAndNode for {}", andNode.toString());
		
		boolean isNonLinearGeneralMagic = false;
		CompilerVariableList boundVariableList = new CompilerVariableList();
		CompilerVariableList freeHeadVariableList = new CompilerVariableList();
		CompilerVariableList conflictVariableList = new CompilerVariableList();
		CompilerVariableList chainVariableList = new CompilerVariableList();

		Utilities.getFreeVariables(andNode.getArguments(), andNode.getBindingPattern(), freeHeadVariableList);
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getBindingPattern(), boundVariableList);
		chainVariableList.appendList(boundVariableList);

		int count = 0;
		for (PCGOrNode orNode : andNode.getChildren()) {
			if (orNode.isRecursive() && (clique == orNode.getChild(0))) {
				// we assume all previous recursive literals DO EXIST in checking the chain-rule
				if (!TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, 
						conflictVariableList, chainVariableList)) {
					if (phase == PhaseType.PHASE1) {
						// if chain-rule fails, we return with failure
						this.deALSContext.logTrace(logger, "** FAIL chain-rule **");

						break;
					} else if (phase == PhaseType.PHASE2) {
						// we can not fail chain-rule in second phase
						// even though we allow propagation of bound variables in phase 2
						//  the chain-rule should not fail if it succeeds in phase 1
						//  because we may get more binding in chainVariableList with
						//  binding from previous recursive literals
						throw new CompilerException("can not fail chain-rule in non-linear generalized magic phase 2 in adorner");
					}
				} else if (!this.adornNonLinearGeneralMagicCliquePredicate(orNode, clique, phase))
					break;
			} else {
				TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, chainVariableList);

				// we only propagate the adornment in phase 2
				if (phase == PhaseType.PHASE2)
					this.adornOrNode(orNode, boundVariableList, conflictVariableList);
			}

			// Update the bound variable list
			Utilities.getVariables(orNode, boundVariableList);
			count++;
		}

		if (count >= andNode.getNumberOfChildren()) {
			isNonLinearGeneralMagic = true;

			// Safety Condition: all free variables in the head must be covered by bound variables in the head or body 
			//                   this also includes anonymous variables
			if (!Utilities.isSubset(freeHeadVariableList, boundVariableList))
				new AdornerException("Free variables in head are not covered in the following unsafe rule:\n" + andNode.toStringAsRule());
		}

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearGeneralMagicAndNode for {} with status = {}", andNode.toString(), isNonLinearGeneralMagic);
		
		return isNonLinearGeneralMagic;
	}

	private void adornNonLinearNaiveCliquePredicate(PCGOrNode orNode, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearNaiveCliquePredicate for {}", orNode.toString());
		
		int arity = orNode.getArity();
		Binding	binding	= new Binding(arity, BindingType.FREE); // naive evaluation requires all free binding 

		// we determine if we need to continue the adornment in this routine
		CliquePredicate cliquePredicate = this.generateCliquePredicate(orNode.getPredicateName(), arity, binding, clique);

		if (cliquePredicate != null) {
			cliquePredicate.setBindingPattern(binding);

			for (PCGAndNode andNode : cliquePredicate.getExitRules()) {
				andNode.setBindingPattern(binding);
				this.adornAndNode(andNode);
			}

			for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
				andNode.setBindingPattern(binding);
				this.adornNonLinearNaiveAndNode(andNode, clique);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearNaiveCliquePredicate for {}", orNode.toString());
	}

	/*************************************************************
	 * adornNonLinearNaiveAndNode(PCGAndNode andNode, Clique clique)
	 *
	 * Given a recursive rule that fails the chain-rule, we will generate
	 *  adornment for a new clique predicate with all free binding.
	 * We can be sure that this will be the last clique predicate generated
	 *  for clique predicate with this predicate name and arity because
	 *  when we do adornment for a recursive rule with all free arguments
	 *  in the head, it will DEFINITELY fail the chain-rule.
	 *  Thus, when we generate the adornment for the clique predicate with 
	 *  all free binding again, it will terminate the adornment because
	 *  clique predicate with all free arguments has already been generated.
	 **************************************************************/
	private void adornNonLinearNaiveAndNode(PCGAndNode andNode, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering adornNonLinearNaiveAndNode for {}", andNode.toString());
		
		// we do not use the chainVariableList at all
		CompilerVariableList 	boundVariableList = new CompilerVariableList();
		CompilerVariableList 	freeHeadVariableList = new CompilerVariableList();
		CompilerVariableList 	conflictVariableList = new CompilerVariableList();
		CompilerVariableList 	chainVariableList = new CompilerVariableList();
		Binding 		allFreeBinding;

		// since it should be all free, we have no bound arguments from the head
		Utilities.getFreeVariables(andNode.getArguments(), andNode.getBindingPattern(), freeHeadVariableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			if (orNode.isRecursive() && (clique == orNode.getChild(0)))  {
				TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, 
						conflictVariableList, chainVariableList, SetBindingOption.BOUND);

				allFreeBinding = new Binding(orNode.getArity(), BindingType.FREE);
				orNode.setBindingPattern(allFreeBinding);

				this.adornNonLinearNaiveCliquePredicate(orNode, clique);
			} else {
				TopDownAdorner.setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, chainVariableList);

				this.adornOrNode(orNode, boundVariableList, conflictVariableList);
			}

			// Update the bound variable list
			Utilities.getVariables(orNode, boundVariableList);
		}

		// Safety Condition: all free variables in the head must be covered by bound variables in the head or body this also includes anonymous variables
		if (!Utilities.isSubset(freeHeadVariableList, boundVariableList))
			new AdornerException("Free variables in head are not covered in the following unsafe rule:\n" + andNode.toStringAsRule());

		this.deALSContext.logTrace(logger, "Exiting adornNonLinearNaiveAndNode for {}", andNode.toString());
	}

	private static boolean setCliqueOrNodeBindingPattern(PCGOrNode orNode, PCGAndNode andNode, 
			CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList, 
			CompilerVariableList chainVariableList) {
		return setCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList, chainVariableList, SetBindingOption.ALL);
	}		

	private static boolean setCliqueOrNodeBindingPattern(PCGOrNode orNode, PCGAndNode andNode, 
			CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList, 
			CompilerVariableList chainVariableList, SetBindingOption setBindingOption) {

		boolean status = false;
		CompilerVariableList variableList = new CompilerVariableList();
		CompilerVariableList orNodeBoundVariableList = new CompilerVariableList();
		CompilerVariableList argumentVariableList = new CompilerVariableList();

		orNode.clearBindingPattern();
		
		int position = 0;
		for (CompilerTypeBase argument : orNode.getArguments()) {
			argumentVariableList.clear();
			Utilities.getVariables(argument, argumentVariableList);

			if (Utilities.allVariablesBound(argumentVariableList, boundVariableList)) {
				orNode.setBindingPattern(position, BindingType.BOUND);
				Utilities.mergeVariableLists(orNodeBoundVariableList, argumentVariableList);
			} else {
				orNode.setBindingPattern(position, BindingType.FREE);
			}

			Utilities.mergeVariableLists(variableList, argumentVariableList);
			position++;
		}

		if (!orNodeBoundVariableList.isEmpty() && Utilities.isSubset(orNodeBoundVariableList, chainVariableList)) {
			if (setBindingOption == SetBindingOption.ALL || setBindingOption == SetBindingOption.CHAIN)
				Utilities.mergeVariableLists(chainVariableList, variableList);

			status = true;
		}

		// Safety Condition: ensure that no variable in conflict set is used
		for (int i = 0; i < variableList.size(); i++) {
			if (conflictVariableList.contains(variableList.get(i)))
				new AdornerException("Unsafe variable " + variableList.get(i).toString() + " used in " + orNode.toStringAsPredicate() + " of the following rule:\n" + andNode.toStringAsRule());
		}

		// Safety Condition: ensure that all free variables in negation or deletion are in the conflict set
		if (orNode.getPredicate().isNegative()/* || orNode.getPredicate().isDelete()*/) {
			for (int i = 0; i < variableList.size(); i++) {
				if (!boundVariableList.contains(variableList.get(i)))
					conflictVariableList.add(variableList.get(i));
			}
		}

		return status;
	}

	private void adornClique(PCGOrNode orNode) {
		CliqueBase baseClique = orNode.getBaseClique();

		if (baseClique != null) {
			switch (baseClique.getType())
			{
			case CLIQUE: {
				Clique	clique = (Clique) baseClique;
				clique.determineRecursiveType();

				if (clique.isLinearRecursive())
					this.adornLinearClique(orNode, clique);
				else if (clique.isNonLinearRecursive())
					this.adornNonLinearClique(orNode, clique);
			}
			break;

			case XY_CLIQUE: {
				this.adornXYClique((XYClique) baseClique);
			}
			break;

			}
		}
	}

	private CliquePredicate generateCliquePredicate(String predicateName, int arity, Binding binding, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering generateCliquePredicate for [name = {}, arity = {}, binding = {}]", predicateName.toString(), arity, binding.toString());
		this.deALSContext.logTrace(logger, clique.toString());

		CliquePredicate	cliquePredicate = clique.getCliquePredicate(predicateName, arity, binding);

		// check if the clique predicate has been generated
		if (cliquePredicate != null) {
			cliquePredicate = null;
		} else {
			// we check if there is any clique predicate that has not been adorned
			Binding noBinding = new Binding(arity);
			noBinding.setAsNoBinding();
			cliquePredicate = clique.getCliquePredicate(predicateName, arity, noBinding);

			// check if the clique predicate has been generated
			if (cliquePredicate == null) {
				// We simple get the clique predicate regardless of the binding
				CliquePredicate dummyCliquePredicate = clique.getCliquePredicate(predicateName, arity);

				if (dummyCliquePredicate == null)
					throw new CompilerException("can not find clique predicate in adorner");
				
				
				cliquePredicate = dummyCliquePredicate.copy();
				cliquePredicate.clearBinding();
				clique.addCliquePredicate(cliquePredicate);

				if (this.deALSContext.isTraceEnabled()) {					
					cliquePredicate.resetIsDescribed();					
					dummyCliquePredicate.resetIsDescribed();
					this.deALSContext.logTrace(logger, "new clique predicate is created: ");					
					this.deALSContext.logTrace(logger, cliquePredicate.toStringTree(clique));
					this.deALSContext.logTrace(logger, "from the clique predicate: ");					
					this.deALSContext.logTrace(logger, dummyCliquePredicate.toStringTree(clique));
				}
			}
		}

		if (cliquePredicate == null) {
			// we already have the clique predicate generated at this point
			this.deALSContext.logTrace(logger, "****Reached Closure for Clique Predicate Adornment****");
		} else {
			this.deALSContext.logTrace(logger, "****Continue Clique Predicate Adornment****");
			this.deALSContext.logTrace(logger, "The clique predicate is: ");
			this.deALSContext.logTrace(logger, "{}", cliquePredicate.toString());
		}

		this.deALSContext.logTrace(logger, "{}", clique.toString());
		this.deALSContext.logTrace(logger, "Exiting generateCliquePredicate for [name = {}, arity = {}, binding = {}]", predicateName.toString(), arity, binding.toString());
		
		return cliquePredicate;
	}

	private Clique findAdornedClique(String predicateName, int arity, Binding binding) {
		Clique foundClique = null;
		for (CliqueBase clique : this.localCliqueList) {
			if (clique instanceof Clique) {
				if (((Clique)clique).getCliquePredicate(predicateName, arity, binding) != null) {
					foundClique = (Clique) clique;
					break;
				}
			}
		}

		return foundClique;
	}

	private boolean isCliqueAdorned(CliqueBase baseClique) {
		return this.localCliqueList.contains(baseClique);
	}

	/*************************************************************
	 * adornXYClique(XYClique xyClique)
	 * - we regenerate the actual execution adornment for all XYCliques
	 *   because the original adornment done was used for determining rewriting strategy.
	 *   Now, we need to regenerate the adornment in this xyClique such no binding is passed from the head
	 **************************************************************/
	private void adornXYClique(XYClique xyClique) {
		this.deALSContext.logTrace(logger, "Entering adornXYClique");
		this.deALSContext.logTrace(logger, "{}", xyClique.toString());

		// do not repeat adornment
		if (!this.isCliqueAdorned(xyClique)) {
			this.localCliqueList.add(xyClique);

			for (XYCliquePredicate xyCliquePredicate : xyClique.getCliquePredicates()) {
				xyCliquePredicate.clearBindingPattern();

				for (PCGAndNode andNode : xyCliquePredicate.getExitRules()) {
					andNode.clearBindingPattern();
					this.adornAndNode(andNode);
				}
				
				for (PCGAndNode andNode : xyCliquePredicate.getXRules()) {
					andNode.clearBindingPattern();
					this.adornXYCliqueAndNode(andNode, xyClique);
				}

				for (PCGAndNode andNode : xyCliquePredicate.getYRules()) {
					andNode.clearBindingPattern();
					this.adornXYCliqueAndNode(andNode, xyClique);
				}

				for (PCGAndNode andNode : xyCliquePredicate.getCopyRules()) {
					andNode.clearBindingPattern();
					this.adornXYCliqueAndNode(andNode, xyClique);
				}

				for (PCGAndNode andNode : xyCliquePredicate.getDeleteRules()) {
					andNode.clearBindingPattern();
					this.adornXYCliqueAndNode(andNode, xyClique);
				}
			}
		}
		
		this.deALSContext.logTrace(logger, "{}", xyClique.toString());
		this.deALSContext.logTrace(logger, "Exiting adornXYClique");
	}

	private void adornXYCliqueAndNode(PCGAndNode andNode, XYClique xyClique) {
		this.deALSContext.logTrace(logger, "Entering adornXYCliqueAndNode for {}", andNode.toString());
		
		CompilerVariableList boundVariableList = new CompilerVariableList();
		CompilerVariableList freeHeadVariableList = new CompilerVariableList();
		CompilerVariableList conflictVariableList = new CompilerVariableList();

		Utilities.getVariables(andNode, freeHeadVariableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			if (orNode.isRecursiveLiteral(xyClique)) {
				this.setXYCliqueOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList);
			} else {
				TopDownAdorner.setOrNodeBindingPattern(orNode, andNode, boundVariableList, conflictVariableList);
				this.adornOrNode(orNode, boundVariableList, conflictVariableList);
			}

			Utilities.getVariables(orNode, boundVariableList);
		}

		// Safety Condition: all free variables in the head must be covered by bound variables 
		// in the head or body this also includes anonymous variables
		if (!Utilities.isSubset(freeHeadVariableList, boundVariableList))
			new AdornerException("Free variables in head are not covered in the following unsafe rule:\n" + andNode.toStringAsRule());

		// Safety Condition: ensure that no variable in conflict set is used
		for (int i = 0; i < freeHeadVariableList.size(); i++) {
			if (conflictVariableList.contains(freeHeadVariableList.get(i)))
				new AdornerException("Unsafe variable " + freeHeadVariableList.get(i).toString() + " used in head of rule:\n" + andNode.toStringAsRule());
		}

		this.deALSContext.logTrace(logger, "Exiting adornXYCliqueAndNode for {}", andNode.toString());
	}

	/*************************************************************
	 * setXYCliqueOrNodeBindingPattern()
	 * This routine does 4 things:
	 *  1. set binding for the orNode
	 *  2. it updates the boundVariableList with all variables in orNode
	 *  3. it updates the conflictVariableList for negation and deletion
	 *  4. it checks for safety
	 **************************************************************/
	@SuppressWarnings("static-method")
	private boolean setXYCliqueOrNodeBindingPattern(PCGOrNode orNode, PCGAndNode andNode, 
			CompilerVariableList boundVariableList, CompilerVariableList conflictVariableList) {

		boolean	status = true;
		CompilerVariableList variableList = new CompilerVariableList();
		CompilerVariableList argumentVariableList = new CompilerVariableList();

		orNode.clearBindingPattern();

		int position = 0;
		for (CompilerTypeBase argument : orNode.getArguments()) {
			argumentVariableList.clear();
			Utilities.getVariables(argument, argumentVariableList);

			if (Utilities.allVariablesBound(argumentVariableList, boundVariableList))
				orNode.setBindingPattern(position, BindingType.BOUND);
			else
				orNode.setBindingPattern(position, BindingType.FREE);

			Utilities.mergeVariableLists(variableList, argumentVariableList);
			position++;
		}		

		// Safety Condition: ensure that no variable in conflict set is used
		for (int i = 0; i < variableList.size(); i++) {
			if (conflictVariableList.contains(variableList.get(i))) {
				status = false;
				new AdornerException("Unsafe variable " + variableList.get(i).toString() + " used in " + orNode.toStringAsPredicate() + " of the following rule:\n" + andNode.toStringAsRule());
			}
		}

		// Safety Condition: ensure that all free variables in negation or deletion are in the conflict set
		if (orNode.getPredicate().isNegative()/* || orNode.getPredicate().isDelete()*/) {
			for (int i = 0; i < variableList.size(); i++) {
				if (!boundVariableList.contains(variableList.get(i)))
					conflictVariableList.add(variableList.get(i));
			}
		}

		return status;
	}
}
