package edu.ucla.cs.wis.bigdatalog.compiler.adornment;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Quad;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.Rewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYClique;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYCliquePredicate;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

	/* This class applied execution binding to the PCG
	 * 6/11/2013 APS */
public class BottomUpAdorner {
	private static Logger logger = LoggerFactory.getLogger(BottomUpAdorner.class.getName());

	private DeALSContext 		deALSContext;
	private List<CliqueBase> 	localCliqueList;
	
	public BottomUpAdorner(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
	}
	
	public void adornExecutionQueryForm(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering adornExecutionQueryForm for {}", orNode.toString());
						
		this.localCliqueList = new ArrayList<>();

		BottomUpAdorner.setBottomUpQueryFormBindingPattern(orNode);

		CompilerVariableList boundVariableList = new CompilerVariableList();

		this.adornBottomUpOrNode(orNode, boundVariableList);

		this.deALSContext.logTrace(logger, "Exiting adornExecutionQueryForm for {}", orNode.toString());
	}
	
	private boolean isCliqueAdorned(CliqueBase baseClique) {
		return this.localCliqueList.contains(baseClique);
	}
	
	private static void setBottomUpQueryFormBindingPattern(PCGOrNode orNode) {
		orNode.clearExecutionBindingPattern();

		int position = 0;
		for (CompilerTypeBase argument : orNode.getArguments()) {
			if (argument.isGround())
				orNode.setExecutionBindingPattern(position, BindingType.BOUND);
			else
				orNode.setExecutionBindingPattern(position, BindingType.FREE);
			
			position++;
		}
	}
	
	private void adornBottomUpOrNode(PCGOrNode orNode, CompilerVariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering adornBottonUpOrNode for {}", orNode.toString());
		
		if (orNode.isRecursive()) {
			CliqueBase baseClique = orNode.getBaseClique();

			switch (baseClique.getType())
			{
			case CLIQUE: {
				this.adornBottomUpClique((Clique) baseClique);
			}
			break;

			case XY_CLIQUE: {
				this.adornBottomUpXYClique((XYClique) baseClique);
			}
			break;

			default:
				break;
			}
		} else if (orNode.getPredicate().isBuiltIn()) {
			this.adornBottomUpBuiltInOrNode(orNode, boundVariableList);
		} else if (orNode.getPredicate().isDerived()) {
			boolean isMaterializedAdornment = false;

			// We generate Materialized Or Node for Linear Supplementary Node and thus,
			// we set the materialized rule with all free binding
			if (orNode.getPredicateName().startsWith(Rewriter.LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX) 
					|| orNode.getPredicateName().startsWith(Rewriter.NON_LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)
					|| orNode.getPredicateName().startsWith(Rewriter.REVERSE_MAGIC_PREDICATE_NAME_PREFIX)) // APS 11/29/2013
				isMaterializedAdornment = true;

			PCGAndNode andNode;
			for (PCGOrNodeChild childNode : orNode.getChildren()) {
				andNode = (PCGAndNode)childNode;
				if (isMaterializedAdornment)
					andNode.clearExecutionBindingPattern();
				else
					BottomUpAdorner.setBottomUpAndNodeBindingPattern(andNode, orNode);

				this.adornBottomUpAndNode(andNode);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting adornBottomUpOrNode for {}", orNode.toString());
	}
	
	private void adornBottomUpAndNode(PCGAndNode andNode) {
		this.deALSContext.logTrace(logger, "Entering adornBottomUpAndNode for {}", andNode.toString());
		
		CompilerVariableList boundVariableList = new CompilerVariableList();

		Utilities.getBoundVariables(andNode.getArguments(), andNode.getExecutionBindingPattern(), boundVariableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			this.setBottomUpOrNodeBindingPattern(orNode, boundVariableList);
			
			this.adornBottomUpOrNode(orNode, boundVariableList);
			
			Utilities.getVariables(orNode, boundVariableList);
		}

		this.deALSContext.logTrace(logger, "Exiting adornBottomUpAndNode for {}", andNode.toString());
	}
	
	private void adornBottomUpBuiltInOrNode(PCGOrNode orNode, CompilerVariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering adornBottomUpBuiltInOrNode for {}", orNode.toString());
		
		CompilerVariableList boundVariableList1 = boundVariableList.copyList();
		CompilerVariableList boundVariableList2 = boundVariableList.copyList();
		CompilerVariableList boundVariableList3 = boundVariableList.copyList();
		CompilerVariableList ifBoundVariableList = new CompilerVariableList();
		CompilerVariableList thenBoundVariableList = new CompilerVariableList();
		CompilerVariableList elseBoundVariableList = new CompilerVariableList();
		boolean intersectsBoundVariableList;

		if (orNode.isIfThenElseNode()) {
			PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0); 
			PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1); 
			PCGAndNode elseAndNode = (PCGAndNode) orNode.getChild(2);

			this.setIfThenElseAndNodeExecutionBindingPattern(orNode, ifAndNode, thenAndNode, elseAndNode);

			Quad<Boolean, CompilerVariableList, CompilerVariableList, CompilerVariableList> quadRetval 
			= Utilities.assignIfThenElseBoundVariableList(orNode, boundVariableList, 
					boundVariableList1, boundVariableList2, boundVariableList3);

			intersectsBoundVariableList = quadRetval.getFirst();
			ifBoundVariableList = quadRetval.getSecond();
			thenBoundVariableList = quadRetval.getThird();
			elseBoundVariableList = quadRetval.getFourth();		    

			this.adornBottomUpDummyAndNode(ifAndNode, ifBoundVariableList);
			this.adornBottomUpDummyAndNode(thenAndNode, thenBoundVariableList);
			this.adornBottomUpDummyAndNode(elseAndNode, elseBoundVariableList);

			if (intersectsBoundVariableList) {
				boundVariableList.clear();
				Utilities.getIntersectingVariables(thenBoundVariableList, elseBoundVariableList, boundVariableList);
			}
		} else if (orNode.isIfThenNode()) {
			Pair<CompilerVariableList, CompilerVariableList> retvalPair = Utilities.assignIfThenBoundVariableList(orNode, 
					boundVariableList, boundVariableList1, boundVariableList2);

			ifBoundVariableList = retvalPair.getFirst();
			thenBoundVariableList = retvalPair.getSecond();

			PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0);
			PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1);
			this.setIfThenElseAndNodeExecutionBindingPattern(orNode, ifAndNode, thenAndNode, null);
			this.adornBottomUpDummyAndNode(ifAndNode, ifBoundVariableList);
			this.adornBottomUpDummyAndNode(thenAndNode, thenBoundVariableList);
		} else if (orNode.isAggregateNode() || orNode.isFSAggregateNode()
				|| orNode.isBuiltInPredicateType(BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE)
				|| orNode.isBuiltInPredicateType(BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS)
				|| orNode.isBuiltInPredicateType(BuiltInPredicateType.SORT)
				|| orNode.isBuiltInPredicateType(BuiltInPredicateType.TOPK)) {
			PCGAndNode aggregateAndNode = (PCGAndNode) orNode.getChild(0); 
			aggregateAndNode.setExecutionBindingPattern(orNode.getExecutionBindingPattern());
			this.adornBottomUpAndNode(aggregateAndNode);
		}

		this.deALSContext.logTrace(logger, "Exiting adornBottomUpBuiltInOrNode for {}", orNode.toString());
	}

	private void adornBottomUpDummyAndNode(PCGAndNode andNode, CompilerVariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering adornBottomUpDummyAndNode for {}", andNode.toString());
				
		Utilities.getBoundVariables(andNode.getArguments(), andNode.getExecutionBindingPattern(), boundVariableList);

		for (PCGOrNode orNode : andNode.getChildren()) {
			this.setBottomUpOrNodeBindingPattern(orNode, boundVariableList);
			
			this.adornBottomUpOrNode(orNode, boundVariableList);
			
			Utilities.getVariables(orNode, boundVariableList);
		}

		this.deALSContext.logTrace(logger, "Exiting adornBottomUpDummyAndNode for {}", andNode.toString());
	}

	private void adornBottomUpClique(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering adornBottomUpClique"); 
		this.deALSContext.logTrace(logger, clique.toString());

		// do not repeat adornment for a clique
		if (!this.isCliqueAdorned(clique)) {
			this.localCliqueList.add(clique);

			for (CliquePredicate cliquePredicate : clique.getCliquePredicates()) {
				for (PCGAndNode andNode : cliquePredicate.getExitRules()) {
					andNode.clearExecutionBindingPattern();					
					this.adornBottomUpCliqueAndNode(andNode);
				}

				for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
					andNode.clearExecutionBindingPattern();
					this.adornBottomUpCliqueAndNode(andNode);
				}
			}
		}

		this.deALSContext.logTrace(logger, "{}", clique.toString());
		this.deALSContext.logTrace(logger, "Exiting adornBottomUpClique");
	}

	private void adornBottomUpCliqueAndNode(PCGAndNode andNode) {
		this.deALSContext.logTrace(logger, "Entering adornBottomUpCliqueAndNode for {}", andNode.toString());
		
		CompilerVariableList boundVariableList = new CompilerVariableList();

		//boolean isFirst = true;
		for (PCGOrNode orNode : andNode.getChildren()) {
			/*if (isFirst) { 
			 APS 1/2/2014 - trying to fix when magic sets isn't used - some top down not same as bottom up for base relations
			 doesn't seem to matter though, because lack of bound value is the real problem, not the adornment  
				if (orNode.getPredicate().isBase()) {
					if (!orNode.getBindingPattern().allFree()) {
						System.out.println(orNode.getPredicate().toStringShort());

						// keep top down binding
						for (int i = 0 ; i < orNode.getArity(); i++) {
							if (orNode.getBinding(i) == BindingType.BOUND) {
								orNode.setExecutionBindingPattern(i, BindingType.BOUND);
								boundVariableList.add((Variable)orNode.getArgument(i));
							} else {
								orNode.setExecutionBindingPattern(i, BindingType.FREE);
							}
						}
						
						Utilities.getVariables(orNode, boundVariableList);
						
					} else {
						BottomUpAdorner.setBottomUpOrNodeBindingPattern(orNode, boundVariableList);
						this.adornBottomUpOrNode(orNode, boundVariableList);
						Utilities.getVariables(orNode, boundVariableList);
					}
				} else {
					BottomUpAdorner.setBottomUpOrNodeBindingPattern(orNode, boundVariableList);
					this.adornBottomUpOrNode(orNode, boundVariableList);
					Utilities.getVariables(orNode, boundVariableList);
				}
				isFirst = false;
			} else {*/
			this.setBottomUpOrNodeBindingPattern(orNode, boundVariableList);
				this.adornBottomUpOrNode(orNode, boundVariableList);
				Utilities.getVariables(orNode, boundVariableList);
			//}
		}

		this.deALSContext.logTrace(logger, "Exiting adornBottomUpCliqueAndNode for {}", andNode.toString());
	}

	/*************************************************************
	 * adornBottomUpXYClique(XYClique xyClique)
	 *
	 * - we regenerate the actual execution adornment for all xy_cliques
	 *   because the original adornment done was used for determining the rewriting strategy.
	 *   Now, we need to regenerate the adornment in this xy_clique such that
	 *   no binding is passed from the head
	 **************************************************************/
	private void adornBottomUpXYClique(XYClique xyClique) {
		this.deALSContext.logTrace(logger, "Entering adornBottomUpXYClique");
		this.deALSContext.logTrace(logger, xyClique.toString());

		// do not repeat adornment for any clique
		if (!this.isCliqueAdorned(xyClique)) {
			this.localCliqueList.add(xyClique);

			for (XYCliquePredicate xyCliquePredicate : xyClique.getCliquePredicates()) {

				for (PCGAndNode andNode : xyCliquePredicate.getExitRules()) {
					andNode.clearExecutionBindingPattern();
					this.adornBottomUpXYCliqueAndNode(andNode);
				}

				for (PCGAndNode andNode : xyCliquePredicate.getXRules()) {
					andNode.clearExecutionBindingPattern();
					this.adornBottomUpXYCliqueAndNode(andNode);
				}

				for (PCGAndNode andNode : xyCliquePredicate.getYRules()) {
					andNode.clearExecutionBindingPattern();
					this.adornBottomUpXYCliqueAndNode(andNode);
				}

				for (PCGAndNode andNode : xyCliquePredicate.getCopyRules()) {
					andNode.clearExecutionBindingPattern();
					this.adornBottomUpXYCliqueAndNode(andNode);
				}

				for (PCGAndNode andNode : xyCliquePredicate.getDeleteRules()) {
					andNode.clearExecutionBindingPattern();
					this.adornBottomUpXYCliqueAndNode(andNode);
				}
			}
		}

		this.deALSContext.logTrace(logger, "{}", xyClique.toString());
		this.deALSContext.logTrace(logger, "Exiting adornBottomUpXYClique");
	}

	private void adornBottomUpXYCliqueAndNode(PCGAndNode andNode) { 
		this.deALSContext.logTrace(logger, "Entering adornBottomUpXYCliqueAndNode for {}", andNode.toString());
		
		CompilerVariableList boundVariableList = new CompilerVariableList();

		for (PCGOrNode orNode : andNode.getChildren()) {			
			this.setBottomUpOrNodeBindingPattern(orNode, boundVariableList);			
			this.adornBottomUpOrNode(orNode, boundVariableList);
			
			// for xy-delete rules, don't update boundVariableList for delete
			// literal, that is, delete literals will have free binding. -- HW
			/*      if (delete_rule_p && (*andNode.getPredicateName() == *(orNode.getPredicateName())))
		          delete_rule_p = 0;
		      if (!delete_rule_p) */
			Utilities.getVariables(orNode, boundVariableList);
		}

		this.deALSContext.logTrace(logger, "Exiting adornBottomUpXYCliqueAndNode for {}", andNode.toString());
	}
	
	private static void setBottomUpAndNodeBindingPattern(PCGAndNode andNode, PCGOrNode orNode) {
		Binding binding = andNode.getExecutionBindingPattern();

		int position = 0;
		for (CompilerTypeBase argument : andNode.getArguments()) {
			if (argument.containsAnyAggregate())
				binding.setBinding(position, BindingType.FREE);
			else
				binding.setBinding(position, orNode.getExecutionBinding(position));
			position++;
		}
	}

	/*************************************************************
	 * setBottomUpPcgOrNodeBindingPattern(PCGOrNode orNode, CompilerTypeList boundVariableList)
	 *
	 * This routine does 3 things:
	 *  1. set binding for the orNode
	 *  2. it updates the boundVariableList with all variables in orNode
	 *  3. compare new with old binding
	 **************************************************************/
	private void setBottomUpOrNodeBindingPattern(PCGOrNode orNode, CompilerVariableList boundVariableList) {
		this.deALSContext.logTrace(logger, "Entering setBottomUpOrNodeBindingPattern for {}", orNode.toString());
				
		CompilerVariableList argumentVariableList = new CompilerVariableList();

		orNode.clearExecutionBindingPattern();

		int position = 0;
		for (CompilerTypeBase argument : orNode.getArguments()) {
			argumentVariableList.clear();
			Utilities.getVariables(argument, argumentVariableList);

			if (Utilities.allVariablesBound(argumentVariableList, boundVariableList))
				orNode.setExecutionBindingPattern(position, BindingType.BOUND);
			else
				orNode.setExecutionBindingPattern(position, BindingType.FREE);

			position++;
		}
		
		this.deALSContext.logTrace(logger, "Exiting setBottomUpOrNodeBindingPattern with {}", orNode.toString());
	}
	
	private void setIfThenElseAndNodeExecutionBindingPattern(PCGOrNode orNode, PCGAndNode ifAndNode, 
			PCGAndNode thenAndNode, PCGAndNode elseAndNode) {

		this.deALSContext.logTrace(logger, "Entering setIfThenElseAndNodeExecutionBindingPattern for {}", orNode.toString());
				
		// Assuming that all arguments of both andNode and orNode are variables
		ifAndNode.clearExecutionBindingPattern();
		thenAndNode.clearExecutionBindingPattern();
		
		boolean noBindingFromIfNode = false;		
		int index = 0;
		int position = 0;
		for (CompilerTypeBase term : ifAndNode.getArguments()){
			if ((index = orNode.getArguments().getPosition(term)) > -1)
				ifAndNode.setExecutionBindingPattern(position, orNode.getExecutionBinding(index));
			else if (term.isBound())
				ifAndNode.setExecutionBindingPattern(position, BindingType.BOUND);
			else
				ifAndNode.setExecutionBindingPattern(position, BindingType.FREE);
			
			position++;
		}

		if ((ifAndNode.getLastChild()).isFalseNode())
			noBindingFromIfNode = true;

		position = 0;
		for (CompilerTypeBase term : thenAndNode.getArguments()) {
			if ((index = orNode.getArguments().getPosition(term)) > -1)
				thenAndNode.setExecutionBindingPattern(position, orNode.getExecutionBinding(index));
			else if (term.isBound())
				thenAndNode.setExecutionBindingPattern(position, BindingType.BOUND);
			else if ((!noBindingFromIfNode) && (ifAndNode.getArguments().contains(term)))
				thenAndNode.setExecutionBindingPattern(position, BindingType.BOUND);
			else
				thenAndNode.setExecutionBindingPattern(position, BindingType.FREE);

			position++;
		}

		if (elseAndNode != null) {
			elseAndNode.clearExecutionBindingPattern();

			position = 0;
			for (CompilerTypeBase term : elseAndNode.getArguments()) {
				if ((index = orNode.getArguments().getPosition(term)) > -1)
					elseAndNode.setExecutionBindingPattern(position, orNode.getExecutionBinding(index));
				else if (term.isBound())
					elseAndNode.setExecutionBindingPattern(position, BindingType.BOUND);
				else
					elseAndNode.setExecutionBindingPattern(position, BindingType.FREE);

				position++;
			}
		}

		this.deALSContext.logTrace(logger, "Exiting setIfThenElseAndNodeExecutionBindingPattern with {}", orNode.toString());
	}
}