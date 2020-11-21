package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class CliqueGenerator {
	private static Logger logger = LoggerFactory.getLogger(CliqueGenerator.class.getName());

	protected DeALSContext					deALSContext;
	protected int 							count;
	protected int 							cliqueCount;
	protected List<DerivedPredicate>		derivedPredicates;
	protected List<DerivedPredicateNode> 	derivedPredicateNodes;
	protected List<BasicClique> 			cliques;
	protected Stack<DerivedPredicateNode> 	derivedPredicateNodeStack;
	protected List<DerivedPredicateNode>	recursiveDerivedPredicateNodes;

	protected List<Clique> 					regeneratedCliques;
	protected List<CliquePredicateNode>		cliquePredicateNodes;
	protected Stack<CliquePredicateNode> 	cliquePredicateNodeStack;
	protected List<CliquePredicateNode>		recursiveCliquePredicateNodes;
		
	// this constructor used to generate the initial cliques
	public CliqueGenerator(DeALSContext deALSContext, RuleDependencyGraph dependencyGraph) {
		this.deALSContext			= deALSContext;
		this.cliqueCount 			= 1;
		this.cliques 				= dependencyGraph.getCliques();
		this.derivedPredicates 		= dependencyGraph.getDerivedPredicates();
		this.derivedPredicateNodes 	= dependencyGraph.getDerivedPredicateNodes();
	}
	
	// this constructor used to regenerate cliques 
	public CliqueGenerator(DeALSContext deALSContext) {
		this.deALSContext			= deALSContext;
		this.cliqueCount 			= 1;
		this.regeneratedCliques 	= new ArrayList<>();
		this.cliquePredicateNodes 	= new ArrayList<>();
	}
	
	private void reset() {
		this.regeneratedCliques.clear();
		this.cliquePredicateNodes.clear();
	}
	
	public List<BasicClique> getCliques() { return this.cliques; }
	
	public List<DerivedPredicate> getNonRecursivePredicates() { return this.derivedPredicates; }
		
	public void generateCliques() {
		this.cliqueCount = 1;
		this.derivedPredicateNodeStack = new Stack<>();
		this.recursiveDerivedPredicateNodes = new ArrayList<>();

		// STEP 1 - generate cliques
		for (DerivedPredicateNode derivedPredicateNode : this.derivedPredicateNodes) {
			this.count = 0;
			if (!derivedPredicateNode.visited())
				this.searchClique(derivedPredicateNode);			
		}

		for (DerivedPredicateNode derivedPredicateNode : this.recursiveDerivedPredicateNodes) {
			if (derivedPredicateNode.getCliqueId() == null) {
				BasicClique clique = new BasicClique(String.valueOf(this.cliqueCount++));
				derivedPredicateNode.setCliqueId(clique.getCliqueId());
				clique.addDerivedPredicate(derivedPredicateNode.getDerivedPredicate());
				// Reset to indicate that the derived predicate has been used
				derivedPredicateNode.clearDerivedPredicate();
				this.cliques.add(clique);
			}
		}

		// STEP 2 - generate list of non-recursive rules left over
		this.derivedPredicates.clear();
		DerivedPredicate derivedPredicate;

		// For those nodes that are used in the cliques, their
		// derived predicates have been used and set to null
		// Thus, only those remaining nodes with non-null derived
		// predicates are non-recursive and put back to the original derived predicates
		for (DerivedPredicateNode derivedPredicateNode : this.derivedPredicateNodes) {
			if (derivedPredicateNode != null) {
				derivedPredicate = derivedPredicateNode.getDerivedPredicate();
				if (derivedPredicate != null) {
					this.derivedPredicates.add(derivedPredicate);
					derivedPredicateNode.clearDerivedPredicate();
				}				
			}
		}
	}
	
	private void searchClique(DerivedPredicateNode derivedPredicateNode) {
		DerivedPredicateNode dependentDRNode;
		DerivedPredicateNode stackDRNode;
		BasicClique basicClique;
		
		derivedPredicateNode.setVisited();
		derivedPredicateNode.setLowLink(this.count);
		derivedPredicateNode.setDepth(this.count);
		this.count++;
		this.derivedPredicateNodeStack.push(derivedPredicateNode);

		for (AnalysisNode<DerivedPredicate> dependentderivedPredicateNode : derivedPredicateNode.getDependentNodes()) {	
			dependentDRNode = (DerivedPredicateNode)dependentderivedPredicateNode;

			if (dependentDRNode.visited()) {
				//System.out.println("Already visited: " + dependentDRNode.getPredicateName());
				// check stack
				for (int j = 0; j < this.derivedPredicateNodeStack.size(); j++) {
					if (this.derivedPredicateNodeStack.get(j) == dependentDRNode) {
						this.recursiveDerivedPredicateNodes.add(dependentDRNode);

						if (dependentDRNode.getDepth() < derivedPredicateNode.getDepth() 
								&& dependentDRNode.getDepth() < derivedPredicateNode.getLowLink())
							derivedPredicateNode.setLowLink(dependentDRNode.getDepth());
						break;
					}
				}
			} else {
				//System.out.println("Searching: " + dependentDRNode.getPredicateName());
				this.searchClique(dependentDRNode);

				if (dependentDRNode.getLowLink() < derivedPredicateNode.getLowLink())
					derivedPredicateNode.setLowLink(dependentDRNode.getLowLink());
					//derivedPredicateNode.setLowLink(dependentDRNode.getDepth());
			}
		}

		if (derivedPredicateNode.getDepth() == derivedPredicateNode.getLowLink()
				&& !this.derivedPredicateNodeStack.isEmpty()) {
			stackDRNode = this.derivedPredicateNodeStack.pop();
			
			// we do not generate new clique for only mutually-recursive node
			if (derivedPredicateNode != stackDRNode) {
				basicClique = new BasicClique(String.valueOf(this.cliqueCount++));
				String cliqueId = basicClique.getCliqueId();
				this.cliques.add(basicClique);
				stackDRNode.setCliqueId(cliqueId);
				basicClique.addDerivedPredicate(stackDRNode.getDerivedPredicate());
				// Reset to indicate that the derived predicate has been used
				stackDRNode.clearDerivedPredicate();

				while (derivedPredicateNode != stackDRNode 
						&& !this.derivedPredicateNodeStack.isEmpty()) {
					stackDRNode = this.derivedPredicateNodeStack.pop();
					stackDRNode.setCliqueId(cliqueId);
					basicClique.addDerivedPredicate(stackDRNode.getDerivedPredicate());
					// Reset to indicate that the derived predicate has been used
					stackDRNode.clearDerivedPredicate();
				}
			}
		}
		this.count--;
	}
	
	/*
	public boolean checkCliques() {	    
		for (BasicClique clique : this.cliques) {
			if (!this.checkClique(clique))
				return true;
		}

		return false;
	}

	public boolean checkClique(BasicClique clique) {
		int numberOfExitRules = 0;
		boolean hasError = false;

		for (DerivedPredicate derivedPredicate : clique.getDerivedPredicates()) {
			for (Rule rule : derivedPredicate.getRules()) {
				Pair<Boolean, Boolean> recursiveLiteralCheck = this.hasRecursiveLiterals(rule.getBody(), clique, rule, hasError); 
				if (!recursiveLiteralCheck.getFirst())
					numberOfExitRules++;

				hasError = recursiveLiteralCheck.getSecond();
			}
		}

		if (numberOfExitRules == 0) {
			Runtime.getErrorHandler().errRcaNoExitRule(clique.getCliqueId());
			hasError = true;
		}

		return !hasError;
	}
	
	private Pair<Boolean, Boolean> hasRecursiveLiterals(CompilerTypeList literals, BasicClique clique, Rule rule, boolean hasError) {
		boolean isRecursive = false;
		Predicate literal;

		for (compilerTypeObject compilerTypeObject : literals) {
			literal = (Predicate)compilerTypeObject;

			if (literal instanceof IfThenElsePredicate) {
				IfThenElsePredicate itePredicate = (IfThenElsePredicate) literal;

				Pair<Boolean, Boolean> checkIfLiterals;
				Pair<Boolean, Boolean> checkThenLiterals;
				Pair<Boolean, Boolean> checkElseLiterals;

				if ((((checkIfLiterals = this.hasRecursiveLiterals(itePredicate.getIfLiterals(), clique, rule, hasError)) != null) 
						&& checkIfLiterals.getFirst())
						|| (((checkThenLiterals = this.hasRecursiveLiterals(itePredicate.getThenLiterals(), clique, rule, hasError)) != null)
								&& checkThenLiterals.getFirst())
								|| (((checkElseLiterals = this.hasRecursiveLiterals(itePredicate.getElseLiterals(), clique, rule, hasError)) != null)
										&& checkElseLiterals.getFirst())) {
					Runtime.getErrorHandler().errRcaUnstratifiedLiteral(clique.getCliqueId(), rule, literal);
					isRecursive = true;
					hasError = true;
				}
			} else if (literal instanceof IfThenPredicate) {
				IfThenPredicate itPredicate = (IfThenPredicate) literal;

				Pair<Boolean, Boolean> checkIfLiterals;
				Pair<Boolean, Boolean> checkThenLiterals;

				if ((((checkIfLiterals = this.hasRecursiveLiterals(itPredicate.getIfLiterals(), clique, rule, hasError)) != null) 
						&& checkIfLiterals.getFirst())
						|| (((checkThenLiterals = this.hasRecursiveLiterals(itPredicate.getThenLiterals(), clique, rule, hasError)) != null)
								&& checkThenLiterals.getFirst())) {
					Runtime.getErrorHandler().errRcaUnstratifiedLiteral(clique.getCliqueId(), rule, literal);
					isRecursive = true;
					hasError = true;
				}
			} else if (literal instanceof ForeverPredicate) {
				Pair<Boolean, Boolean> checkForeverLiterals;
				if (((checkForeverLiterals 
						= this.hasRecursiveLiterals(((ForeverPredicate)literal).getForeverLiterals(), clique, rule, hasError)) != null)
						&& checkForeverLiterals.getFirst()) {
					Runtime.getErrorHandler().errRcaUnstratifiedLiteral(clique.getCliqueId(), rule, literal);
					isRecursive = true;
					hasError = true;
				}
			} else {
				BasicClique clique2 = this.getClique(literal.getPredicateName(), literal.getArity());
				if ((clique2 != null) && (clique.equals(clique2)))  {
					isRecursive = true;

					if (literal.isNegative()) {
						Runtime.getErrorHandler().errRcaUnstratifiedNegation(clique.getCliqueId(), rule, literal);
						hasError = true;
					}
				}
			}
		}

		return new Pair<Boolean, Boolean>(isRecursive, hasError);
	}
*/
	
	public void regenerateClique(Clique oldClique, List<Clique> regeneratedCliques) {
		this.reset();
		//this.cliqueCount = 1;
		if (regeneratedCliques != null)
			this.regeneratedCliques = regeneratedCliques;
		
		this.deALSContext.logTrace(logger, "[Regenerating Clique : " + oldClique.getCliqueId() + "]");
		this.deALSContext.logTrace(logger, oldClique.toString());
		this.deALSContext.logTrace(logger, "[Stage 1 Generating Clique Dependency Graph]");
		
		this.generateCliqueDependencyGraph(oldClique);
		
		this.deALSContext.logTrace(logger, this.toStringCliqueDependencyGraph());
		this.deALSContext.logTrace(logger, "[Stage 2 Generating New Cliques]");
		
		this.regenerateCliques();
		
		this.deALSContext.logTrace(logger, this.toStringCliques());
		//	this.displayCliques(Runtime.getTracer().outs());		
		//	this.displayNonRecursiveCliquePredicate(Runtime.getTracer().outs());
		this.deALSContext.logTrace(logger, "[Stage 3 Patching New Clique to PCG]");
		
		this.patchPCGToClique(oldClique);

		// do this after patching otherwise we might point to the wrong clique
		this.separateRulesInCliques();

		CliquePredicate cliquePredicate;
		CliquePredicateNode cliquePredicateNode;
		
		// Delete the remaining clique predicates not used in regenerated cliques
		for (int i = this.cliquePredicateNodes.size() - 1; i >= 0; i--)  {
			cliquePredicateNode = this.cliquePredicateNodes.remove(i);

			if ((cliquePredicate = cliquePredicateNode.getCliquePredicate()) != null) {
				cliquePredicate.delete(this.deALSContext);
				cliquePredicateNode.resetCliquePredicate();
			}
		}

		this.cliquePredicateNodes.clear();
		
		this.deALSContext.logTrace(logger, "[Complete ReGenerating Cliques.]");
	}

	private void generateCliqueDependencyGraph(Clique oldClique) {
		this.deALSContext.logTrace(logger, "Entering generateCliqueDependencyGraph");
		
		CliquePredicate cliquePredicate;

		// for each clique predicate, create one clique predicate node
		for (int i = oldClique.getNumberOfCliquePredicates() - 1; i >= 0; i--) {
			cliquePredicate = oldClique.removeCliquePredicate(i);
			CliquePredicateNode cliquePredicateNode = new CliquePredicateNode();
			cliquePredicateNode.setCliquePredicate(cliquePredicate);
			this.cliquePredicateNodes.add(cliquePredicateNode);
		}

		// predicate not defined in this clique will not be used including those in body
		// However, we still need to navigate through the body to make sure that we establish the dependency
		for (CliquePredicateNode cliquePredicateNode : this.cliquePredicateNodes) {
			cliquePredicate = cliquePredicateNode.getCliquePredicate();

			for (PCGAndNode andNode : cliquePredicate.getExitRules())
				this.setDependentcliquePredicateNodes(andNode);
			
			for (PCGAndNode andNode : cliquePredicate.getRecursiveRules())
				this.setDependentcliquePredicateNodes(andNode);
		}
		
		this.deALSContext.logTrace(logger, "Exiting generateCliqueDependencyGraph");		
	}

	private void setDependentcliquePredicateNodes(PCGAndNode andNode) {		
		CliquePredicateNode headcliquePredicateNode = this.getCliquePredicateNode(andNode.getPredicateName(), 
				andNode.getArity(), andNode.getBindingPattern());
		
		if (headcliquePredicateNode != null) {
			CliquePredicateNode cliquePredicateNode;
			for (PCGOrNode orNode : andNode.getChildren()) {
				cliquePredicateNode = this.getCliquePredicateNode(orNode.getPredicateName(), 
						orNode.getArity(), orNode.getBindingPattern());

				if (cliquePredicateNode != null)
					headcliquePredicateNode.addDependentNode(cliquePredicateNode);
			}
		}
	}

	private void regenerateCliques() {
		Clique clique;

		this.count = 1;
		this.cliquePredicateNodeStack = new Stack<>();
		this.recursiveCliquePredicateNodes = new ArrayList<>();

		// STEP 1 - search through predicate connection graph and identify recursive components
		for (CliquePredicateNode cliquePredicateNode : this.cliquePredicateNodes) {
			if (!cliquePredicateNode.visited())
				this.searchClique(cliquePredicateNode);
		}

		// STEP 2 - use recursive components found in step 1 to generate cliques
		for (CliquePredicateNode cliquePredicateNode : this.recursiveCliquePredicateNodes) {
			if (cliquePredicateNode.getCliqueId() == null) {
				clique = new Clique(String.valueOf(this.cliqueCount++));
				cliquePredicateNode.setCliqueId(clique.getCliqueId());
				clique.addCliquePredicate(cliquePredicateNode.getCliquePredicate());
				cliquePredicateNode.resetCliquePredicate();
				this.regeneratedCliques.add(clique);
			}
		}
	}

	private void searchClique(CliquePredicateNode cliquePredicateNode) {
		CliquePredicateNode	dependentcliquePredicateNode;

		cliquePredicateNode.setVisited();
		cliquePredicateNode.setLowLink(this.count);
		cliquePredicateNode.setDepth(this.count);
		this.count++;
		this.cliquePredicateNodeStack.push(cliquePredicateNode);

		for (AnalysisNode<CliquePredicate> dependentNode : cliquePredicateNode.getDependentNodes()) {
			dependentcliquePredicateNode = (CliquePredicateNode)dependentNode;
			if (dependentcliquePredicateNode.visited()) {
				for (CliquePredicateNode cliquePredicateNodeFromStack : this.cliquePredicateNodeStack) {
					if (cliquePredicateNodeFromStack == dependentcliquePredicateNode) {
						this.recursiveCliquePredicateNodes.add(dependentcliquePredicateNode);

						if ((dependentcliquePredicateNode.getDepth() < cliquePredicateNode.getDepth()) 
								&& (dependentcliquePredicateNode.getDepth() < cliquePredicateNode.getLowLink()))
							cliquePredicateNode.setLowLink(dependentcliquePredicateNode.getDepth());
						break;
					}
				}
			} else {
				this.searchClique(dependentcliquePredicateNode);

				if (dependentcliquePredicateNode.getLowLink() < cliquePredicateNode.getLowLink())
					cliquePredicateNode.setLowLink(dependentcliquePredicateNode.getLowLink());
			}
		}

		if (cliquePredicateNode.getDepth() == cliquePredicateNode.getLowLink()) {
			if (!this.cliquePredicateNodeStack.isEmpty()) {
				CliquePredicateNode cliquePredicateNodeStackNode = this.cliquePredicateNodeStack.pop();

				// we do not generate new clique for only mutually-recursive node
				if (cliquePredicateNode != cliquePredicateNodeStackNode) {
					Clique clique = new Clique(String.valueOf(this.cliqueCount++));
					String cliqueId = clique.getCliqueId();
					this.regeneratedCliques.add(clique);
					cliquePredicateNodeStackNode.setCliqueId(cliqueId);
					clique.addCliquePredicate(cliquePredicateNodeStackNode.getCliquePredicate());
					cliquePredicateNodeStackNode.resetCliquePredicate();

					while ((cliquePredicateNode != cliquePredicateNodeStackNode) 
							&& (!this.cliquePredicateNodeStack.isEmpty())) {
						cliquePredicateNodeStackNode = this.cliquePredicateNodeStack.pop();
						cliquePredicateNodeStackNode.setCliqueId(cliqueId);
						clique.addCliquePredicate(cliquePredicateNodeStackNode.getCliquePredicate());
						cliquePredicateNodeStackNode.resetCliquePredicate();
					}
				}
			}
		}
	}

	private void patchPCGToClique(Clique oldClique) {
		this.deALSContext.logTrace(logger, "Entering patchPCGToClique");
		/*	
		VariableList variables = new VariableList();
		VariableList variableList = variables.copyList();
		
		Utilities.getVariables(oldClique, variables);
		
		for (Variable variable : variables)
			variableList.add(variable);
*/
		String 				predicateName;
		int 				arity;
		Binding 			binding;
		Binding 			allFreeBinding;
		PCGOrNode 			parent;		
		Clique 				clique;
		CliquePredicate 	cliquePredicate;
		CliquePredicateNode cliquePredicateNode;
		
		for (int i = 0; i < oldClique.getNumberOfParents(); i++) {
			parent = oldClique.getParent(i);
			predicateName = parent.getPredicateName();
			arity = parent.getArity();
			binding = parent.getBindingPattern();

			clique = this.getRegeneratedClique(predicateName, arity, binding);

			// we could also use a clique with all-free binding and
			//  do post-select in getting the results
			if (clique == null) {
				allFreeBinding = new Binding(arity, BindingType.FREE);
				clique = this.getRegeneratedClique(predicateName, arity, allFreeBinding);
			}

			// patch clique
			if (clique != null) {
				this.deALSContext.logDebug(logger, "[Patching Clique : {}]", parent);
				
				parent.resetBaseClique(clique);
				clique.addParent(parent);
			} else {                  
				// patch pcg
				this.deALSContext.logDebug(logger, "[Patching PCGOrNode : {}]", parent);

				cliquePredicateNode = this.getCliquePredicateNode(predicateName, arity, binding);

				if ((cliquePredicateNode != null) 
						&& ((cliquePredicate = cliquePredicateNode.getCliquePredicate()) != null)) {
					parent.getChildren().clear();
					parent.getPredicate().setAsDerivedPredicate();
					parent.setAsNonRecursive();

					for (PCGAndNode andNode : cliquePredicate.getExitRules())
						parent.addChild(andNode.copyTree(/*variableList*/));

					for (PCGAndNode andNode : cliquePredicate.getRecursiveRules())
						parent.addChild(andNode.copyTree(/*variableList*/));

				} else {
					//throw new CompilerException("Parent node of clique can not be patched.  Parent node = " + parent.toString());
				}
			}
		}
		
		this.deALSContext.logTrace(logger, "Exiting patchPCGToClique");
	}

	private void separateRulesInCliques() {
		PCGAndNode rule;
		List<PCGAndNode> recursiveRules = new ArrayList<>();
		List<PCGAndNode> exitRules = new ArrayList<>();

		for (Clique clique : this.regeneratedCliques) {
			for (CliquePredicate cliquePredicate : clique.getCliquePredicates()) {
				for (int i = cliquePredicate.getNumberOfExitRules() - 1; i >= 0; i--) {
					rule = cliquePredicate.getExitRule(i);

					if (rule.isRecursiveRule(clique)) {
						this.deALSContext.logTrace(logger, "*** Separating Exit as Recursive Rule:");
						this.deALSContext.logTrace(logger, "{}", rule.toStringAsRule());

						recursiveRules.add(rule);
						cliquePredicate.removeExitRule(i);
					}
				}

				for (int i = cliquePredicate.getNumberOfRecursiveRules() - 1; i >= 0; i--) {
					rule = cliquePredicate.getRecursiveRule(i);

					if (!rule.isRecursiveRule(clique)) {
						this.deALSContext.logTrace(logger, "*** Separating Recursive as Exit Rule:");
						this.deALSContext.logTrace(logger, "{}", rule.toStringAsRule());

						exitRules.add(rule);
						cliquePredicate.removeRecursiveRule(i);
					}
				}

				for (PCGAndNode andNode : exitRules)
					cliquePredicate.addExitRule(andNode);

				for (PCGAndNode andNode : recursiveRules)
					cliquePredicate.addRecursiveRule(andNode);
			}
		}
	}
	
	private Clique getRegeneratedClique(String predicateName, int arity, Binding binding) {
		for (Clique clique : this.regeneratedCliques) {
			if (clique.getCliquePredicate(predicateName, arity, binding) != null)
				return clique;
		}
		
		return null;
	}

	private CliquePredicateNode getCliquePredicateNode(String predicateName, int arity, Binding binding) {
		for (CliquePredicateNode cliquePredicateNode : this.cliquePredicateNodes) {
			if ((cliquePredicateNode.getCliquePredicate() != null)
					&& cliquePredicateNode.getPredicateName().equals(predicateName) 
					&& (arity == cliquePredicateNode.getArity()) 
					&& cliquePredicateNode.getBindingPattern().equals(binding)) {
				return cliquePredicateNode;
			}
		}

		return null;
	}
	
	protected String toStringCliqueDependencyGraph() {
		StringBuilder retval = new StringBuilder();
		retval.append("Clique Dependency Graph: ");

		for (CliquePredicateNode cliquePredicateNode : this.cliquePredicateNodes)
			retval.append(cliquePredicateNode.toString());

		retval.append("End Clique Dependency Graph");
		return retval.toString();
	}

	protected String toStringCliques() { 
		StringBuilder retval = new StringBuilder();
		retval.append("Cliques: ");

		for (Clique clique : this.regeneratedCliques)
			retval.append(clique.toString());

		retval.append("\nEnd Cliques"); 
		return retval.toString();
	}

	protected String toStringNonRecursiveCliquePredicate() {
		StringBuilder retval = new StringBuilder();
		retval.append("Non Recursive Clique Predicate: ");

		for (CliquePredicateNode cliquePredicateNode : this.cliquePredicateNodes) {
			if (cliquePredicateNode.getCliquePredicate() != null)
				retval.append(cliquePredicateNode.getCliquePredicate().toString());
		}

		retval.append("\nEnd Non Recursive Clique Predicate");
		return retval.toString();
	}
}
