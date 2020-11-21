package edu.ucla.cs.wis.bigdatalog.compiler.rewriting;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGAndNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNodeChild;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerString;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class Compressor {
	private static Logger logger = LoggerFactory.getLogger(Compressor.class.getName());
	
	private enum CompressMode { COMPRESS, SUSPEND, NONE; }
	
	private enum SubstituteMode { ALL, DOWNWARD; }
	
	protected DeALSContext				deALSContext;
	protected Rewriter 				rewriter;
	protected List<CliqueBase> 		cliqueList;
	protected Stack<PCGAndNode> 		parentPCGAndNodeStack;
	protected CompilerVariableList 	genericUtilityList;
	protected List<Integer>			deferredSubstituteArgIndices;

	public Compressor(DeALSContext deALSContext, Rewriter rewriter) {
		this.deALSContext	 				= deALSContext;
		this.rewriter						= rewriter;
		this.cliqueList 					= new ArrayList<>();
		this.parentPCGAndNodeStack 			= new Stack<>();
		this.genericUtilityList				= new CompilerVariableList();
		this.deferredSubstituteArgIndices 	= new ArrayList<>();
	}
	
	protected void compressQueryForm(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering compressQueryForm for {}", orNode.toString());
				
		this.cliqueList.clear();
		this.parentPCGAndNodeStack.clear();
		this.compressOrNode(orNode);
		this.parentPCGAndNodeStack.clear();
		this.cliqueList.clear();

		this.deALSContext.logTrace(logger, "Exiting compressQueryForm for {}", orNode.toString());
	}

	protected void compressOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering compressOrNode for {}", orNode.toString());
		
		CompressMode compressMode = CompressMode.NONE;

		//System.out.println("Compressing: " + orNode.toStringAsPredicate());
		
		if (orNode.isRecursive()) {
			this.compressClique(orNode);
		} else {
			if (orNode.getPredicate().isDerived()) {					
				compressMode = CompressMode.COMPRESS;
			} else {				
				if (orNode.isAggregateNode())
					compressMode = CompressMode.COMPRESS;
				else if (orNode.isIfThenElseNode() 
						|| orNode.isIfThenNode())
					compressMode = CompressMode.SUSPEND;
				//else
				//	System.out.println("Ignoring :" + orNode.toStringAsPredicate());
			}
			
			if ((compressMode == CompressMode.COMPRESS) || (compressMode == CompressMode.SUSPEND)) {
				for (PCGOrNodeChild andNode : orNode.getChildren())
					this.compressAndNode((PCGAndNode)andNode, compressMode);				
			}
		}

		this.deALSContext.logTrace(logger, "Exiting compressOrNode for {}", orNode.toString());
	}

	protected void compressAndNode(PCGAndNode andNode, CompressMode compressMode) {
		this.deALSContext.logTrace(logger, "Entering compressAndNode for {}", andNode.toString());
		
		// rules with negative id have already been re-written, so do not compress
		//if (andNode.getRuleNumber() >= 0) {		
		//if (!andNode.isRewritten()) {
			for (PCGOrNode orNode : andNode.getChildren())
				this.compressOrNode(orNode);
	
			if (compressMode == CompressMode.COMPRESS) {
				//this.parentPCGAndNodeStack.clear();
				//System.out.println("Substituting in: "+andNode.toStringAsPredicate());
				this.substituteAndNode(andNode, SubstituteMode.ALL);
			}
		//}

		this.deALSContext.logTrace(logger, "Exiting compressAndNode for {}", andNode.toString());
	}

	//Compress those or nodes that are not recursive with respect to this clique
	protected void compressNonCliqueOrNodes(Clique clique) {
		this.deALSContext.logTrace(logger, "Entering compressNonCliqueOrNodes");

		for (CliquePredicate cliquePredicate : clique.getCliquePredicates()) {
			
			for (PCGAndNode andNode : cliquePredicate.getExitRules())
				this.compressAndNode(andNode, CompressMode.COMPRESS);
			
			for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
				this.parentPCGAndNodeStack.push(andNode);
				for (PCGOrNode orNode : andNode.getChildren()) {
					//System.out.println(orNode.toStringAsPredicate());
					//if (!(orNode.isRecursiveLiteral(clique)))
						this.compressOrNode(orNode);
				}
				
				this.substituteAndNode(andNode, SubstituteMode.ALL);
				this.parentPCGAndNodeStack.clear();
			}
		}

		this.deALSContext.logTrace(logger, "Exiting compressNonCliqueOrNodes");
	}

	protected void compressClique(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering compressClique for {}", orNode.toString());
		
		CliqueBase baseClique = orNode.getBaseClique();

		if ((baseClique != null) && (!this.cliqueList.contains(baseClique))) {
			this.cliqueList.add(baseClique);
		
			switch (baseClique.getType()) {
				case CLIQUE:
				{
					this.compressNonCliqueOrNodes((Clique) baseClique);
				}
				break;
	
				case XY_CLIQUE:
				{
					// Unknown, to be implemented 
				}
				break;
			}			
		}
		
		this.deALSContext.logTrace(logger, "Exiting compressClique for {}", orNode.toString());
	}
	

	protected void substituteAndNode(PCGAndNode andNode, SubstituteMode substituteMode) {
		this.deALSContext.logTrace(logger, "Entering substituteAndNode for {} and substituteMode = {}", andNode.toString(), substituteMode);
		
		int i = 0;
		int j;
		int numberOfSubstituteOrNodes;
		PCGOrNode orNode;
		PCGOrNode substituteOrNode;
		PCGAndNode substituteAndNode;
		PCGAndNode nestedAndNode;

		while (i < andNode.getNumberOfChildren()) {
			orNode = andNode.getChild(i);

			if (orNode.getPredicateName().equals(BuiltInPredicate.RETURN_PREDICATE_NAME)) {
				i++;
				continue;
			}

			// Only one rule
			if (orNode.getPredicate().isDerived()
					&& (!orNode.isRecursive())
					&& (orNode.getNumberOfChildren() == 1)
					&& ((substituteAndNode = (PCGAndNode) orNode.getChild(0)) != null)
					//&& (substituteAndNode.getRuleNumber() >= 0)	 // APS added 3/5/2013 - rules already rewritten should be ignored
					&& (!substituteAndNode.isRewritten())
					&& (!substituteAndNode.containsAnyAggregate())) {
				numberOfSubstituteOrNodes = substituteAndNode.getNumberOfChildren();

				if ((orNode.getPredicate().isNegative()) && (numberOfSubstituteOrNodes != 1)) {
					i++;
				} else {
					this.substituteOrNode(andNode, orNode, substituteMode);
					andNode.removeChild(i);

					if (orNode.getPredicate().isNegative()) {
						// We can assume (numberOfSubstituteOrNodes == 1)
						substituteOrNode = substituteAndNode.removeChild(0);
						andNode.insertChild(i, substituteOrNode);

						if (substituteOrNode.getPredicate().isPositive())
							substituteOrNode.getPredicate().setAsNegative();
						else if (substituteOrNode.getPredicate().isNegative())
							substituteOrNode.getPredicate().setAsPositive();
					} else {
						for (j = substituteAndNode.getNumberOfChildren() - 1; j >= 0; j--) {
							substituteOrNode = substituteAndNode.removeChild(j);
							andNode.insertChild(i, substituteOrNode);
						}
					}

					i += numberOfSubstituteOrNodes;
				}
			} else if (orNode.getPredicate().isDerived() && orNode.isRecursive()) {
				compressClique(orNode);

				i++;
				//System.out.println(orNode.toStringAsPredicate());
				//this.parentPCGAndNodeStack.push(andNode);
				//this.substituteAndNode(nestedAndNode, substituteMode);
				//this.parentPCGAndNodeStack.pop();				
			} else {
				if (orNode.getPredicate() instanceof BuiltInPredicate) {
					BuiltInPredicate bip = (BuiltInPredicate)orNode.getPredicate();
					if (bip.isIfThenElse() || bip.isIfThen()) {
						this.parentPCGAndNodeStack.push(andNode);
	
						// In the IF of ifthenelse and ifthen,
						// we can always substitute upwards unless our parents
						// are in the THEN or ELSE part
						nestedAndNode = (PCGAndNode) orNode.getChild(0);
						this.substituteAndNode(nestedAndNode, substituteMode);
	
						for (j = 1; j < orNode.getNumberOfChildren(); j++) {
							nestedAndNode = (PCGAndNode) orNode.getChild(j);
							this.substituteAndNode(nestedAndNode, SubstituteMode.DOWNWARD);
						}
	
						this.parentPCGAndNodeStack.pop();
					} /*else if (bip.isReadAggregate() || bip.isReadAggregateFS()) {
						this.parentPCGAndNodeStack.push(andNode);
						if (orNode.getChild(0) instanceof PCGAndNode) {
							nestedAndNode = (PCGAndNode)orNode.getChild(0);
							//this.substituteOrNode(nestedAndNode, orNode, SubstituteMode.ALL);
							this.substituteOrNode(orNode);
						} else {
							this.compressClique(orNode);
						}
	
						this.parentPCGAndNodeStack.pop();
					}*/
				}

				i++;
			}
		}

		this.deALSContext.logTrace(logger, "Exiting substituteAndNode for {} and substituteMode = {}", andNode.toString(), substituteMode);		
	}

	protected void substituteOrNode(PCGAndNode andNode, PCGOrNode orNode, SubstituteMode substituteMode) {
		this.deALSContext.logTrace(logger, "Entering substituteOrNode for {} and {} and substituteMode = {}", andNode.toString(), orNode.toString(), substituteMode);
				
		CompilerTypeBase arg1;
		CompilerTypeBase arg2;
		PCGAndNode substituteAndNode = (PCGAndNode) orNode.getChild(0);
		PCGAndNode parentAndNode;

		// We need to rename all variables within substituteAndNode
		this.genericUtilityList.clear();
		Utilities.getVariables(substituteAndNode, this.genericUtilityList);
		Utilities.getVariables(substituteAndNode.getChildren(), this.genericUtilityList);

		for (int i = 0; i < this.genericUtilityList.size(); i++)
			this.genericUtilityList.get(i).renameVariableName();

		// We are using the this.genericUtilityList to store all variables that have been
		// used for downward substitution. This is to handle the case where the 
		// substituteAndNode has two arguments with the same variable. For example,
		// Given p(X, X) as the sub_andNode and p(A, B) as the orNode,
		// If X is first substituted by A and then A is then substituted by B,
		// we will lose the constraint that A and B is equal.
		this.genericUtilityList.clear();

		// We need to defer downward substitution because to substitute
		// complex terms upward, those complex terms may contain variables
		// that may be substituted downward in other arguments.
		// Thus, instead of keep track of what has been substituted upward,
		// we simple defer the upward substitution and perform all the
		// downward substitution first.
		this.deferredSubstituteArgIndices.clear();

		for (int i = 0; i < orNode.getArity(); i++) {
			arg1 = orNode.getArgument(i);
			arg2 = substituteAndNode.getArgument(i);

			if (arg2.equals(new CompilerString("nil")))
				continue;

			// If two terms are the same object, no substitution is necessary.
			if (arg1 != arg2) {
				// In the nested case such as ifthenelse, ifthen,
				// we can not substitute upward and in such case, we will introduce
				// equality rewrite to facilitate downward substitution
				if (arg2.isVariable()) {
					if (this.genericUtilityList.contains((CompilerVariable) arg2)) {
						this.substituteArgumentWithEqualityNode(substituteAndNode, i);
						// Get the tmp variable
						arg2 = substituteAndNode.getArgument(i);
						this.substituteArgumentInAndNode(substituteAndNode, arg2, arg1);
					} else {
						if (arg1.isVariable())
							this.genericUtilityList.add((CompilerVariable) arg1);

						this.substituteArgumentInAndNode(substituteAndNode, arg2, arg1);
					}
				} else if ((arg1.isVariable()) && (substituteMode != SubstituteMode.DOWNWARD)) {
					// This is ugly but hopefully downward substitution is infrequent
					// (and normally is) and the effort to allow downward substitution
					// is probably worth it since it will facilitate better rewrite
					this.deferredSubstituteArgIndices.add(i);
				}
				// If two arguments are ground and they are the same, the equality is
				// trivially true and thus, no equality needs to be generated
				else if ((!arg1.isGround()) || (!arg2.isGround()) || (!(arg1.equals(arg2)))) {
					this.substituteArgumentWithEqualityNode(substituteAndNode, i);
					// Get the tmp variable
					arg2 = substituteAndNode.getArgument(i);
					this.substituteArgumentInAndNode(substituteAndNode, arg2, arg1);
				}
			}
		}

		this.genericUtilityList.clear();

		//for (int index = 0; index < this.deferredSubstituteArgIndices.size(); index++) {
		for (Integer index : this.deferredSubstituteArgIndices) {		
			//int i = ((CompilerInteger) this.deferredSubstituteArgIndices.get(index)).getValue();
			arg1 = orNode.getArgument(index);
			arg2 = substituteAndNode.getArgument(index);

			for (int j = this.parentPCGAndNodeStack.size() - 1; j >= 0; j--) {
				parentAndNode = this.parentPCGAndNodeStack.get(j);
				this.substituteArgumentInAndNode(parentAndNode, arg1, arg2);
			}

			this.substituteArgumentInAndNode(andNode, arg1, arg2);
			this.substituteArgumentInAndNode(substituteAndNode, arg1, arg2);
		}

		this.deferredSubstituteArgIndices.clear();

		this.deALSContext.logTrace(logger, "Exiting substituteOrNode for {} and {} and substituteMode = {}", andNode.toString(), orNode.toString(), substituteMode);
	}
	/*
	protected void substituteOrNode(PCGOrNode orNode) {
		if (Runtime.isTraceEnabled())
			this.deALSContext.logTrace(logger, "Entering substituteOrNode for {} ", orNode.toString());
				
		CompilerTypeBase arg1;
		CompilerTypeBase arg2;
		PCGAndNode substituteAndNode = (PCGAndNode) orNode.getChild(0);
		PCGAndNode parentAndNode;

		this.deferredSubstituteArgIndices.clear();

		for (int i = 0; i < orNode.getArity(); i++) {
			arg1 = orNode.getArgument(i);
			arg2 = substituteAndNode.getArgument(i);

			if (arg2.equals(new CompilerString("nil")))
				continue;
			
			// If two terms are the same object, no substitution is necessary.
			if (arg1 != arg2) {
				if (arg2.isVariable()) {
					if (this.genericUtilityList.contains((Variable) arg2)) {
						this.substituteArgumentWithEqualityNode(substituteAndNode, i);
						// Get the tmp variable
						arg2 = substituteAndNode.getArgument(i);
						this.substituteArgumentInAndNode(substituteAndNode, arg2, arg1);
					} else {
						if (arg1.isVariable())
							this.genericUtilityList.add((Variable) arg1);

						this.substituteArgumentInAndNode(substituteAndNode, arg2, arg1);
					}
				} else if (arg1.isVariable()) {
					// This is ugly but hopefully downward substitution is infrequent
					// (and normally is) and the effort to allow downward substitution
					// is probably worth it since it will facilitate better rewrite
					this.deferredSubstituteArgIndices.add(i);
				}
			}
		}

		this.genericUtilityList.clear();

		//for (int index = 0; index < this.deferredSubstituteArgIndices.size(); index++) {
		for (Integer index : this.deferredSubstituteArgIndices) {		
			//int i = ((CompilerInteger) this.deferredSubstituteArgIndices.get(index)).getValue();
			arg1 = orNode.getArgument(index);
			arg2 = substituteAndNode.getArgument(index);

			for (int j = this.parentPCGAndNodeStack.size() - 1; j >= 0; j--) {
				parentAndNode = this.parentPCGAndNodeStack.get(j);
				this.substituteArgumentInAndNode(parentAndNode, arg1, arg2);
			}

			//this.substituteArgumentInAndNode(andNode, arg1, arg2);
			this.substituteArgumentInAndNode(substituteAndNode, arg1, arg2);
		}

		this.deferredSubstituteArgIndices.clear();

		if (Runtime.isTraceEnabled())
			this.deALSContext.logTrace(logger, "Exiting substituteOrNode for {}", orNode.toString());
	}
*/

	protected void substituteArgumentWithEqualityNode(PCGAndNode andNode, int index) {
		this.deALSContext.logTrace(logger, "Entering substituteArgumentWithEquality for {} and index = ({})", andNode.toString(), index);
				
		CompilerTypeList equalityArgs = new CompilerTypeList(2);
		CompilerTypeBase arg = andNode.getArgument(index);
		CompilerVariable tempVariable = rewriter.generateUniqueVariable();
		// APS 1/19/2015 - make it the same datatype as the other variable
		tempVariable.setDataType(arg.getDataType());
		equalityArgs.add(tempVariable);
		equalityArgs.add(arg);

		PCGOrNode equality = new PCGOrNode(BuiltInPredicate.EQUALITY_PREDICATE_NAME, equalityArgs);
		equality.setBuiltInPredicateType(BuiltInPredicateType.BINARY);

		andNode.overwriteNthArgument(index, tempVariable);

		if (andNode.getBinding(index) == BindingType.BOUND) {
			equality.setBindingPattern(0, BindingType.BOUND);
			equality.setBindingPattern(1, BindingType.FREE);
			andNode.prependChild(equality);
		} else {
			equality.setBindingPattern(0, BindingType.FREE);
			equality.setBindingPattern(1, BindingType.BOUND);
			andNode.addChild(equality);
		}

		this.deALSContext.logTrace(logger, "Entering substituteArgumentWithEquality for {} and index = ({})", andNode.toString(), index);		
	}

	protected void substituteArgumentInAndNode(PCGAndNode andNode, CompilerTypeBase oldArgument, CompilerTypeBase newArgument) {
		this.deALSContext.logTrace(logger, "Entering substituteArgumentInAndNode for {}", andNode.toString());

		if (oldArgument.isVariable()) {
			CompilerVariable oldVariable = (CompilerVariable) oldArgument;

			Utilities.substituteVariable(andNode, oldVariable, newArgument);

			for (PCGOrNode orNode : andNode.getChildren()) {				
				this.deALSContext.logDebug(logger, "substituteArgumentInAndNode (before) : {}", orNode);
				
				Utilities.substituteVariable(orNode, oldVariable, newArgument);

				this.deALSContext.logDebug(logger, "substituteArgumentInAndNode (after) : {}", orNode);
			}
		} else {
			this.deALSContext.logError(logger, "variable expected for old argument in substituteArgumentInAndNode");
			throw new CompilerException("variable expected for old argument in substituteArgumentInAndNode");
		}

		this.deALSContext.logTrace(logger, "From: {}", oldArgument.toString());
		this.deALSContext.logTrace(logger, "To: {}", newArgument.toString());
		this.deALSContext.logTrace(logger, "Exiting substituteArgumentInAndNode for {}", andNode.toString());
	}
	
	protected void substituteArgumentInOrNode(PCGOrNode orNode, CompilerTypeBase oldArgument, CompilerTypeBase newArgument) {
		this.deALSContext.logTrace(logger, "Entering substituteArgumentInOrNode for {}", orNode);

		if (oldArgument.isVariable()) {
			CompilerVariable oldVariable = (CompilerVariable) oldArgument;

			Utilities.substituteVariable(orNode, oldVariable, newArgument);

			for (PCGOrNodeChild childNode : orNode.getChildren()) {
				if (childNode instanceof PCGAndNode) {
					this.deALSContext.logDebug(logger, "substituteArgumentInOrNode (before) : {}", childNode);
				
					Utilities.substituteVariable((PCGAndNode)childNode, oldVariable, newArgument);
					
					this.deALSContext.logDebug(logger, "substituteArgumentInOrNode (after) : {}", childNode);
				}
			}
		} else {
			this.deALSContext.logError(logger, "variable expected for old argument in substituteArgumentInOrNode");
			throw new CompilerException("variable expected for old argument in substituteArgumentInOrNode");
		}

		this.deALSContext.logTrace(logger, "From: {}", oldArgument.toString());
		this.deALSContext.logTrace(logger, "To: {}", newArgument.toString());
		this.deALSContext.logTrace(logger, "Exiting substituteArgumentInOrNode for {}", orNode);
	}
}
