package edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.Aggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;

public class Unifier {

	public static boolean unifyTerms(CompilerTypeList toTerms, CompilerTypeList fromTerms, PCGAndNode andNode) { 
		for (int i = 0; i < toTerms.size(); i++) {
			if (!Unifier.unifyTerm(toTerms.get(i), fromTerms.get(i), andNode))
				return false;			
		}
		return true;
	}

	// propagate unified variables with available constant values from andNode to its children
	private static boolean unifyTerm(CompilerTypeBase to, CompilerTypeBase from, PCGAndNode andNode) {
		CompilerTypeBase value = null;
		boolean status = true;
		CompilerVariable variable;

		if (to.isInputVariable()) {
			// this is only used for unification between query forms
			if (!from.isConstant())
				throw new CompilerException("Can only unify InputVariable with constant");
			
			((CompilerInputVariable)to).setValue(from);
		} else if (to.isVariable()) {
			variable = (CompilerVariable)to;

			if ((from.isVariable()) && (((CompilerVariable)from).isBound())) {
				CompilerTypeBase derefValue = ((CompilerVariable)from).deepDereference();
				if (derefValue instanceof CompilerVariable)
					value = derefValue;
				else if (derefValue instanceof CompilerInputVariable)
					value = derefValue;
			} else {
				value = from;
			}

			// We should not substitute inputvariables because, the substituted inputvariables will
			// be set with the values at query time.
			if (!(value instanceof CompilerInputVariable) && value.isGround()) {
				Utilities.substituteVariable(andNode, variable, value);
				for (PCGOrNode node : andNode.getChildren())
					Utilities.substituteVariable(node, variable, value);
			}
		} else if (from.isVariable()) {
			// to is a constant
			if (((CompilerVariable)from).isBound()) {
				CompilerTypeBase defValue = ((CompilerVariable)from).deepDereference();
				if (defValue instanceof CompilerVariable)
					value = defValue;
				else if (defValue instanceof CompilerInputVariable)
					value = defValue;
				if (!(defValue instanceof CompilerVariable) && !(defValue instanceof CompilerInputVariable)) {
					status = Unifier.unifyValue(to, value, andNode);
				}
			}
			// else equality rewrite is needed
		} else if (to.isFunctor() && from.isAnyAggregate()) {
			status = unifyTerm(to, ((Aggregate)from).getAggregateTerm(), andNode);
		} else if (!from.isInputVariable()) {
			status = Unifier.unifyValue(to, from, andNode);
		}

		return status;
	}

	private static boolean unifyValue(CompilerTypeBase to, CompilerTypeBase from, PCGAndNode andNode) {
		boolean status = false;

		if (to.isConstant() && from.isConstant()) {
			status = to.equals(from);
		} else if ((to.isFunctor()) && (from.isFunctor())) {
			CompilerFunctor toFunc = (CompilerFunctor)to;
			CompilerFunctor fromFunc = (CompilerFunctor)from;

			if (toFunc.getFunctorName().equals(fromFunc.getFunctorName()) 
					&& (toFunc.getArity() == fromFunc.getArity()))
				status = Unifier.unifyTerms(toFunc.getArguments(), fromFunc.getArguments(), andNode);
		} else if (to.isList() && from.isList()) {
			CompilerList toList = (CompilerList)to;
			CompilerList fromList = (CompilerList)from;		

			if (toList.isEmpty() && fromList.isEmpty()) {
				status = true;
			} else if (!toList.isEmpty() && !fromList.isEmpty()) {
				if (Unifier.unifyTerm(toList.getHead(), fromList.getHead(), andNode)) {
					if ((toList.getTail() == null || toList.getTail().isEmpty())
							&& (fromList.getTail() == null || fromList.getTail().isEmpty()))
						status = true;
					else
						status = (Unifier.unifyTerm(toList.getTail(), fromList.getTail(), andNode));
				}	
			}
		}

		// incompatible type are not unifiable
		return status;
	}
}
