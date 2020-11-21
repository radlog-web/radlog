package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.exception.ProgramGeneratorException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class Unifier {
	private static Logger logger = LoggerFactory.getLogger(Unifier.class.getName());

	public static boolean unifyTerms(DeALSContext deALSContext, NodeArguments toTerms, NodeArguments fromTerms) {
		deALSContext.logTrace(logger, "Entering unifyTerms");

		deALSContext.logDebug(logger, "toTerms = {}fromTerms = {}", toTerms, fromTerms);

		if (toTerms.size() == fromTerms.size()) {
			Argument toTerm, fromTerm;
			for (int i = 0; i < toTerms.size(); i++) {
				toTerm = toTerms.get(i);
				fromTerm = fromTerms.get(i);

				// toTerm should be a variable
				if (toTerm instanceof Variable) {
					// We do short circuiting here
					if ((fromTerm instanceof Variable)
							&& ((Variable) fromTerm).isBound()) {
						Argument value = ((Variable) fromTerm).deepDereference();
						((Variable) toTerm).setValue(value);
					} else {
						((Variable) toTerm).setValue(fromTerm);
					}
				} else {
					deALSContext.logTrace(logger, "Unexpected constant found after equality rewrite");
					throw new ProgramGeneratorException("Unexpected constant found after equality rewrite");
				}
			}
		}

		deALSContext.logDebug(logger, "toTerms = {}, fromTerms = {}", toTerms, fromTerms);
		deALSContext.logTrace(logger, "Exiting unifyTerms");

		return true;
	}

	public static boolean unifyValue(DeALSContext deALSContext, Argument toValue, Argument fromValue) {
		deALSContext.logTrace(logger, "Entering unifyValue for toValue = {}, fromValue = {}", toValue, fromValue);

		boolean status = false;

		if (toValue instanceof Variable) {
			Variable toVariable = (Variable) toValue;
			Argument newToValue;
			Argument newFromValue;

			if (toVariable.isBound())
				newToValue = toVariable.deepDereference();
			else
				newToValue = toValue;

			if (fromValue instanceof Variable) {
				Variable fromVariable = (Variable) fromValue;

				if (fromVariable.isBound())
					newFromValue = fromVariable.deepDereference();
				else
					newFromValue = fromValue;
			} else {
				newFromValue = fromValue;
			}

			status = Unifier.unifyValue(deALSContext, newToValue, newFromValue);
		} else if (toValue instanceof InputVariable) {
			if ((fromValue instanceof InputVariable)
					&& ((toValue == fromValue) || toValue.equals(fromValue)))
				status = true;
		} else if (toValue instanceof DbList) {
			if (fromValue instanceof DbList) {
				DbList toList = (DbList) toValue;
				DbList fromList = (DbList) fromValue;

				if (toList.isEmpty() && fromList.isEmpty())
					status = true;
				else if (!toList.isEmpty()
						&& !fromList.isEmpty()
						&& Unifier.unifyValue(deALSContext, toList.getHead(), fromList.getHead())
						&& Unifier.unifyValue(deALSContext, toList.getTail(), fromList.getTail()))
					status = true;
			}
		} else if (toValue instanceof InterpreterFunctor) {
			InterpreterFunctor toFunctor = (InterpreterFunctor) toValue;
			InterpreterFunctor fromFunctor = (InterpreterFunctor) fromValue;

			if (toFunctor.getFunctorName().equals(fromFunctor.getFunctorName())
					&& toFunctor.getArity() == fromFunctor.getArity()) {
				int i = 0;
				for (i = 0; i < toFunctor.getArity(); i++)
					if (!Unifier.unifyValue(deALSContext, toFunctor.getArgument(i), fromFunctor.getArgument(i)))
						break;
				
				status = (i >= toFunctor.getArity());					
			}
		} else if (toValue instanceof InterpreterList) {	
			InterpreterList toList = (InterpreterList) toValue;
			InterpreterList fromList = (InterpreterList) fromValue;

			if (toList.isEmpty() && fromList.isEmpty()) {
				status = true;
			} else if (!toList.isEmpty() && !fromList.isEmpty()
					&& Unifier.unifyValue(deALSContext, toList.getHead(), fromList.getHead())) {
				if (toList.getTail() == null && fromList.getTail() == null)
					status = true;
				else
					status = Unifier.unifyValue(deALSContext, toList.getTail(), fromList.getTail());
			}
		} else if (toValue.isConstant()) {
			status = toValue.equals(fromValue);
		}

		deALSContext.logTrace(logger, "Exiting unifyValue for toValue = {}, fromValue = {} with status = {}",
					toValue, fromValue, status);

		return status;
	}

	private static boolean unifyEqualityTerm(DeALSContext deALSContext, Argument leftTerm, Argument rightTerm) {
		deALSContext.logTrace(logger, "Entering unifyEqualityTerm for leftTerm = {}, rightTerm = {}", leftTerm, rightTerm);

		boolean status = true;
		Argument leftValue = leftTerm;
		Argument rightValue = rightTerm;

		if ((leftValue instanceof Variable) && ((Variable) leftValue).isBound())
			leftValue = ((Variable) leftValue).deepDereference();

		if ((rightValue instanceof Variable)
				&& ((Variable) rightValue).isBound())
			rightValue = ((Variable) rightValue).deepDereference();

		if (leftValue instanceof Variable) {
			if (rightValue instanceof Variable) {
				if (rightValue == rightTerm)
					((Variable) rightValue).setValue(leftValue);
				else
					((Variable) leftValue).setValue(rightValue);
			} else {
				((Variable) leftValue).setValue(rightValue);
			}
		} else {
			if (rightValue instanceof Variable)
				((Variable) rightValue).setValue(leftValue);
			else
				status = Unifier.unifyEqualityValue(deALSContext, leftValue, rightValue);
		}

		deALSContext.logTrace(logger, "Exiting unifyEqualityTerm for leftTerm = {}, rightTerm = {} with status = {}",
					leftTerm, rightTerm, status);

		return status;
	}

	private static boolean unifyEqualityValue(DeALSContext deALSContext, Argument leftValue, Argument rightValue) {
		deALSContext.logTrace(logger, "Entering unifyEqualityValue for leftValue = {}, rightValue = {}",
					leftValue, rightValue);

		boolean status = false;

		if ((leftValue instanceof InputVariable)
				|| (rightValue instanceof InputVariable)) {
			status = true;
		} else {
			if (leftValue.isConstant() && rightValue.isConstant()) {
				if (leftValue.equals(rightValue))
					status = true;
			} else {
				if ((leftValue instanceof InterpreterFunctor)
						&& (rightValue instanceof InterpreterFunctor)) {
					InterpreterFunctor leftFunctor = (InterpreterFunctor) leftValue;
					InterpreterFunctor rightFunctor = (InterpreterFunctor) rightValue;

					if (leftFunctor.getFunctorName().equals(rightFunctor.getFunctorName())
							&& leftFunctor.getArity() == rightFunctor.getArity()) {
						// status = this.unifyEqualityTerms(leftFunctor.getArguments(), rightFunctor.getArguments());
						status = true;
						for (int i = 0; i < leftFunctor.getArguments().size(); i++) {
							status = Unifier.unifyEqualityTerm(deALSContext, leftFunctor.getArguments().get(i), 
									rightFunctor.getArguments().get(i));
							if (!status)
								break;
						}
					}
				} else {
					if ((leftValue instanceof DbList) && (rightValue instanceof DbList)) {
						CompilerList leftList = (CompilerList) leftValue;
						CompilerList rightList = (CompilerList) rightValue;

						if (leftList.isEmpty() && rightList.isEmpty()) {
							status = true;
						} else {
							if (!leftList.isEmpty() && !rightList.isEmpty()
									&& Unifier.unifyEqualityTerm(deALSContext, (Argument) leftList.getHead(), (Argument) rightList.getHead())
											&& Unifier.unifyEqualityTerm(deALSContext, (Argument) leftList.getTail(), (Argument) rightList.getTail()))
								status = true;
						}
					}
				}
			}
		}

		deALSContext.logTrace(logger, "Entering unifyEqualityValue for leftValue = {}, rightValue = {} with status = {}",
					leftValue, rightValue, status);

		return status;
	}
}