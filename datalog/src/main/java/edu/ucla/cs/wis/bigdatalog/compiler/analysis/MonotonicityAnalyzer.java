package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.MonotonicRuleType;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.ArgumentType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerByte;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerDouble;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFloat;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerInt;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerLong;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerShort;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class MonotonicityAnalyzer {
	private static Logger logger = LoggerFactory.getLogger(MonotonicityAnalyzer.class.getName());

	private DeALSContext deALSContext;
	
	public MonotonicityAnalyzer(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;		
	}
	
	public MonotonicRuleType analyzeFSRule(Rule rule) {
		// this method analyzes an fs rule and makes sure it if written such that fs variants hold
		// Analysis done:
		// 1) determining what type a rule is: 
		//		a) Normal to Normal (N2N)
		//		b) Normal to FS (N2FS)
		// 		c) FS to FS (FS2FS)
		//		d) FS to Normal (FS2N)
		// 2) checking that FS to FS rules only perform monotonic operations on FS variables
		
		boolean headIsFS = false;
		for (ArgumentType at : rule.getHead().getArgumentTypeAdornment()) {
			if (at == ArgumentType.FSAGGREGATE) {
				headIsFS = true;
				break;
			}
		}
		
		boolean bodyIsFS = false;
		for (Predicate goal : rule.getBody()) {
			for (ArgumentType at : goal.getArgumentTypeAdornment()) {
				if (at == ArgumentType.FSAGGREGATE) {
					bodyIsFS = true;
					break;
				}
			}
			
			if (bodyIsFS)
				break;
		}
		
		if (headIsFS && bodyIsFS)
			verifyMonotonicity(rule);
		
		if (headIsFS) {
			if (bodyIsFS)
				return MonotonicRuleType.FS2FS;
			
			return MonotonicRuleType.N2FS;
		}
		
		if (bodyIsFS)
			return MonotonicRuleType.FS2N;

		return MonotonicRuleType.N2N;
	}
	
	// for FS2FS rules
	private void verifyMonotonicity(Rule rule) {
		if (rule.allFunctionsMonotonic())
			return;
		
		for (Predicate goal : rule.getBody())
			if (!isMonotonic(goal))
				throw new CompilerException("Rule " + rule.getHead().toString() + " is an FS2FS rule and has antimonotonic operations.  This is not permitted!");
				
		/*// gather fs variables
		VariableList headVariables = new VariableList();
		VariableList fsHeadVariables = new VariableList();
		VariableList fsBodyVariables = new VariableList();
		Utilities.getVariables(rule.getHead().getArguments(), headVariables);
		
		List<ArgumentType> argumentAdornment = rule.getHead().getArgumentTypeAdornment();
		
		for (int i = 0; i < argumentAdornment.size(); i++) {
			if (argumentAdornment.get(i) == ArgumentType.FSAGGREGATE)
				fsHeadVariables.add(headVariables.get(i));			
		}
		
		for (int i = 0; i < rule.getBody().size(); i++) {
			Predicate goal = rule.getLiteral(i);
			for (int j = 0; j < argumentAdornment.size(); j++) {
				// collect each body fs variable that contributes to head
				if (argumentAdornment.get(j) == ArgumentType.FSAGGREGATE) {
					Variable fsVariable = (Variable)goal.getArgument(j);
				
					if (contributesToHead(fsVariable, fsHeadVariables, rule.getBody(), i+1))
						fsBodyVariables.add(fsVariable);
				}
			}
		}*/
	}	
	
	private static boolean contributesToHead(CompilerVariable variable, CompilerVariableList fsHeadVariables, List<Predicate> body, int nextGoalIndex) {
		CompilerVariableList variables = new CompilerVariableList();
	
		if (fsHeadVariables.contains(variable))
			return true;
	
		// search for subsequent goals to see if variable is head variable
		for (int i = nextGoalIndex; i < body.size(); i++) {
			variables.clear();		
			Utilities.getVariables(body.get(i), variables);
			
			if (variables.contains(variable)) {
				for (int j = 0; j < variables.size(); j++) {
					if (fsHeadVariables.contains(variables.get(j)))
						return true;
				}
				
				if (contributesToHead(variable, fsHeadVariables, body, nextGoalIndex + 1))
					return true;
			}
		}
		
		return false;
	}
		
	public boolean isMonotonic(Predicate goal) {
		if (goal.isNegative())
			return false;
		
		if (goal.isBuiltIn()) {
			BuiltInPredicate bip = (BuiltInPredicate)goal;
			if (bip.getBuiltInPredicateType() == BuiltInPredicateType.BINARY) {
				ComparisonOperation co = ComparisonOperation.getOperation(bip.getPredicateName());
				if (co != ComparisonOperation.NONE) {
					CompilerTypeBase[] args = new CompilerTypeBase[bip.getArity()];
					for (int i = 0; i < bip.getArity(); i++)
						args[i] = bip.getArgument(i);
					
					CompilerFunctor func = CompilerFunctor.createFunctor(bip.getPredicateName(), args);
					if (!isMonotonic(func))
						return false;
				} else {
					CompilerArithmeticExpression expression;
					if (bip.getArity() == 2)
						expression = CompilerArithmeticExpression.createExpression(bip.getPredicateName(), bip.getArgument(0), bip.getArgument(1));
					else
						expression = CompilerArithmeticExpression.createExpression(bip.getPredicateName(), bip.getArgument(0));
										
					if (!isMonotonic(expression))
						return false;
				}
			}
		}
		return true;
	}
	
	public boolean isMonotonic(CompilerFunctor functor) {
		boolean isMonotonic = false;
		CompilerFunctor func = functor.copy();
		
		expandNegativeNumbers(func);
		
		CompilerTypeBase arg;
		boolean anyArgIsAntimonotonic = false;
		for (int i = 0; i < func.getArity(); i++) {
			 arg = func.getArgument(i);
			 if (arg.isFunctor()) {
				 anyArgIsAntimonotonic = !isMonotonic(((CompilerFunctor)arg));
				 if (anyArgIsAntimonotonic)
					 break;
			 } else if (arg.isExpression()) {
				 anyArgIsAntimonotonic = !isMonotonic(((CompilerArithmeticExpression)arg));
				 if (anyArgIsAntimonotonic)
					 break;
			 }
		}
		
		boolean operationIsMonotonic = ComparisonOperation.getOperation(func.getFunctorName()).isMonotonic();
		
		if (anyArgIsAntimonotonic) 
			isMonotonic = !operationIsMonotonic;
		else
			isMonotonic = operationIsMonotonic;
		
		if (this.deALSContext.isInfoEnabled()) {
			StringBuilder retval = new StringBuilder();
			retval.append(func.toString());
			retval.append(" is ");
			if (isMonotonic)
				retval.append("monotonic");
			else
				retval.append("antimonotonic");
			this.deALSContext.logInfo(logger, "{}", retval.toString());
		}
		return isMonotonic;
	}
		
	private static void expandNegativeNumbers(CompilerFunctor func) {
		CompilerTypeBase arg;
		for (int i = 0; i < func.getArity(); i++) {
			arg = func.getArgument(i);
			if (arg.isFunctor()) {
				expandNegativeNumbers((CompilerFunctor)arg);
			} else {
				if (arg.isNumeric()) {
					switch (arg.getDataType()) {
						case FLOAT:
							if (((CompilerFloat)arg).getValue() < 0) {
								CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
										new CompilerFloat(Math.abs(((CompilerFloat)arg).getValue())));
								func.getArguments().set(i, expr);
							}
							break;
						case DOUBLE:
							if (((CompilerDouble)arg).getValue() < 0) {
								CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
										new CompilerDouble(Math.abs(((CompilerDouble)arg).getValue())));
								func.getArguments().set(i, expr);
							}
							break;
						case INT:
							if (((CompilerInt)arg).getValue() < 0) {
								CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
										new CompilerInt(Math.abs(((CompilerInt)arg).getValue())));
								func.getArguments().set(i, expr);
							}
							break;
						case SHORT:
							if (((CompilerShort)arg).getValue() < 0) {
								CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
										new CompilerShort((short) Math.abs(((CompilerShort)arg).getValue())));
								func.getArguments().set(i, expr);
							}							break;
						case BYTE:
							if (((CompilerByte)arg).getValue() < 0) {
								CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
										new CompilerByte((byte) Math.abs(((CompilerByte)arg).getValue())));
								func.getArguments().set(i, expr);
							}
							break;
						case LONG:
							if (((CompilerLong)arg).getValue() < 0) {
								CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
										new CompilerLong(Math.abs(((CompilerLong)arg).getValue())));
								func.getArguments().set(i, expr);
							}
							break;						
					}
				}
			}
		}
	}
	
	public boolean isMonotonic(CompilerArithmeticExpression expression) {
		boolean isMonotonic = false;
		CompilerArithmeticExpression expr = expression.copy();
		
		expandNegativeNumbers(expr);
		
		boolean anyArgIsAntimonotonic = false;
		
		if (expr.getArgument1().isExpression())
			 anyArgIsAntimonotonic = !isMonotonic((CompilerArithmeticExpression) expr.getArgument1());
			
		if (!anyArgIsAntimonotonic && (expr.getArgument2() != null))
			if (expr.getArgument2().isExpression())
				anyArgIsAntimonotonic = !isMonotonic((CompilerArithmeticExpression) expr.getArgument2());

		boolean operationIsMonotonic = expr.getOperation().isMonotonic();
				
		if (anyArgIsAntimonotonic) 
			isMonotonic = !operationIsMonotonic;
		else
			isMonotonic = operationIsMonotonic;
		
		if (this.deALSContext.isInfoEnabled()) {
			StringBuilder retval = new StringBuilder();
			retval.append(expression.toString());
			retval.append(" is ");
			if (isMonotonic)
				retval.append("monotonic");
			else
				retval.append("antimonotonic");
			this.deALSContext.logInfo(logger, "{}", retval.toString());
		}
		return isMonotonic;
	}
	
	private static void expandNegativeNumbers(CompilerArithmeticExpression expression) {
		if (expression.getArgument1().isExpression())
			expandNegativeNumbers((CompilerArithmeticExpression) expression.getArgument1());
		
		CompilerTypeBase arg = expression.getArgument1();
		if (arg.isNumeric()) {
			switch (arg.getDataType()) {
				case DOUBLE: 
					if (((CompilerDouble)arg).getValue() < 0) {
						CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
								new CompilerDouble(Math.abs(((CompilerDouble)arg).getValue())));
						expression.setArgument1(expr);
					}
					break;
				case FLOAT:
					if (((CompilerFloat)arg).getValue() < 0) {
						CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
								new CompilerFloat(Math.abs(((CompilerFloat)arg).getValue())));
						expression.setArgument1(expr);
					}
					break;
				case INT:
					if (((CompilerInt)arg).getValue() < 0) {
						CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
								new CompilerInt(Math.abs(((CompilerInt)arg).getValue())));
						expression.setArgument1(expr);
					}
					break;
				case SHORT:
					if (((CompilerShort)arg).getValue() < 0) {
						CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
								new CompilerShort((short) Math.abs(((CompilerShort)arg).getValue())));
						expression.setArgument1(expr);
					}
					break;
				case BYTE:
					if (((CompilerByte)arg).getValue() < 0) {
						CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
								new CompilerByte((byte) Math.abs(((CompilerByte)arg).getValue())));
						expression.setArgument1(expr);
					}
					break;
				case LONG:
					if (((CompilerLong)arg).getValue() < 0) {
						CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
								new CompilerLong(Math.abs(((CompilerLong)arg).getValue())));
						expression.setArgument1(expr);
					}
					break;
			}
		}
		
		if (expression.getArgument2() != null) {
			if (expression.getArgument2().isExpression())
				expandNegativeNumbers((CompilerArithmeticExpression) expression.getArgument2());
			
			arg = expression.getArgument2();
			if (arg.isNumeric()) {
				switch (arg.getDataType()) {
					case FLOAT:
						if (((CompilerFloat)arg).getValue() < 0) {
							CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
									new CompilerFloat(Math.abs(((CompilerFloat)arg).getValue())));
							expression.setArgument2(expr);
						}
						break;
					case INT:
						if (((CompilerInt)arg).getValue() < 0) {
							CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
									new CompilerInt(Math.abs(((CompilerInt)arg).getValue())));
							expression.setArgument2(expr);
						}
						break;
					case LONG:
						if (((CompilerLong)arg).getValue() < 0) {
							CompilerArithmeticExpression expr = new CompilerArithmeticExpression("-", new CompilerInt(0), 
									new CompilerLong(Math.abs(((CompilerLong)arg).getValue())));
							expression.setArgument2(expr);
						}
						break;
				}
			}
		}
	}
}
