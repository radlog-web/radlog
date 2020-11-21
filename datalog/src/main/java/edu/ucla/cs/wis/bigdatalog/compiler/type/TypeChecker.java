package edu.ucla.cs.wis.bigdatalog.compiler.type;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.ComparisonOperation;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.type.expression.CompilerArithmeticExpression;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.BindingType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// APS 5/22/2014 - still being implemented
public class TypeChecker {
	private static Logger logger = LoggerFactory.getLogger(TypeChecker.class.getName());
	
	public static void checkTypes(Module module) {
		for (DerivedPredicate dp : module.getDerivedPredicates())
			for (Rule rule : dp.getRules())
				TypeChecker.checkExpressions(rule);
	}

	private static void checkExpressions(Rule rule) {
		Predicate literal;
		for (int i = 0; i < rule.getBody().size(); i++) {
			literal = rule.getBody().get(i);
			if (literal.isBuiltIn() && ((BuiltInPredicate)literal).isBinary()) {
				if (!isSafe(literal))
					throw new CompilerException("Comparison/Assignment ("+literal.toString()+") has type mismatch.");
			}
		}
	}

	private static boolean isSafe(Predicate expression) {		
		ComparisonOperation operation = ComparisonOperation.getOperation(expression.getPredicateName());
		if (operation == ComparisonOperation.NONE) return true;
		
		//if (operation == ComparisonOperation.EQUALITY)
		//	return TypeChecker.isAssignmentSafe(expression);
		
		CompilerTypeBase left = expression.getArgument(0);
		CompilerTypeBase right = expression.getArgument(1);
				 
		DataType leftDataType = TypeInferrer.inferTermDataType(left);
		DataType rightDataType = TypeInferrer.inferTermDataType(right);

		//if (leftDataType != rightDataType) {
		if (!DataType.comparable(leftDataType, rightDataType)) {
			logger.error("Uncomparable types : " + leftDataType + " " + operation.toString() + " " + rightDataType + " for '" + expression + "'");
			return false;
		}
		
		/*if ((left instanceof CompilerVariable) && (right instanceof CompilerArithmeticExpression))
			if ((leftDataType != rightDataType) && DataType.isPromotable(leftDataType, rightDataType))
				expression.setArgument(0, CompilerTypeCast.create(left, rightDataType));*/

		/*if (to.isFunctor()) TypeChecker.checkExpression(to);
		if (from.isFunctor()) TypeChecker.checkExpression(from);*/
		return true;
	}
	
	public static void makeSafe(PCGOrNode pcgOrNode) {
		if (pcgOrNode.getBuiltInPredicateType() != BuiltInPredicateType.BINARY) return;
		
		BuiltInPredicate bip = (BuiltInPredicate)pcgOrNode.getPredicate();
		
		ComparisonOperation operation = ComparisonOperation.getOperation(bip.getPredicateName());
		if (operation == ComparisonOperation.NONE) return;
		
		CompilerTypeBase left = bip.getArgument(0);
		CompilerTypeBase right = bip.getArgument(1);

		DataType leftDataType = TypeInferrer.inferTermDataType(left);
		DataType rightDataType = TypeInferrer.inferTermDataType(right);

		//if (leftDataType != rightDataType) {
		if (!DataType.comparable(leftDataType, rightDataType)) {
			logger.error("Uncomparable types : " + leftDataType + " " + operation.toString() + " " + rightDataType + " for '" + bip + "'");
			return;
		}
		
		if ((left instanceof CompilerVariable) 
				&& (right instanceof CompilerArithmeticExpression) 
				&& (leftDataType != rightDataType) 
				&& DataType.isPromotable(leftDataType, rightDataType)) {
			if ((pcgOrNode.getBinding(0) == BindingType.FREE) && (pcgOrNode.getBinding(1) == BindingType.BOUND))
				((CompilerVariable)left).setDataType(rightDataType);
			else if (pcgOrNode.getBindingPattern().allBound())
				bip.setArgument(0, CompilerTypeCast.create(left, rightDataType));
		}
	}
	
	/*private static boolean isAssignmentSafe(Predicate expression) {
		ComparisonOperation operation = ComparisonOperation.getOperation(expression.getPredicateName());
			
		if (operation != ComparisonOperation.EQUALITY) return true;
		
		CompilerTypeBase to = expression.getArgument(0);
		CompilerTypeBase from = expression.getArgument(1);
		
		DataType toDataType = TypeInferrer.inferTermDataType(to);
		DataType fromDataType = TypeInferrer.inferTermDataType(from);
		
		if (toDataType == fromDataType) return true;
		
		if (!DataType.assignable(fromDataType, toDataType)) {
			if (DeALSContext.isErrorEnabled())
				logger.error("Uncomparable types : " + fromDataType + " => " + toDataType + " for '" + expression + "'");
			return false;
		}		

		return false;
	}
	
	private static boolean isArithmeticSafe(CompilerFunctor expression) {
		ArithmeticOperation operation = ArithmeticOperation.getOperation(expression.getFunctorName());
		if (operation == ArithmeticOperation.NONE) return true;
		
		CompilerTypeBase from = expression.getArgument(0);
		CompilerTypeBase to = expression.getArgument(1);
		
		DataType toDataType = TypeInferrer.inferTermDataType(to);
		DataType fromDataType = TypeInferrer.inferTermDataType(from);

		if (!DataType.comparable(fromDataType, toDataType)) {
			if (DeALSContext.isErrorEnabled())
				logger.error("Uncomparable types : " + fromDataType + " => " + toDataType + " for '" + expression + "'");
			return false;
		}

		if (to.isFunctor())
			TypeChecker.checkExpression(to);

		if (from.isFunctor())
			TypeChecker.checkExpression(from);
		
		return true;
	}*/
}