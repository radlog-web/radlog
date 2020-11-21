package edu.ucla.cs.wis.bigdatalog.compiler.predicate;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class BuiltInPredicate extends Predicate {
	private static final long serialVersionUID = 1L;
	/*BEGIN OR NODES*/
	public static final String EQUALITY_PREDICATE_NAME = "=";
	public static final String INEQUALITY_PREDICATE_NAME = "~=";
	public static final String LESS_THAN_OR_EQUAL_PREDICATE_NAME = "<=";
	public static final String GREATER_THAN_OR_EQUAL_PREDICATE_NAME = ">=";
	public static final String LESS_THAN_PREDICATE_NAME = "<";
	public static final String GREATER_THAN_PREDICATE_NAME = ">";
	public static final String LIKE_PREDICATE_NAME = "*=";
	public static final String NOT_LIKE_PREDICATE_NAME = "~*=";
		
	public static final String TRUE_PREDICATE_NAME = "true";
	public static final String FALSE_PREDICATE_NAME = "false";
	public static final String APPEND_PREDICATE_NAME = "append";
	public static final String MEMBER_PREDICATE_NAME = "member";
	public static final String CARDINALITY_PREDICATE_NAME = "cardinality";
	public static final String GET_NTH_MEMBER_PREDICATE_NAME = "get_nth_member";
	public static final String FUNCTOR_PREDICATE_NAME = "functor";
	public static final String SORT_PREDICATE_NAME = "sort";
	public static final String SUBSET_PREDICATE_NAME = "subset";
	
	public static final String NEGATION_PREDICATE_NAME = "negation";
	
	public static final String EMPTY_PREDICATE_NAME = "empty";
	public static final String SINGLE_PREDICATE_NAME = "single";
	public static final String MULTI_PREDICATE_NAME = "multi";
	public static final String RETURN_PREDICATE_NAME = "return";
	public static final String IFTHENELSE_PREDICATE_NAME = "ifthenelse";
	public static final String IFTHEN_PREDICATE_NAME = "ifthen";
	public static final String SINGLE_MULTI_PREDICATE_NAME = "single_multi";

	public static final String IF_AND_NODE_PREDICATE_NAME = "if_and_node";
	public static final String THEN_AND_NODE_PREDICATE_NAME = "then_and_node";
	public static final String ELSE_AND_NODE_PREDICATE_NAME = "else_and_node";
	
	public static final String FS_MAX_PREDICATE_NAME = "fsmax";
	public static final String FS_MIN_PREDICATE_NAME = "fsmin";
	public static final String FS_COUNT_PREDICATE_NAME = "fscnt";
	public static final String FS_SUM_PREDICATE_NAME = "fssum";
	public static final String FS_IS_NEW_MAX_PREDICATE_NAME = "is_new_max"; // APS 5/28/2013 @DATALOGFS
	public static final String LIMIT_PREDICATE_NAME = "limit";
	
	public static final String GET_DATE_PREDICATE_NAME = "getdate";
	public static final String DATE_PART_PREDICATE_NAME = "datepart";
	public static final String DATE_ADD_PREDICATE_NAME = "dateadd";
	public static final String DATE_DIFF_PREDICATE_NAME = "datediff";
	public static final String SUB_STRING_PREDICATE_NAME = "substring";
	
	protected BuiltInPredicateType builtInPredicateType;
	
	public BuiltInPredicate(String predicateName, CompilerTypeList arguments, 
			CompilerType compilerType, BuiltInPredicateType builtInPredicateType) {
		super(predicateName, arguments, compilerType, PredicateType.BUILT_IN);
		
		this.isRecursive = false;	// APS added 6/6/2013 - if we have a clique, we adorn differently than with a nonrecursive builtin predicate
		this.builtInPredicateType = builtInPredicateType;
	}
	
	public BuiltInPredicate(String predicateName, 
			CompilerTypeList arguments, 
			BuiltInPredicateType builtInPredicateType) {
		this(predicateName, arguments, CompilerType.PREDICATE, builtInPredicateType); 
	}
	
	public BuiltInPredicateType getBuiltInPredicateType() { return this.builtInPredicateType; }

	public void setBuiltInPredicateType(BuiltInPredicateType built_in_predicate_type) {this.builtInPredicateType = built_in_predicate_type;}

	public boolean isBinary() {return (this.builtInPredicateType == BuiltInPredicateType.BINARY);}

	public boolean isAggregate() {return (this.builtInPredicateType == BuiltInPredicateType.AGGREGATE); }
	
	public boolean isFSAggregate() {return (this.builtInPredicateType == BuiltInPredicateType.AGGREGATE_FS); }

	public boolean isSingle() {return (this.builtInPredicateType == BuiltInPredicateType.SINGLE);}

	public boolean isMulti() {return (this.builtInPredicateType == BuiltInPredicateType.MULTI);}

	public boolean isTrue() {return (this.builtInPredicateType == BuiltInPredicateType.TRUE);}

	public boolean isFalse() {return (this.builtInPredicateType == BuiltInPredicateType.FALSE);}

	public boolean isChoice() {return (this.builtInPredicateType == BuiltInPredicateType.CHOICE);}

	public boolean isIfThenElse() {return (this.builtInPredicateType == BuiltInPredicateType.IFTHENELSE);}

	public boolean isIfThen() {return (this.builtInPredicateType == BuiltInPredicateType.IFTHEN);}

	public boolean isGeneric() {return (this.builtInPredicateType == BuiltInPredicateType.GENERIC);}

	/* HW adds the following 5 methods */
	public boolean isXYStage() {return (this.builtInPredicateType == BuiltInPredicateType.XY_STAGE);}
	
	public boolean isReadAggregate() {return (this.builtInPredicateType == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE);}
	
	public boolean isWriteAggregate() {return (this.builtInPredicateType == BuiltInPredicateType.WRITE_USER_DEFINED_AGGREGATE);}

	public boolean isReturn() {return (this.builtInPredicateType == BuiltInPredicateType.RETURN);}

	public boolean isSingleMultiBuiltInAggregate() {return (this.builtInPredicateType == BuiltInPredicateType.SINGLE_MULTI_USER_DEFINED_AGGREGATE);}

	// APS 3/21/2013 @DATALOGFS
	public boolean isReadAggregateFS() {return (this.builtInPredicateType == BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS);}
	
	public boolean isWriteAggregateFS() {return (this.builtInPredicateType == BuiltInPredicateType.WRITE_USER_DEFINED_AGGREGATE_FS);}

	public boolean isAggregatePredicate() { return (this.isReadAggregate() || this.isReadAggregateFS() 
			|| this.isWriteAggregate() || this.isWriteAggregateFS()); }
	
	public boolean isNonEqualityComparison() { return (this.predicateName.equals(BuiltInPredicate.GREATER_THAN_PREDICATE_NAME) 
			|| this.predicateName.equals(BuiltInPredicate.GREATER_THAN_OR_EQUAL_PREDICATE_NAME)
			|| this.predicateName.equals(BuiltInPredicate.LESS_THAN_OR_EQUAL_PREDICATE_NAME)
			|| this.predicateName.equals(BuiltInPredicate.LESS_THAN_PREDICATE_NAME)); }
	
	public boolean isSort() { return this.builtInPredicateType == BuiltInPredicateType.SORT; }
	
	public boolean isDatePredicate() { return this.builtInPredicateType == BuiltInPredicateType.DATE; }
	
	public boolean isTopK() { return this.builtInPredicateType == BuiltInPredicateType.TOPK; }
	
	public BuiltInPredicate copy() {
		BuiltInPredicate bip = new BuiltInPredicate(this.predicateName, this.arguments.copy(),   
				this.builtInPredicateType);
		bip.setArgumentTypeAdornment(this.getArgumentTypeAdornment());
		bip.setFSAggregateType(this.getFSAggregateType());
		bip.isRecursive = this.isRecursive;
		return bip;
	}

	public BuiltInPredicate copy(CompilerVariableList variableList) { 
		BuiltInPredicate bip = new BuiltInPredicate(this.predicateName, this.arguments.copy(variableList),   
				this.builtInPredicateType);
		bip.setArgumentTypeAdornment(this.getArgumentTypeAdornment());
		bip.setFSAggregateType(this.getFSAggregateType());
		bip.isRecursive = this.isRecursive;
		return bip;
	}
	
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof BuiltInPredicate))
			return false;
		
		BuiltInPredicate otherPredicate = (BuiltInPredicate)other;
		
		return (this.predicateName.equals(otherPredicate.getPredicateName()) 
				&& this.arity == otherPredicate.getArity() 
				&& this.builtInPredicateType == otherPredicate.getBuiltInPredicateType()
				&& this.arguments.equals(otherPredicate.getArguments()));
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		switch (this.predicateOperatorType)
		{
			case POSITIVE:
				break;
			case NEGATIVE:
				retval.append("~");
				break;
		}

		if (this.isBinary()) {
			retval.append(this.arguments.get(0).toString());
			retval.append(" " + this.predicateName + " ");
			retval.append(this.arguments.get(1).toString());
		} else if (this.isTrue()) {
			retval.append("true");
		} else if (this.isFalse()) {
			retval.append("false");
		} else {
			retval.append(this.predicateName + "(");
			if (this.arity > 0) {
				retval.append(this.arguments.get(0).toString());
				
				for (int i = 1; i < this.arguments.size(); i++) {
					retval.append(", ");
					retval.append(this.arguments.get(i).toString());
				}
			}
			retval.append(")");
		}
	
		return retval.toString();
	}
}
