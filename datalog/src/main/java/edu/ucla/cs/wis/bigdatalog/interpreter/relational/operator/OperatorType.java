package edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator;

public enum OperatorType {
	JOIN,
	UNION,
	FILTER,
	PROJECT,
	AGGREGATE,
	BASE_RELATION,
	RECURSIVE_RELATION,
	SORT,
	LIMIT,
	NEGATION,
	RECURSION,
	RECURSIVE_CLIQUE,
	MUTUAL_RECURSIVE_CLIQUE,
	ASSIGNMENT,
	COMPARISON,
	TRUE,
	FALSE,
	TUPLE, // for magic exit rule assignments
	AGGREGATE_FS;
	
	public static boolean isProjectionOperatorType(OperatorType operatorType) {
		switch (operatorType) {
			case AGGREGATE:
			case PROJECT:
				return true;
		}
		return false;
	}
		
	public static boolean enforcesSetSemantics(OperatorType operatorType) {
		switch (operatorType) {
			case AGGREGATE:
			//case DISTINCT:
			case UNION:
			case RECURSIVE_CLIQUE:
			case MUTUAL_RECURSIVE_CLIQUE:
			case RECURSION:
			case BASE_RELATION:
			case RECURSIVE_RELATION:
				return true;
		}
		return false;
	}
	
	public boolean isAggregate() {
		return (this == AGGREGATE || this == AGGREGATE_FS);
	}
	
	public boolean isRecursive() {
		return (this == RECURSION || this == RECURSIVE_CLIQUE || this == MUTUAL_RECURSIVE_CLIQUE || this == RECURSIVE_RELATION);
	}
	
	public boolean isLeaf() {
		return (this == BASE_RELATION || this.isRecursive());
	}
}