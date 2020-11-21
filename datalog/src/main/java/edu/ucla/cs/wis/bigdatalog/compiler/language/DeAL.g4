grammar DeAL;

options {
  language = Java;
  //tokenVocab = DeAL;
  //backtrack = true;
 // memoize = true;
}

@parser::header {
	import java.util.*;
	import org.slf4j.LoggerFactory;
	import org.slf4j.Logger;
	/*import org.antlr.runtime.BitSet;
	import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
	import edu.ucla.cs.wis.bigdatalog.common.Pair;
	import edu.ucla.cs.wis.bigdatalog.compiler.*;
	import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.*;*/
	import edu.ucla.cs.wis.bigdatalog.compiler.predicate.*;
	/*import edu.ucla.cs.wis.bigdatalog.compiler.type.*;	
	import edu.ucla.cs.wis.bigdatalog.compiler.variable.*;
	import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
	import edu.ucla.cs.wis.bigdatalog.type.*;*/
}

@parser::members {
  private static Logger logger = LoggerFactory.getLogger(ParserManager.class.getName());

	private ParserManager parserManager;
		
	public void setParserManager(ParserManager parserManager) {
		this.parserManager = parserManager;
	}
	
	public ParserManager getParserManager() { return this.parserManager; }
	
	private static Map<String, Integer> builtInPredicates = new HashMap<String, Integer>() {{ 
		put("append", 3);
		put("sort", 2);
		put("member", 2);
		put("cardinality", 2);
		put("nth_member", 3);
		put("functor", 3);	
		put("getdate", 1);
		put("datepart", 3);
		put("dateadd", 4);
		put("datediff", 4);
		put("substring", 4);
	}};
	
	public static boolean isBuiltInPredicate(String predicateName, int arity) { 
		boolean status = false;

		predicateName = predicateName.toLowerCase();
		if (builtInPredicates.containsKey(predicateName))
			if (builtInPredicates.get(predicateName).equals(arity))
				status = true;

		return status;
	}
}

parseQueryForm : queryForm ('.')? EOF;	

parseQuery : predicate '.' EOF;

parseRule :	predicateRule EOF;

parseGroundTerm : groundTerm '.' EOF;

parseGroundPredicate : fact EOF;
	
parseLoadDatabaseObjects : databaseObjectDeclarations EOF;

parseLoadDatabaseFacts : fact* EOF;	

databaseObjectDeclarations : module=moduleDeclaration (tail=databaseObjectDeclarations)? | databaseDeclaration+;

databaseDeclaration : dbbps=databaseBasePredicates | bpkis=basePredicateKeyOrIndex | ex=export | f=fact | pr=predicateRule;
	
moduleDeclaration : BEGIN MODULE anyString '.' databaseDeclaration+ END MODULE '.';

databaseBasePredicates : DATABASE '(' '{' basePredicate (',' basePredicate)* '}' ')' '.';

fact : IDENTIFIER '(' groundArguments ')' '.';

groundArguments: groundTerm (',' groundTerm)*;

groundTerm : (fn=IDENTIFIER)? l='(' gafunc=groundArguments r=')'
	| l='[' galist=groundListTerms r=']' | dv=signedDecimal | iv=signedInteger | s=anyString;
	
groundListTerms : groundTerm (groundListTerms2)?;

groundListTerms2 : '|' groundTerm | '|' '[' groundListTerms? ']' | ',' groundListTerms;

basePredicate : pn=IDENTIFIER '(' basePredicateStructuralAttribute (',' basePredicateStructuralAttribute)* ')';

basePredicateStructuralAttribute : (n=name ':')? dt=DATA_TYPE
	| (n=name)? l='(' basePredicateStructuralAttribute (',' basePredicateStructuralAttribute)* r=')'
	| (n=name ':')? l2='[' basePredicateStructuralAttribute (',' basePredicateStructuralAttribute)* r2=']';

basePredicateKeyOrIndex : (KEY | INDEX) '(' pn=IDENTIFIER ',' '[' INTEGER (',' INTEGER)* ']' ')' '.';

//basePredicateIndex : INDEX '(' pn=IDENTIFIER ',' '[' INTEGER (',' INTEGER)* ']' ')' '.';

export : EXPORT queryForm '.';

predicateRule : head=ruleHead ('<-' body=ruleBody)? '.' annotations?;

ruleHead : action=udaPredicates '(' an=IDENTIFIER ',' term (',' term)*')' | hp=headPredicate;
	
ruleBody : literal (',' literal)*;

literal returns [Predicate retval] : action=TRUE | action=FALSE
	| action=IF '(' b1=ruleBody THEN b2=ruleBody (ELSE b3=ruleBody)? ')'	
	| action=CHOICE '(' ca1=choiceArgument ',' ca2=choiceArgument ')'
	| action=LIMIT '(' (variable | INTEGER) ')'
	| action=SORT '(' '(' variable ',' SORT_ORDER ')' (',' '(' variable ',' SORT_ORDER ')')* ')'
	| t1=term o=binaryOperator t2=term
	| (neg='~')? p=predicate;

predicate : IDENTIFIER ('(' term (',' term)* ')')? | IDENTIFIER '(' ')';

binaryOperator : '=' | '~=' | '>' | '<' | '>=' | '<=' | '*=' | '~*=';

choiceArgument : '(' (variable (',' variable)*)? ')';

headPredicate : pn=IDENTIFIER ('(' headTerm (',' headTerm)* ')')?;

headTerm : ae=arithmeticExpression | hat=headAggregateTerm | hnat=nonArithmeticTerm | nil=NIL;

headAggregateTerm : aggr=IDENTIFIER '<' hs=term '>'
	| fsagg=(AGGREGATE_FSMAX | AGGREGATE_FSMIN) '<' vt=variable '>' 
	| fsagg=(AGGREGATE_FSCNT | AGGREGATE_FSSUM) '<' hfs=headFscntSubterms '>'
	;  

headFscntSubterms : '(' vt1=variable ',' '(' vt2=variable ',' vts=variables ')' ')'
  	| '(' vt1=variable ',' at=arithmeticTerm ')'
  	| '(' vt1=variable ',' '(' vt2=variable ')' ')'
  	| '(' '(' vt1=variable ',' at=arithmeticTerm ')' ')'
  	| at=arithmeticTerm
  	| '(' at=arithmeticTerm ')';

term : arithmeticExpression | nonArithmeticTerm;

basicExpression : at=arithmeticTerm | '(' ae=arithmeticExpression ')' ;

unaryArithmeticExpression : be=basicExpression | l=LOG be=basicExpression | l=EXP be=basicExpression | l=STEP be=basicExpression;

multiplicativeArithmeticExpression : uae1=unaryArithmeticExpression (p=('*' | '/' | DIV | MOD | OPC) uae2=unaryArithmeticExpression)*
	| s='-' uae=unaryArithmeticExpression;

arithmeticExpression : mae1=multiplicativeArithmeticExpression (p=('+' | '-') mae2=multiplicativeArithmeticExpression)*;

arithmeticTerm : v=variable | dv=signedDecimal | iv=signedInteger;

nonArithmeticTerm : s=anyString | ft=functorTerm | lt=listTerm;

functorTerm : (n=IDENTIFIER)? '(' term (',' term)* ')';

listTerm : '[' listTermArguments? ']';

listTermArguments : term ('|' variable | '|' '[' listTermArguments? ']' | ',' listTermArguments)?;

queryForm : IDENTIFIER ('(' queryFormArguments? ')')?;
	
queryFormArguments : queryFormTerm (',' queryFormTerm)*;

queryFormTerm : (fn=IDENTIFIER)? '(' qfa=queryFormArguments ')' | '[' (qfaList=queryFormArguments)? ']'
	| vt=variable | ivt=INPUT_VARIABLE | dv=signedDecimal | iv=signedInteger | s=anyString;
		    
name : VARIABLENAME | IDENTIFIER | keyword;
anyString : STRING | name;
annotations : '@' anyString;
variables 	: variable (',' variable)*;
variable 	: VARIABLENAME | '_';

udaPredicates : EMPTY | SINGLE | MULTI | RETURN;
keyword : EMPTY | SINGLE | MULTI | RETURN | AGGREGATE_FSMAX | AGGREGATE_FSMIN | AGGREGATE_FSCNT | AGGREGATE_FSSUM | DATA_TYPE 
	| SORT_ORDER  | MOD | OPC | LOG | EXP | STEP | MODULE | DATABASE | EXPORT | INDEX | KEY | IF | THEN | ELSE | TRUE | FALSE | BEGIN | END | CHOICE;

signedInteger 	: '-'? INTEGER;
signedDecimal 	: '-'? DECIMAL;

// lexer
LINE_COMMENT : '%' ~[\r\n]* '\r'? '\n'-> skip;
MULTILINE_COMMENT : '/*'.*? ('*/' | EOF) -> skip;
NEWLINE			: '\r'?'\n' -> skip;
WS				: [ \t\n\r\f]+ -> skip;

STRING : '"' ('"' '"' | ~('"'/*|'\r'|'\n'*/))* '"' {setText(String.valueOf(getText().substring(1, getText().length()-1)));}
	|	'\'' ('\\' ('\\'|'\'') | ~('\\'|'\''/*|'\r'|'\n'*/))* '\'' {setText(String.valueOf(getText().substring(1, getText().length()-1)));};

DATA_TYPE : 'integer' | 'long' | 'float' | 'string' | 'any' | 'complex' | 'list' | 'datetime' | 'double' | 'short' | 'byte';
SORT_ORDER : ASC | DESC;

MODULE		: M O D U L E;
DATABASE	: D A T A B A S E;
EXPORT		: E X P O R T;
INDEX		: I N D E X;
KEY			: K E Y;
IF			: I F;
THEN		: T H E N;
ELSE		: E L S E;
TRUE		: T R U E;
FALSE		: F A L S E;
NIL			: N I L;
BEGIN 		: B E G I N;
END			: E N D;
CHOICE		: C H O I C E;
MULTI		: M U L T I;
SINGLE		: S I N G L E;
EMPTY		: E M P T Y;
RETURN		: R E T U R N;
LIMIT		: L I M I T;
SORT		: S O R T;
DESC		: D E S C;
ASC			: A S C;
MOD			: M O D;
OPC			: O P C;
LOG			: L O G;
EXP			: E X P;
STEP		: S T E P;
DIV			: D I V;

AGGREGATE_FSMAX 			: ((F S) | M) M A X;
AGGREGATE_FSMIN 			: ((F S) | M) M I N;
AGGREGATE_FSCNT 			: (F S C N T) | (M C O U N T);
AGGREGATE_FSSUM 			: ((F S) | M) S U M;

IDENTIFIER : LOWERCASELETTER (LETTER | DIGIT | '_' | '-')*;
VARIABLENAME : UPPERCASELETTER (LETTER | DIGIT | '_')*;
INPUT_VARIABLE	: '$'[a-zA-Z][a-zA-Z0-9]*('_'[a-zA-Z0-9]+)*;

INTEGER : '0'|[1-9]DIGIT*;
DECIMAL	: (DIGIT+)?('.'DIGIT+);

fragment DIGIT : [0-9];
fragment LETTER : UPPERCASELETTER | LOWERCASELETTER;

fragment UPPERCASELETTER : [A-Z];
fragment LOWERCASELETTER : [a-z];

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');