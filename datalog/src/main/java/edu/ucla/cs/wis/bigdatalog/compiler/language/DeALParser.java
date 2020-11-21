// Generated from DeAL.g4 by ANTLR 4.5.3
package edu.ucla.cs.wis.bigdatalog.compiler.language;

	import java.util.*;

    import org.antlr.v4.runtime.misc.Predicate;
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

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class DeALParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, LINE_COMMENT=27, MULTILINE_COMMENT=28, NEWLINE=29, 
		WS=30, STRING=31, DATA_TYPE=32, SORT_ORDER=33, MODULE=34, DATABASE=35, 
		EXPORT=36, INDEX=37, KEY=38, IF=39, THEN=40, ELSE=41, TRUE=42, FALSE=43, 
		NIL=44, BEGIN=45, END=46, CHOICE=47, MULTI=48, SINGLE=49, EMPTY=50, RETURN=51, 
		LIMIT=52, SORT=53, DESC=54, ASC=55, MOD=56, OPC=57, LOG=58, EXP=59, STEP=60, 
		DIV=61, AGGREGATE_FSMAX=62, AGGREGATE_FSMIN=63, AGGREGATE_FSCNT=64, AGGREGATE_FSSUM=65, 
		IDENTIFIER=66, VARIABLENAME=67, INPUT_VARIABLE=68, INTEGER=69, DECIMAL=70;
	public static final int
		RULE_parseQueryForm = 0, RULE_parseQuery = 1, RULE_parseRule = 2, RULE_parseGroundTerm = 3, 
		RULE_parseGroundPredicate = 4, RULE_parseLoadDatabaseObjects = 5, RULE_parseLoadDatabaseFacts = 6, 
		RULE_databaseObjectDeclarations = 7, RULE_databaseDeclaration = 8, RULE_moduleDeclaration = 9, 
		RULE_databaseBasePredicates = 10, RULE_fact = 11, RULE_groundArguments = 12, 
		RULE_groundTerm = 13, RULE_groundListTerms = 14, RULE_groundListTerms2 = 15, 
		RULE_basePredicate = 16, RULE_basePredicateStructuralAttribute = 17, RULE_basePredicateKeyOrIndex = 18, 
		RULE_export = 19, RULE_predicateRule = 20, RULE_ruleHead = 21, RULE_ruleBody = 22, 
		RULE_literal = 23, RULE_predicate = 24, RULE_binaryOperator = 25, RULE_choiceArgument = 26, 
		RULE_headPredicate = 27, RULE_headTerm = 28, RULE_headAggregateTerm = 29, 
		RULE_headFscntSubterms = 30, RULE_term = 31, RULE_basicExpression = 32, 
		RULE_unaryArithmeticExpression = 33, RULE_multiplicativeArithmeticExpression = 34, 
		RULE_arithmeticExpression = 35, RULE_arithmeticTerm = 36, RULE_nonArithmeticTerm = 37, 
		RULE_functorTerm = 38, RULE_listTerm = 39, RULE_listTermArguments = 40, 
		RULE_queryForm = 41, RULE_queryFormArguments = 42, RULE_queryFormTerm = 43, 
		RULE_name = 44, RULE_anyString = 45, RULE_annotations = 46, RULE_variables = 47, 
		RULE_variable = 48, RULE_udaPredicates = 49, RULE_keyword = 50, RULE_signedInteger = 51, 
		RULE_signedDecimal = 52;
	public static final String[] ruleNames = {
		"parseQueryForm", "parseQuery", "parseRule", "parseGroundTerm", "parseGroundPredicate", 
		"parseLoadDatabaseObjects", "parseLoadDatabaseFacts", "databaseObjectDeclarations", 
		"databaseDeclaration", "moduleDeclaration", "databaseBasePredicates", 
		"fact", "groundArguments", "groundTerm", "groundListTerms", "groundListTerms2", 
		"basePredicate", "basePredicateStructuralAttribute", "basePredicateKeyOrIndex", 
		"export", "predicateRule", "ruleHead", "ruleBody", "literal", "predicate", 
		"binaryOperator", "choiceArgument", "headPredicate", "headTerm", "headAggregateTerm", 
		"headFscntSubterms", "term", "basicExpression", "unaryArithmeticExpression", 
		"multiplicativeArithmeticExpression", "arithmeticExpression", "arithmeticTerm", 
		"nonArithmeticTerm", "functorTerm", "listTerm", "listTermArguments", "queryForm", 
		"queryFormArguments", "queryFormTerm", "name", "anyString", "annotations", 
		"variables", "variable", "udaPredicates", "keyword", "signedInteger", 
		"signedDecimal"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'.'", "'('", "'{'", "','", "'}'", "')'", "'['", "']'", "'|'", "':'", 
		"'<-'", "'~'", "'='", "'~='", "'>'", "'<'", "'>='", "'<='", "'*='", "'~*='", 
		"'*'", "'/'", "'-'", "'+'", "'@'", "'_'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, "LINE_COMMENT", "MULTILINE_COMMENT", "NEWLINE", "WS", 
		"STRING", "DATA_TYPE", "SORT_ORDER", "MODULE", "DATABASE", "EXPORT", "INDEX", 
		"KEY", "IF", "THEN", "ELSE", "TRUE", "FALSE", "NIL", "BEGIN", "END", "CHOICE", 
		"MULTI", "SINGLE", "EMPTY", "RETURN", "LIMIT", "SORT", "DESC", "ASC", 
		"MOD", "OPC", "LOG", "EXP", "STEP", "DIV", "AGGREGATE_FSMAX", "AGGREGATE_FSMIN", 
		"AGGREGATE_FSCNT", "AGGREGATE_FSSUM", "IDENTIFIER", "VARIABLENAME", "INPUT_VARIABLE", 
		"INTEGER", "DECIMAL"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "DeAL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }


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

	public DeALParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class ParseQueryFormContext extends ParserRuleContext {
		public QueryFormContext queryForm() {
			return getRuleContext(QueryFormContext.class,0);
		}
		public TerminalNode EOF() { return getToken(DeALParser.EOF, 0); }
		public ParseQueryFormContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parseQueryForm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterParseQueryForm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitParseQueryForm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitParseQueryForm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseQueryFormContext parseQueryForm() throws RecognitionException {
		ParseQueryFormContext _localctx = new ParseQueryFormContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_parseQueryForm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(106);
			queryForm();
			setState(108);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(107);
				match(T__0);
				}
			}

			setState(110);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParseQueryContext extends ParserRuleContext {
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public TerminalNode EOF() { return getToken(DeALParser.EOF, 0); }
		public ParseQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parseQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterParseQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitParseQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitParseQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseQueryContext parseQuery() throws RecognitionException {
		ParseQueryContext _localctx = new ParseQueryContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_parseQuery);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(112);
			predicate();
			setState(113);
			match(T__0);
			setState(114);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParseRuleContext extends ParserRuleContext {
		public PredicateRuleContext predicateRule() {
			return getRuleContext(PredicateRuleContext.class,0);
		}
		public TerminalNode EOF() { return getToken(DeALParser.EOF, 0); }
		public ParseRuleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parseRule; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterParseRule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitParseRule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitParseRule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseRuleContext parseRule() throws RecognitionException {
		ParseRuleContext _localctx = new ParseRuleContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_parseRule);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(116);
			predicateRule();
			setState(117);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParseGroundTermContext extends ParserRuleContext {
		public GroundTermContext groundTerm() {
			return getRuleContext(GroundTermContext.class,0);
		}
		public TerminalNode EOF() { return getToken(DeALParser.EOF, 0); }
		public ParseGroundTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parseGroundTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterParseGroundTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitParseGroundTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitParseGroundTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseGroundTermContext parseGroundTerm() throws RecognitionException {
		ParseGroundTermContext _localctx = new ParseGroundTermContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_parseGroundTerm);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(119);
			groundTerm();
			setState(120);
			match(T__0);
			setState(121);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParseGroundPredicateContext extends ParserRuleContext {
		public FactContext fact() {
			return getRuleContext(FactContext.class,0);
		}
		public TerminalNode EOF() { return getToken(DeALParser.EOF, 0); }
		public ParseGroundPredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parseGroundPredicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterParseGroundPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitParseGroundPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitParseGroundPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseGroundPredicateContext parseGroundPredicate() throws RecognitionException {
		ParseGroundPredicateContext _localctx = new ParseGroundPredicateContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_parseGroundPredicate);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(123);
			fact();
			setState(124);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParseLoadDatabaseObjectsContext extends ParserRuleContext {
		public DatabaseObjectDeclarationsContext databaseObjectDeclarations() {
			return getRuleContext(DatabaseObjectDeclarationsContext.class,0);
		}
		public TerminalNode EOF() { return getToken(DeALParser.EOF, 0); }
		public ParseLoadDatabaseObjectsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parseLoadDatabaseObjects; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterParseLoadDatabaseObjects(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitParseLoadDatabaseObjects(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitParseLoadDatabaseObjects(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseLoadDatabaseObjectsContext parseLoadDatabaseObjects() throws RecognitionException {
		ParseLoadDatabaseObjectsContext _localctx = new ParseLoadDatabaseObjectsContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_parseLoadDatabaseObjects);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(126);
			databaseObjectDeclarations();
			setState(127);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParseLoadDatabaseFactsContext extends ParserRuleContext {
		public TerminalNode EOF() { return getToken(DeALParser.EOF, 0); }
		public List<FactContext> fact() {
			return getRuleContexts(FactContext.class);
		}
		public FactContext fact(int i) {
			return getRuleContext(FactContext.class,i);
		}
		public ParseLoadDatabaseFactsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parseLoadDatabaseFacts; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterParseLoadDatabaseFacts(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitParseLoadDatabaseFacts(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitParseLoadDatabaseFacts(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParseLoadDatabaseFactsContext parseLoadDatabaseFacts() throws RecognitionException {
		ParseLoadDatabaseFactsContext _localctx = new ParseLoadDatabaseFactsContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_parseLoadDatabaseFacts);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(132);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==IDENTIFIER) {
				{
				{
				setState(129);
				fact();
				}
				}
				setState(134);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(135);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatabaseObjectDeclarationsContext extends ParserRuleContext {
		public ModuleDeclarationContext module;
		public DatabaseObjectDeclarationsContext tail;
		public ModuleDeclarationContext moduleDeclaration() {
			return getRuleContext(ModuleDeclarationContext.class,0);
		}
		public DatabaseObjectDeclarationsContext databaseObjectDeclarations() {
			return getRuleContext(DatabaseObjectDeclarationsContext.class,0);
		}
		public List<DatabaseDeclarationContext> databaseDeclaration() {
			return getRuleContexts(DatabaseDeclarationContext.class);
		}
		public DatabaseDeclarationContext databaseDeclaration(int i) {
			return getRuleContext(DatabaseDeclarationContext.class,i);
		}
		public DatabaseObjectDeclarationsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_databaseObjectDeclarations; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterDatabaseObjectDeclarations(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitDatabaseObjectDeclarations(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitDatabaseObjectDeclarations(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatabaseObjectDeclarationsContext databaseObjectDeclarations() throws RecognitionException {
		DatabaseObjectDeclarationsContext _localctx = new DatabaseObjectDeclarationsContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_databaseObjectDeclarations);
		int _la;
		try {
			setState(146);
			switch (_input.LA(1)) {
			case BEGIN:
				enterOuterAlt(_localctx, 1);
				{
				setState(137);
				((DatabaseObjectDeclarationsContext)_localctx).module = moduleDeclaration();
				setState(139);
				_la = _input.LA(1);
				if (((((_la - 35)) & ~0x3f) == 0 && ((1L << (_la - 35)) & ((1L << (DATABASE - 35)) | (1L << (EXPORT - 35)) | (1L << (INDEX - 35)) | (1L << (KEY - 35)) | (1L << (BEGIN - 35)) | (1L << (MULTI - 35)) | (1L << (SINGLE - 35)) | (1L << (EMPTY - 35)) | (1L << (RETURN - 35)) | (1L << (IDENTIFIER - 35)))) != 0)) {
					{
					setState(138);
					((DatabaseObjectDeclarationsContext)_localctx).tail = databaseObjectDeclarations();
					}
				}

				}
				break;
			case DATABASE:
			case EXPORT:
			case INDEX:
			case KEY:
			case MULTI:
			case SINGLE:
			case EMPTY:
			case RETURN:
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(142); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(141);
					databaseDeclaration();
					}
					}
					setState(144); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( ((((_la - 35)) & ~0x3f) == 0 && ((1L << (_la - 35)) & ((1L << (DATABASE - 35)) | (1L << (EXPORT - 35)) | (1L << (INDEX - 35)) | (1L << (KEY - 35)) | (1L << (MULTI - 35)) | (1L << (SINGLE - 35)) | (1L << (EMPTY - 35)) | (1L << (RETURN - 35)) | (1L << (IDENTIFIER - 35)))) != 0) );
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatabaseDeclarationContext extends ParserRuleContext {
		public DatabaseBasePredicatesContext dbbps;
		public BasePredicateKeyOrIndexContext bpkis;
		public ExportContext ex;
		public FactContext f;
		public PredicateRuleContext pr;
		public DatabaseBasePredicatesContext databaseBasePredicates() {
			return getRuleContext(DatabaseBasePredicatesContext.class,0);
		}
		public BasePredicateKeyOrIndexContext basePredicateKeyOrIndex() {
			return getRuleContext(BasePredicateKeyOrIndexContext.class,0);
		}
		public ExportContext export() {
			return getRuleContext(ExportContext.class,0);
		}
		public FactContext fact() {
			return getRuleContext(FactContext.class,0);
		}
		public PredicateRuleContext predicateRule() {
			return getRuleContext(PredicateRuleContext.class,0);
		}
		public DatabaseDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_databaseDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterDatabaseDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitDatabaseDeclaration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitDatabaseDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatabaseDeclarationContext databaseDeclaration() throws RecognitionException {
		DatabaseDeclarationContext _localctx = new DatabaseDeclarationContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_databaseDeclaration);
		try {
			setState(153);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,5,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(148);
				((DatabaseDeclarationContext)_localctx).dbbps = databaseBasePredicates();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(149);
				((DatabaseDeclarationContext)_localctx).bpkis = basePredicateKeyOrIndex();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(150);
				((DatabaseDeclarationContext)_localctx).ex = export();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(151);
				((DatabaseDeclarationContext)_localctx).f = fact();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(152);
				((DatabaseDeclarationContext)_localctx).pr = predicateRule();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ModuleDeclarationContext extends ParserRuleContext {
		public TerminalNode BEGIN() { return getToken(DeALParser.BEGIN, 0); }
		public List<TerminalNode> MODULE() { return getTokens(DeALParser.MODULE); }
		public TerminalNode MODULE(int i) {
			return getToken(DeALParser.MODULE, i);
		}
		public AnyStringContext anyString() {
			return getRuleContext(AnyStringContext.class,0);
		}
		public TerminalNode END() { return getToken(DeALParser.END, 0); }
		public List<DatabaseDeclarationContext> databaseDeclaration() {
			return getRuleContexts(DatabaseDeclarationContext.class);
		}
		public DatabaseDeclarationContext databaseDeclaration(int i) {
			return getRuleContext(DatabaseDeclarationContext.class,i);
		}
		public ModuleDeclarationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_moduleDeclaration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterModuleDeclaration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitModuleDeclaration(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitModuleDeclaration(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ModuleDeclarationContext moduleDeclaration() throws RecognitionException {
		ModuleDeclarationContext _localctx = new ModuleDeclarationContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_moduleDeclaration);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(155);
			match(BEGIN);
			setState(156);
			match(MODULE);
			setState(157);
			anyString();
			setState(158);
			match(T__0);
			setState(160); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(159);
				databaseDeclaration();
				}
				}
				setState(162); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( ((((_la - 35)) & ~0x3f) == 0 && ((1L << (_la - 35)) & ((1L << (DATABASE - 35)) | (1L << (EXPORT - 35)) | (1L << (INDEX - 35)) | (1L << (KEY - 35)) | (1L << (MULTI - 35)) | (1L << (SINGLE - 35)) | (1L << (EMPTY - 35)) | (1L << (RETURN - 35)) | (1L << (IDENTIFIER - 35)))) != 0) );
			setState(164);
			match(END);
			setState(165);
			match(MODULE);
			setState(166);
			match(T__0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatabaseBasePredicatesContext extends ParserRuleContext {
		public TerminalNode DATABASE() { return getToken(DeALParser.DATABASE, 0); }
		public List<BasePredicateContext> basePredicate() {
			return getRuleContexts(BasePredicateContext.class);
		}
		public BasePredicateContext basePredicate(int i) {
			return getRuleContext(BasePredicateContext.class,i);
		}
		public DatabaseBasePredicatesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_databaseBasePredicates; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterDatabaseBasePredicates(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitDatabaseBasePredicates(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitDatabaseBasePredicates(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatabaseBasePredicatesContext databaseBasePredicates() throws RecognitionException {
		DatabaseBasePredicatesContext _localctx = new DatabaseBasePredicatesContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_databaseBasePredicates);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(168);
			match(DATABASE);
			setState(169);
			match(T__1);
			setState(170);
			match(T__2);
			setState(171);
			basePredicate();
			setState(176);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(172);
				match(T__3);
				setState(173);
				basePredicate();
				}
				}
				setState(178);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(179);
			match(T__4);
			setState(180);
			match(T__5);
			setState(181);
			match(T__0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FactContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public GroundArgumentsContext groundArguments() {
			return getRuleContext(GroundArgumentsContext.class,0);
		}
		public FactContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fact; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterFact(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitFact(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitFact(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FactContext fact() throws RecognitionException {
		FactContext _localctx = new FactContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_fact);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(183);
			match(IDENTIFIER);
			setState(184);
			match(T__1);
			setState(185);
			groundArguments();
			setState(186);
			match(T__5);
			setState(187);
			match(T__0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroundArgumentsContext extends ParserRuleContext {
		public List<GroundTermContext> groundTerm() {
			return getRuleContexts(GroundTermContext.class);
		}
		public GroundTermContext groundTerm(int i) {
			return getRuleContext(GroundTermContext.class,i);
		}
		public GroundArgumentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groundArguments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterGroundArguments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitGroundArguments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitGroundArguments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroundArgumentsContext groundArguments() throws RecognitionException {
		GroundArgumentsContext _localctx = new GroundArgumentsContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_groundArguments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(189);
			groundTerm();
			setState(194);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(190);
				match(T__3);
				setState(191);
				groundTerm();
				}
				}
				setState(196);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroundTermContext extends ParserRuleContext {
		public Token fn;
		public Token l;
		public GroundArgumentsContext gafunc;
		public Token r;
		public GroundListTermsContext galist;
		public SignedDecimalContext dv;
		public SignedIntegerContext iv;
		public AnyStringContext s;
		public GroundArgumentsContext groundArguments() {
			return getRuleContext(GroundArgumentsContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public GroundListTermsContext groundListTerms() {
			return getRuleContext(GroundListTermsContext.class,0);
		}
		public SignedDecimalContext signedDecimal() {
			return getRuleContext(SignedDecimalContext.class,0);
		}
		public SignedIntegerContext signedInteger() {
			return getRuleContext(SignedIntegerContext.class,0);
		}
		public AnyStringContext anyString() {
			return getRuleContext(AnyStringContext.class,0);
		}
		public GroundTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groundTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterGroundTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitGroundTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitGroundTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroundTermContext groundTerm() throws RecognitionException {
		GroundTermContext _localctx = new GroundTermContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_groundTerm);
		int _la;
		try {
			setState(211);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(198);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(197);
					((GroundTermContext)_localctx).fn = match(IDENTIFIER);
					}
				}

				setState(200);
				((GroundTermContext)_localctx).l = match(T__1);
				setState(201);
				((GroundTermContext)_localctx).gafunc = groundArguments();
				setState(202);
				((GroundTermContext)_localctx).r = match(T__5);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(204);
				((GroundTermContext)_localctx).l = match(T__6);
				setState(205);
				((GroundTermContext)_localctx).galist = groundListTerms();
				setState(206);
				((GroundTermContext)_localctx).r = match(T__7);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(208);
				((GroundTermContext)_localctx).dv = signedDecimal();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(209);
				((GroundTermContext)_localctx).iv = signedInteger();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(210);
				((GroundTermContext)_localctx).s = anyString();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroundListTermsContext extends ParserRuleContext {
		public GroundTermContext groundTerm() {
			return getRuleContext(GroundTermContext.class,0);
		}
		public GroundListTerms2Context groundListTerms2() {
			return getRuleContext(GroundListTerms2Context.class,0);
		}
		public GroundListTermsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groundListTerms; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterGroundListTerms(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitGroundListTerms(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitGroundListTerms(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroundListTermsContext groundListTerms() throws RecognitionException {
		GroundListTermsContext _localctx = new GroundListTermsContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_groundListTerms);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(213);
			groundTerm();
			setState(215);
			_la = _input.LA(1);
			if (_la==T__3 || _la==T__8) {
				{
				setState(214);
				groundListTerms2();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroundListTerms2Context extends ParserRuleContext {
		public GroundTermContext groundTerm() {
			return getRuleContext(GroundTermContext.class,0);
		}
		public GroundListTermsContext groundListTerms() {
			return getRuleContext(GroundListTermsContext.class,0);
		}
		public GroundListTerms2Context(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groundListTerms2; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterGroundListTerms2(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitGroundListTerms2(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitGroundListTerms2(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroundListTerms2Context groundListTerms2() throws RecognitionException {
		GroundListTerms2Context _localctx = new GroundListTerms2Context(_ctx, getState());
		enterRule(_localctx, 30, RULE_groundListTerms2);
		int _la;
		try {
			setState(227);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(217);
				match(T__8);
				setState(218);
				groundTerm();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(219);
				match(T__8);
				setState(220);
				match(T__6);
				setState(222);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << T__6) | (1L << T__22) | (1L << STRING) | (1L << DATA_TYPE) | (1L << SORT_ORDER) | (1L << MODULE) | (1L << DATABASE) | (1L << EXPORT) | (1L << INDEX) | (1L << KEY) | (1L << IF) | (1L << THEN) | (1L << ELSE) | (1L << TRUE) | (1L << FALSE) | (1L << BEGIN) | (1L << END) | (1L << CHOICE) | (1L << MULTI) | (1L << SINGLE) | (1L << EMPTY) | (1L << RETURN) | (1L << MOD) | (1L << OPC) | (1L << LOG) | (1L << EXP) | (1L << STEP) | (1L << AGGREGATE_FSMAX) | (1L << AGGREGATE_FSMIN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (AGGREGATE_FSCNT - 64)) | (1L << (AGGREGATE_FSSUM - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (VARIABLENAME - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)))) != 0)) {
					{
					setState(221);
					groundListTerms();
					}
				}

				setState(224);
				match(T__7);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(225);
				match(T__3);
				setState(226);
				groundListTerms();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BasePredicateContext extends ParserRuleContext {
		public Token pn;
		public List<BasePredicateStructuralAttributeContext> basePredicateStructuralAttribute() {
			return getRuleContexts(BasePredicateStructuralAttributeContext.class);
		}
		public BasePredicateStructuralAttributeContext basePredicateStructuralAttribute(int i) {
			return getRuleContext(BasePredicateStructuralAttributeContext.class,i);
		}
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public BasePredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_basePredicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterBasePredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitBasePredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitBasePredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BasePredicateContext basePredicate() throws RecognitionException {
		BasePredicateContext _localctx = new BasePredicateContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_basePredicate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(229);
			((BasePredicateContext)_localctx).pn = match(IDENTIFIER);
			setState(230);
			match(T__1);
			setState(231);
			basePredicateStructuralAttribute();
			setState(236);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(232);
				match(T__3);
				setState(233);
				basePredicateStructuralAttribute();
				}
				}
				setState(238);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(239);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BasePredicateStructuralAttributeContext extends ParserRuleContext {
		public NameContext n;
		public Token dt;
		public Token l;
		public Token r;
		public Token l2;
		public Token r2;
		public TerminalNode DATA_TYPE() { return getToken(DeALParser.DATA_TYPE, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public List<BasePredicateStructuralAttributeContext> basePredicateStructuralAttribute() {
			return getRuleContexts(BasePredicateStructuralAttributeContext.class);
		}
		public BasePredicateStructuralAttributeContext basePredicateStructuralAttribute(int i) {
			return getRuleContext(BasePredicateStructuralAttributeContext.class,i);
		}
		public BasePredicateStructuralAttributeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_basePredicateStructuralAttribute; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterBasePredicateStructuralAttribute(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitBasePredicateStructuralAttribute(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitBasePredicateStructuralAttribute(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BasePredicateStructuralAttributeContext basePredicateStructuralAttribute() throws RecognitionException {
		BasePredicateStructuralAttributeContext _localctx = new BasePredicateStructuralAttributeContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_basePredicateStructuralAttribute);
		int _la;
		try {
			setState(277);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(244);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
				case 1:
					{
					setState(241);
					((BasePredicateStructuralAttributeContext)_localctx).n = name();
					setState(242);
					match(T__9);
					}
					break;
				}
				setState(246);
				((BasePredicateStructuralAttributeContext)_localctx).dt = match(DATA_TYPE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(248);
				_la = _input.LA(1);
				if (((((_la - 32)) & ~0x3f) == 0 && ((1L << (_la - 32)) & ((1L << (DATA_TYPE - 32)) | (1L << (SORT_ORDER - 32)) | (1L << (MODULE - 32)) | (1L << (DATABASE - 32)) | (1L << (EXPORT - 32)) | (1L << (INDEX - 32)) | (1L << (KEY - 32)) | (1L << (IF - 32)) | (1L << (THEN - 32)) | (1L << (ELSE - 32)) | (1L << (TRUE - 32)) | (1L << (FALSE - 32)) | (1L << (BEGIN - 32)) | (1L << (END - 32)) | (1L << (CHOICE - 32)) | (1L << (MULTI - 32)) | (1L << (SINGLE - 32)) | (1L << (EMPTY - 32)) | (1L << (RETURN - 32)) | (1L << (MOD - 32)) | (1L << (OPC - 32)) | (1L << (LOG - 32)) | (1L << (EXP - 32)) | (1L << (STEP - 32)) | (1L << (AGGREGATE_FSMAX - 32)) | (1L << (AGGREGATE_FSMIN - 32)) | (1L << (AGGREGATE_FSCNT - 32)) | (1L << (AGGREGATE_FSSUM - 32)) | (1L << (IDENTIFIER - 32)) | (1L << (VARIABLENAME - 32)))) != 0)) {
					{
					setState(247);
					((BasePredicateStructuralAttributeContext)_localctx).n = name();
					}
				}

				setState(250);
				((BasePredicateStructuralAttributeContext)_localctx).l = match(T__1);
				setState(251);
				basePredicateStructuralAttribute();
				setState(256);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(252);
					match(T__3);
					setState(253);
					basePredicateStructuralAttribute();
					}
					}
					setState(258);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(259);
				((BasePredicateStructuralAttributeContext)_localctx).r = match(T__5);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(264);
				_la = _input.LA(1);
				if (((((_la - 32)) & ~0x3f) == 0 && ((1L << (_la - 32)) & ((1L << (DATA_TYPE - 32)) | (1L << (SORT_ORDER - 32)) | (1L << (MODULE - 32)) | (1L << (DATABASE - 32)) | (1L << (EXPORT - 32)) | (1L << (INDEX - 32)) | (1L << (KEY - 32)) | (1L << (IF - 32)) | (1L << (THEN - 32)) | (1L << (ELSE - 32)) | (1L << (TRUE - 32)) | (1L << (FALSE - 32)) | (1L << (BEGIN - 32)) | (1L << (END - 32)) | (1L << (CHOICE - 32)) | (1L << (MULTI - 32)) | (1L << (SINGLE - 32)) | (1L << (EMPTY - 32)) | (1L << (RETURN - 32)) | (1L << (MOD - 32)) | (1L << (OPC - 32)) | (1L << (LOG - 32)) | (1L << (EXP - 32)) | (1L << (STEP - 32)) | (1L << (AGGREGATE_FSMAX - 32)) | (1L << (AGGREGATE_FSMIN - 32)) | (1L << (AGGREGATE_FSCNT - 32)) | (1L << (AGGREGATE_FSSUM - 32)) | (1L << (IDENTIFIER - 32)) | (1L << (VARIABLENAME - 32)))) != 0)) {
					{
					setState(261);
					((BasePredicateStructuralAttributeContext)_localctx).n = name();
					setState(262);
					match(T__9);
					}
				}

				setState(266);
				((BasePredicateStructuralAttributeContext)_localctx).l2 = match(T__6);
				setState(267);
				basePredicateStructuralAttribute();
				setState(272);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(268);
					match(T__3);
					setState(269);
					basePredicateStructuralAttribute();
					}
					}
					setState(274);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(275);
				((BasePredicateStructuralAttributeContext)_localctx).r2 = match(T__7);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BasePredicateKeyOrIndexContext extends ParserRuleContext {
		public Token pn;
		public List<TerminalNode> INTEGER() { return getTokens(DeALParser.INTEGER); }
		public TerminalNode INTEGER(int i) {
			return getToken(DeALParser.INTEGER, i);
		}
		public TerminalNode KEY() { return getToken(DeALParser.KEY, 0); }
		public TerminalNode INDEX() { return getToken(DeALParser.INDEX, 0); }
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public BasePredicateKeyOrIndexContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_basePredicateKeyOrIndex; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterBasePredicateKeyOrIndex(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitBasePredicateKeyOrIndex(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitBasePredicateKeyOrIndex(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BasePredicateKeyOrIndexContext basePredicateKeyOrIndex() throws RecognitionException {
		BasePredicateKeyOrIndexContext _localctx = new BasePredicateKeyOrIndexContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_basePredicateKeyOrIndex);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(279);
			_la = _input.LA(1);
			if ( !(_la==INDEX || _la==KEY) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			setState(280);
			match(T__1);
			setState(281);
			((BasePredicateKeyOrIndexContext)_localctx).pn = match(IDENTIFIER);
			setState(282);
			match(T__3);
			setState(283);
			match(T__6);
			setState(284);
			match(INTEGER);
			setState(289);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(285);
				match(T__3);
				setState(286);
				match(INTEGER);
				}
				}
				setState(291);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(292);
			match(T__7);
			setState(293);
			match(T__5);
			setState(294);
			match(T__0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExportContext extends ParserRuleContext {
		public TerminalNode EXPORT() { return getToken(DeALParser.EXPORT, 0); }
		public QueryFormContext queryForm() {
			return getRuleContext(QueryFormContext.class,0);
		}
		public ExportContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_export; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterExport(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitExport(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitExport(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExportContext export() throws RecognitionException {
		ExportContext _localctx = new ExportContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_export);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(296);
			match(EXPORT);
			setState(297);
			queryForm();
			setState(298);
			match(T__0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateRuleContext extends ParserRuleContext {
		public RuleHeadContext head;
		public RuleBodyContext body;
		public RuleHeadContext ruleHead() {
			return getRuleContext(RuleHeadContext.class,0);
		}
		public AnnotationsContext annotations() {
			return getRuleContext(AnnotationsContext.class,0);
		}
		public RuleBodyContext ruleBody() {
			return getRuleContext(RuleBodyContext.class,0);
		}
		public PredicateRuleContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicateRule; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterPredicateRule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitPredicateRule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitPredicateRule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateRuleContext predicateRule() throws RecognitionException {
		PredicateRuleContext _localctx = new PredicateRuleContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_predicateRule);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(300);
			((PredicateRuleContext)_localctx).head = ruleHead();
			setState(303);
			_la = _input.LA(1);
			if (_la==T__10) {
				{
				setState(301);
				match(T__10);
				setState(302);
				((PredicateRuleContext)_localctx).body = ruleBody();
				}
			}

			setState(305);
			match(T__0);
			setState(307);
			_la = _input.LA(1);
			if (_la==T__24) {
				{
				setState(306);
				annotations();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RuleHeadContext extends ParserRuleContext {
		public UdaPredicatesContext action;
		public Token an;
		public HeadPredicateContext hp;
		public List<TermContext> term() {
			return getRuleContexts(TermContext.class);
		}
		public TermContext term(int i) {
			return getRuleContext(TermContext.class,i);
		}
		public UdaPredicatesContext udaPredicates() {
			return getRuleContext(UdaPredicatesContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public HeadPredicateContext headPredicate() {
			return getRuleContext(HeadPredicateContext.class,0);
		}
		public RuleHeadContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ruleHead; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterRuleHead(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitRuleHead(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitRuleHead(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RuleHeadContext ruleHead() throws RecognitionException {
		RuleHeadContext _localctx = new RuleHeadContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_ruleHead);
		int _la;
		try {
			setState(324);
			switch (_input.LA(1)) {
			case MULTI:
			case SINGLE:
			case EMPTY:
			case RETURN:
				enterOuterAlt(_localctx, 1);
				{
				setState(309);
				((RuleHeadContext)_localctx).action = udaPredicates();
				setState(310);
				match(T__1);
				setState(311);
				((RuleHeadContext)_localctx).an = match(IDENTIFIER);
				setState(312);
				match(T__3);
				setState(313);
				term();
				setState(318);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(314);
					match(T__3);
					setState(315);
					term();
					}
					}
					setState(320);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(321);
				match(T__5);
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(323);
				((RuleHeadContext)_localctx).hp = headPredicate();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RuleBodyContext extends ParserRuleContext {
		public List<LiteralContext> literal() {
			return getRuleContexts(LiteralContext.class);
		}
		public LiteralContext literal(int i) {
			return getRuleContext(LiteralContext.class,i);
		}
		public RuleBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ruleBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterRuleBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitRuleBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitRuleBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RuleBodyContext ruleBody() throws RecognitionException {
		RuleBodyContext _localctx = new RuleBodyContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_ruleBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(326);
			literal();
			setState(331);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(327);
				match(T__3);
				setState(328);
				literal();
				}
				}
				setState(333);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LiteralContext extends ParserRuleContext {
		public Predicate retval;
		public Token action;
		public RuleBodyContext b1;
		public RuleBodyContext b2;
		public RuleBodyContext b3;
		public ChoiceArgumentContext ca1;
		public ChoiceArgumentContext ca2;
		public TermContext t1;
		public BinaryOperatorContext o;
		public TermContext t2;
		public Token neg;
		public PredicateContext p;
		public TerminalNode TRUE() { return getToken(DeALParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(DeALParser.FALSE, 0); }
		public TerminalNode THEN() { return getToken(DeALParser.THEN, 0); }
		public TerminalNode IF() { return getToken(DeALParser.IF, 0); }
		public List<RuleBodyContext> ruleBody() {
			return getRuleContexts(RuleBodyContext.class);
		}
		public RuleBodyContext ruleBody(int i) {
			return getRuleContext(RuleBodyContext.class,i);
		}
		public TerminalNode ELSE() { return getToken(DeALParser.ELSE, 0); }
		public TerminalNode CHOICE() { return getToken(DeALParser.CHOICE, 0); }
		public List<ChoiceArgumentContext> choiceArgument() {
			return getRuleContexts(ChoiceArgumentContext.class);
		}
		public ChoiceArgumentContext choiceArgument(int i) {
			return getRuleContext(ChoiceArgumentContext.class,i);
		}
		public TerminalNode LIMIT() { return getToken(DeALParser.LIMIT, 0); }
		public List<VariableContext> variable() {
			return getRuleContexts(VariableContext.class);
		}
		public VariableContext variable(int i) {
			return getRuleContext(VariableContext.class,i);
		}
		public TerminalNode INTEGER() { return getToken(DeALParser.INTEGER, 0); }
		public List<TerminalNode> SORT_ORDER() { return getTokens(DeALParser.SORT_ORDER); }
		public TerminalNode SORT_ORDER(int i) {
			return getToken(DeALParser.SORT_ORDER, i);
		}
		public TerminalNode SORT() { return getToken(DeALParser.SORT, 0); }
		public List<TermContext> term() {
			return getRuleContexts(TermContext.class);
		}
		public TermContext term(int i) {
			return getRuleContext(TermContext.class,i);
		}
		public BinaryOperatorContext binaryOperator() {
			return getRuleContext(BinaryOperatorContext.class,0);
		}
		public PredicateContext predicate() {
			return getRuleContext(PredicateContext.class,0);
		}
		public LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_literal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LiteralContext literal() throws RecognitionException {
		LiteralContext _localctx = new LiteralContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_literal);
		int _la;
		try {
			setState(390);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(334);
				((LiteralContext)_localctx).action = match(TRUE);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(335);
				((LiteralContext)_localctx).action = match(FALSE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(336);
				((LiteralContext)_localctx).action = match(IF);
				setState(337);
				match(T__1);
				setState(338);
				((LiteralContext)_localctx).b1 = ruleBody();
				setState(339);
				match(THEN);
				setState(340);
				((LiteralContext)_localctx).b2 = ruleBody();
				setState(343);
				_la = _input.LA(1);
				if (_la==ELSE) {
					{
					setState(341);
					match(ELSE);
					setState(342);
					((LiteralContext)_localctx).b3 = ruleBody();
					}
				}

				setState(345);
				match(T__5);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(347);
				((LiteralContext)_localctx).action = match(CHOICE);
				setState(348);
				match(T__1);
				setState(349);
				((LiteralContext)_localctx).ca1 = choiceArgument();
				setState(350);
				match(T__3);
				setState(351);
				((LiteralContext)_localctx).ca2 = choiceArgument();
				setState(352);
				match(T__5);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(354);
				((LiteralContext)_localctx).action = match(LIMIT);
				setState(355);
				match(T__1);
				setState(358);
				switch (_input.LA(1)) {
				case T__25:
				case VARIABLENAME:
					{
					setState(356);
					variable();
					}
					break;
				case INTEGER:
					{
					setState(357);
					match(INTEGER);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(360);
				match(T__5);
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(361);
				((LiteralContext)_localctx).action = match(SORT);
				setState(362);
				match(T__1);
				setState(363);
				match(T__1);
				setState(364);
				variable();
				setState(365);
				match(T__3);
				setState(366);
				match(SORT_ORDER);
				setState(367);
				match(T__5);
				setState(377);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(368);
					match(T__3);
					setState(369);
					match(T__1);
					setState(370);
					variable();
					setState(371);
					match(T__3);
					setState(372);
					match(SORT_ORDER);
					setState(373);
					match(T__5);
					}
					}
					setState(379);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(380);
				match(T__5);
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(382);
				((LiteralContext)_localctx).t1 = term();
				setState(383);
				((LiteralContext)_localctx).o = binaryOperator();
				setState(384);
				((LiteralContext)_localctx).t2 = term();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(387);
				_la = _input.LA(1);
				if (_la==T__11) {
					{
					setState(386);
					((LiteralContext)_localctx).neg = match(T__11);
					}
				}

				setState(389);
				((LiteralContext)_localctx).p = predicate();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PredicateContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public List<TermContext> term() {
			return getRuleContexts(TermContext.class);
		}
		public TermContext term(int i) {
			return getRuleContext(TermContext.class,i);
		}
		public PredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_predicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PredicateContext predicate() throws RecognitionException {
		PredicateContext _localctx = new PredicateContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_predicate);
		int _la;
		try {
			setState(409);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(392);
				match(IDENTIFIER);
				setState(404);
				_la = _input.LA(1);
				if (_la==T__1) {
					{
					setState(393);
					match(T__1);
					setState(394);
					term();
					setState(399);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while (_la==T__3) {
						{
						{
						setState(395);
						match(T__3);
						setState(396);
						term();
						}
						}
						setState(401);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(402);
					match(T__5);
					}
				}

				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(406);
				match(IDENTIFIER);
				setState(407);
				match(T__1);
				setState(408);
				match(T__5);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BinaryOperatorContext extends ParserRuleContext {
		public BinaryOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_binaryOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterBinaryOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitBinaryOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitBinaryOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BinaryOperatorContext binaryOperator() throws RecognitionException {
		BinaryOperatorContext _localctx = new BinaryOperatorContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_binaryOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(411);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__12) | (1L << T__13) | (1L << T__14) | (1L << T__15) | (1L << T__16) | (1L << T__17) | (1L << T__18) | (1L << T__19))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ChoiceArgumentContext extends ParserRuleContext {
		public List<VariableContext> variable() {
			return getRuleContexts(VariableContext.class);
		}
		public VariableContext variable(int i) {
			return getRuleContext(VariableContext.class,i);
		}
		public ChoiceArgumentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_choiceArgument; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterChoiceArgument(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitChoiceArgument(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitChoiceArgument(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ChoiceArgumentContext choiceArgument() throws RecognitionException {
		ChoiceArgumentContext _localctx = new ChoiceArgumentContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_choiceArgument);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(413);
			match(T__1);
			setState(422);
			_la = _input.LA(1);
			if (_la==T__25 || _la==VARIABLENAME) {
				{
				setState(414);
				variable();
				setState(419);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(415);
					match(T__3);
					setState(416);
					variable();
					}
					}
					setState(421);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(424);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HeadPredicateContext extends ParserRuleContext {
		public Token pn;
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public List<HeadTermContext> headTerm() {
			return getRuleContexts(HeadTermContext.class);
		}
		public HeadTermContext headTerm(int i) {
			return getRuleContext(HeadTermContext.class,i);
		}
		public HeadPredicateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_headPredicate; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterHeadPredicate(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitHeadPredicate(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitHeadPredicate(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HeadPredicateContext headPredicate() throws RecognitionException {
		HeadPredicateContext _localctx = new HeadPredicateContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_headPredicate);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(426);
			((HeadPredicateContext)_localctx).pn = match(IDENTIFIER);
			setState(438);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(427);
				match(T__1);
				setState(428);
				headTerm();
				setState(433);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__3) {
					{
					{
					setState(429);
					match(T__3);
					setState(430);
					headTerm();
					}
					}
					setState(435);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(436);
				match(T__5);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HeadTermContext extends ParserRuleContext {
		public ArithmeticExpressionContext ae;
		public HeadAggregateTermContext hat;
		public NonArithmeticTermContext hnat;
		public Token nil;
		public ArithmeticExpressionContext arithmeticExpression() {
			return getRuleContext(ArithmeticExpressionContext.class,0);
		}
		public HeadAggregateTermContext headAggregateTerm() {
			return getRuleContext(HeadAggregateTermContext.class,0);
		}
		public NonArithmeticTermContext nonArithmeticTerm() {
			return getRuleContext(NonArithmeticTermContext.class,0);
		}
		public TerminalNode NIL() { return getToken(DeALParser.NIL, 0); }
		public HeadTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_headTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterHeadTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitHeadTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitHeadTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HeadTermContext headTerm() throws RecognitionException {
		HeadTermContext _localctx = new HeadTermContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_headTerm);
		try {
			setState(444);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,39,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(440);
				((HeadTermContext)_localctx).ae = arithmeticExpression();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(441);
				((HeadTermContext)_localctx).hat = headAggregateTerm();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(442);
				((HeadTermContext)_localctx).hnat = nonArithmeticTerm();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(443);
				((HeadTermContext)_localctx).nil = match(NIL);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HeadAggregateTermContext extends ParserRuleContext {
		public Token aggr;
		public TermContext hs;
		public Token fsagg;
		public VariableContext vt;
		public HeadFscntSubtermsContext hfs;
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public TermContext term() {
			return getRuleContext(TermContext.class,0);
		}
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public TerminalNode AGGREGATE_FSMAX() { return getToken(DeALParser.AGGREGATE_FSMAX, 0); }
		public TerminalNode AGGREGATE_FSMIN() { return getToken(DeALParser.AGGREGATE_FSMIN, 0); }
		public HeadFscntSubtermsContext headFscntSubterms() {
			return getRuleContext(HeadFscntSubtermsContext.class,0);
		}
		public TerminalNode AGGREGATE_FSCNT() { return getToken(DeALParser.AGGREGATE_FSCNT, 0); }
		public TerminalNode AGGREGATE_FSSUM() { return getToken(DeALParser.AGGREGATE_FSSUM, 0); }
		public HeadAggregateTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_headAggregateTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterHeadAggregateTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitHeadAggregateTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitHeadAggregateTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HeadAggregateTermContext headAggregateTerm() throws RecognitionException {
		HeadAggregateTermContext _localctx = new HeadAggregateTermContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_headAggregateTerm);
		int _la;
		try {
			setState(461);
			switch (_input.LA(1)) {
			case IDENTIFIER:
				enterOuterAlt(_localctx, 1);
				{
				setState(446);
				((HeadAggregateTermContext)_localctx).aggr = match(IDENTIFIER);
				setState(447);
				match(T__15);
				setState(448);
				((HeadAggregateTermContext)_localctx).hs = term();
				setState(449);
				match(T__14);
				}
				break;
			case AGGREGATE_FSMAX:
			case AGGREGATE_FSMIN:
				enterOuterAlt(_localctx, 2);
				{
				setState(451);
				((HeadAggregateTermContext)_localctx).fsagg = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==AGGREGATE_FSMAX || _la==AGGREGATE_FSMIN) ) {
					((HeadAggregateTermContext)_localctx).fsagg = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(452);
				match(T__15);
				setState(453);
				((HeadAggregateTermContext)_localctx).vt = variable();
				setState(454);
				match(T__14);
				}
				break;
			case AGGREGATE_FSCNT:
			case AGGREGATE_FSSUM:
				enterOuterAlt(_localctx, 3);
				{
				setState(456);
				((HeadAggregateTermContext)_localctx).fsagg = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==AGGREGATE_FSCNT || _la==AGGREGATE_FSSUM) ) {
					((HeadAggregateTermContext)_localctx).fsagg = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(457);
				match(T__15);
				setState(458);
				((HeadAggregateTermContext)_localctx).hfs = headFscntSubterms();
				setState(459);
				match(T__14);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HeadFscntSubtermsContext extends ParserRuleContext {
		public VariableContext vt1;
		public VariableContext vt2;
		public VariablesContext vts;
		public ArithmeticTermContext at;
		public List<VariableContext> variable() {
			return getRuleContexts(VariableContext.class);
		}
		public VariableContext variable(int i) {
			return getRuleContext(VariableContext.class,i);
		}
		public VariablesContext variables() {
			return getRuleContext(VariablesContext.class,0);
		}
		public ArithmeticTermContext arithmeticTerm() {
			return getRuleContext(ArithmeticTermContext.class,0);
		}
		public HeadFscntSubtermsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_headFscntSubterms; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterHeadFscntSubterms(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitHeadFscntSubterms(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitHeadFscntSubterms(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HeadFscntSubtermsContext headFscntSubterms() throws RecognitionException {
		HeadFscntSubtermsContext _localctx = new HeadFscntSubtermsContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_headFscntSubterms);
		try {
			setState(500);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,41,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(463);
				match(T__1);
				setState(464);
				((HeadFscntSubtermsContext)_localctx).vt1 = variable();
				setState(465);
				match(T__3);
				setState(466);
				match(T__1);
				setState(467);
				((HeadFscntSubtermsContext)_localctx).vt2 = variable();
				setState(468);
				match(T__3);
				setState(469);
				((HeadFscntSubtermsContext)_localctx).vts = variables();
				setState(470);
				match(T__5);
				setState(471);
				match(T__5);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(473);
				match(T__1);
				setState(474);
				((HeadFscntSubtermsContext)_localctx).vt1 = variable();
				setState(475);
				match(T__3);
				setState(476);
				((HeadFscntSubtermsContext)_localctx).at = arithmeticTerm();
				setState(477);
				match(T__5);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(479);
				match(T__1);
				setState(480);
				((HeadFscntSubtermsContext)_localctx).vt1 = variable();
				setState(481);
				match(T__3);
				setState(482);
				match(T__1);
				setState(483);
				((HeadFscntSubtermsContext)_localctx).vt2 = variable();
				setState(484);
				match(T__5);
				setState(485);
				match(T__5);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(487);
				match(T__1);
				setState(488);
				match(T__1);
				setState(489);
				((HeadFscntSubtermsContext)_localctx).vt1 = variable();
				setState(490);
				match(T__3);
				setState(491);
				((HeadFscntSubtermsContext)_localctx).at = arithmeticTerm();
				setState(492);
				match(T__5);
				setState(493);
				match(T__5);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(495);
				((HeadFscntSubtermsContext)_localctx).at = arithmeticTerm();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(496);
				match(T__1);
				setState(497);
				((HeadFscntSubtermsContext)_localctx).at = arithmeticTerm();
				setState(498);
				match(T__5);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TermContext extends ParserRuleContext {
		public ArithmeticExpressionContext arithmeticExpression() {
			return getRuleContext(ArithmeticExpressionContext.class,0);
		}
		public NonArithmeticTermContext nonArithmeticTerm() {
			return getRuleContext(NonArithmeticTermContext.class,0);
		}
		public TermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_term; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TermContext term() throws RecognitionException {
		TermContext _localctx = new TermContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_term);
		try {
			setState(504);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(502);
				arithmeticExpression();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(503);
				nonArithmeticTerm();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BasicExpressionContext extends ParserRuleContext {
		public ArithmeticTermContext at;
		public ArithmeticExpressionContext ae;
		public ArithmeticTermContext arithmeticTerm() {
			return getRuleContext(ArithmeticTermContext.class,0);
		}
		public ArithmeticExpressionContext arithmeticExpression() {
			return getRuleContext(ArithmeticExpressionContext.class,0);
		}
		public BasicExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_basicExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterBasicExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitBasicExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitBasicExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BasicExpressionContext basicExpression() throws RecognitionException {
		BasicExpressionContext _localctx = new BasicExpressionContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_basicExpression);
		try {
			setState(511);
			switch (_input.LA(1)) {
			case T__22:
			case T__25:
			case VARIABLENAME:
			case INTEGER:
			case DECIMAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(506);
				((BasicExpressionContext)_localctx).at = arithmeticTerm();
				}
				break;
			case T__1:
				enterOuterAlt(_localctx, 2);
				{
				setState(507);
				match(T__1);
				setState(508);
				((BasicExpressionContext)_localctx).ae = arithmeticExpression();
				setState(509);
				match(T__5);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnaryArithmeticExpressionContext extends ParserRuleContext {
		public BasicExpressionContext be;
		public Token l;
		public BasicExpressionContext basicExpression() {
			return getRuleContext(BasicExpressionContext.class,0);
		}
		public TerminalNode LOG() { return getToken(DeALParser.LOG, 0); }
		public TerminalNode EXP() { return getToken(DeALParser.EXP, 0); }
		public TerminalNode STEP() { return getToken(DeALParser.STEP, 0); }
		public UnaryArithmeticExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unaryArithmeticExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterUnaryArithmeticExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitUnaryArithmeticExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitUnaryArithmeticExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnaryArithmeticExpressionContext unaryArithmeticExpression() throws RecognitionException {
		UnaryArithmeticExpressionContext _localctx = new UnaryArithmeticExpressionContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_unaryArithmeticExpression);
		try {
			setState(520);
			switch (_input.LA(1)) {
			case T__1:
			case T__22:
			case T__25:
			case VARIABLENAME:
			case INTEGER:
			case DECIMAL:
				enterOuterAlt(_localctx, 1);
				{
				setState(513);
				((UnaryArithmeticExpressionContext)_localctx).be = basicExpression();
				}
				break;
			case LOG:
				enterOuterAlt(_localctx, 2);
				{
				setState(514);
				((UnaryArithmeticExpressionContext)_localctx).l = match(LOG);
				setState(515);
				((UnaryArithmeticExpressionContext)_localctx).be = basicExpression();
				}
				break;
			case EXP:
				enterOuterAlt(_localctx, 3);
				{
				setState(516);
				((UnaryArithmeticExpressionContext)_localctx).l = match(EXP);
				setState(517);
				((UnaryArithmeticExpressionContext)_localctx).be = basicExpression();
				}
				break;
			case STEP:
				enterOuterAlt(_localctx, 4);
				{
				setState(518);
				((UnaryArithmeticExpressionContext)_localctx).l = match(STEP);
				setState(519);
				((UnaryArithmeticExpressionContext)_localctx).be = basicExpression();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiplicativeArithmeticExpressionContext extends ParserRuleContext {
		public UnaryArithmeticExpressionContext uae1;
		public Token p;
		public UnaryArithmeticExpressionContext uae2;
		public Token s;
		public UnaryArithmeticExpressionContext uae;
		public List<UnaryArithmeticExpressionContext> unaryArithmeticExpression() {
			return getRuleContexts(UnaryArithmeticExpressionContext.class);
		}
		public UnaryArithmeticExpressionContext unaryArithmeticExpression(int i) {
			return getRuleContext(UnaryArithmeticExpressionContext.class,i);
		}
		public List<TerminalNode> DIV() { return getTokens(DeALParser.DIV); }
		public TerminalNode DIV(int i) {
			return getToken(DeALParser.DIV, i);
		}
		public List<TerminalNode> MOD() { return getTokens(DeALParser.MOD); }
		public TerminalNode MOD(int i) {
			return getToken(DeALParser.MOD, i);
		}
		public List<TerminalNode> OPC() { return getTokens(DeALParser.OPC); }
		public TerminalNode OPC(int i) {
			return getToken(DeALParser.OPC, i);
		}
		public MultiplicativeArithmeticExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiplicativeArithmeticExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterMultiplicativeArithmeticExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitMultiplicativeArithmeticExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitMultiplicativeArithmeticExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiplicativeArithmeticExpressionContext multiplicativeArithmeticExpression() throws RecognitionException {
		MultiplicativeArithmeticExpressionContext _localctx = new MultiplicativeArithmeticExpressionContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_multiplicativeArithmeticExpression);
		int _la;
		try {
			setState(532);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(522);
				((MultiplicativeArithmeticExpressionContext)_localctx).uae1 = unaryArithmeticExpression();
				setState(527);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__20) | (1L << T__21) | (1L << MOD) | (1L << OPC) | (1L << DIV))) != 0)) {
					{
					{
					setState(523);
					((MultiplicativeArithmeticExpressionContext)_localctx).p = _input.LT(1);
					_la = _input.LA(1);
					if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__20) | (1L << T__21) | (1L << MOD) | (1L << OPC) | (1L << DIV))) != 0)) ) {
						((MultiplicativeArithmeticExpressionContext)_localctx).p = (Token)_errHandler.recoverInline(this);
					} else {
						consume();
					}
					setState(524);
					((MultiplicativeArithmeticExpressionContext)_localctx).uae2 = unaryArithmeticExpression();
					}
					}
					setState(529);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(530);
				((MultiplicativeArithmeticExpressionContext)_localctx).s = match(T__22);
				setState(531);
				((MultiplicativeArithmeticExpressionContext)_localctx).uae = unaryArithmeticExpression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticExpressionContext extends ParserRuleContext {
		public MultiplicativeArithmeticExpressionContext mae1;
		public Token p;
		public MultiplicativeArithmeticExpressionContext mae2;
		public List<MultiplicativeArithmeticExpressionContext> multiplicativeArithmeticExpression() {
			return getRuleContexts(MultiplicativeArithmeticExpressionContext.class);
		}
		public MultiplicativeArithmeticExpressionContext multiplicativeArithmeticExpression(int i) {
			return getRuleContext(MultiplicativeArithmeticExpressionContext.class,i);
		}
		public ArithmeticExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterArithmeticExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitArithmeticExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitArithmeticExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticExpressionContext arithmeticExpression() throws RecognitionException {
		ArithmeticExpressionContext _localctx = new ArithmeticExpressionContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_arithmeticExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(534);
			((ArithmeticExpressionContext)_localctx).mae1 = multiplicativeArithmeticExpression();
			setState(539);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__22 || _la==T__23) {
				{
				{
				setState(535);
				((ArithmeticExpressionContext)_localctx).p = _input.LT(1);
				_la = _input.LA(1);
				if ( !(_la==T__22 || _la==T__23) ) {
					((ArithmeticExpressionContext)_localctx).p = (Token)_errHandler.recoverInline(this);
				} else {
					consume();
				}
				setState(536);
				((ArithmeticExpressionContext)_localctx).mae2 = multiplicativeArithmeticExpression();
				}
				}
				setState(541);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticTermContext extends ParserRuleContext {
		public VariableContext v;
		public SignedDecimalContext dv;
		public SignedIntegerContext iv;
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public SignedDecimalContext signedDecimal() {
			return getRuleContext(SignedDecimalContext.class,0);
		}
		public SignedIntegerContext signedInteger() {
			return getRuleContext(SignedIntegerContext.class,0);
		}
		public ArithmeticTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterArithmeticTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitArithmeticTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitArithmeticTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticTermContext arithmeticTerm() throws RecognitionException {
		ArithmeticTermContext _localctx = new ArithmeticTermContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_arithmeticTerm);
		try {
			setState(545);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,48,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(542);
				((ArithmeticTermContext)_localctx).v = variable();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(543);
				((ArithmeticTermContext)_localctx).dv = signedDecimal();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(544);
				((ArithmeticTermContext)_localctx).iv = signedInteger();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NonArithmeticTermContext extends ParserRuleContext {
		public AnyStringContext s;
		public FunctorTermContext ft;
		public ListTermContext lt;
		public AnyStringContext anyString() {
			return getRuleContext(AnyStringContext.class,0);
		}
		public FunctorTermContext functorTerm() {
			return getRuleContext(FunctorTermContext.class,0);
		}
		public ListTermContext listTerm() {
			return getRuleContext(ListTermContext.class,0);
		}
		public NonArithmeticTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nonArithmeticTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterNonArithmeticTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitNonArithmeticTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitNonArithmeticTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NonArithmeticTermContext nonArithmeticTerm() throws RecognitionException {
		NonArithmeticTermContext _localctx = new NonArithmeticTermContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_nonArithmeticTerm);
		try {
			setState(550);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,49,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(547);
				((NonArithmeticTermContext)_localctx).s = anyString();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(548);
				((NonArithmeticTermContext)_localctx).ft = functorTerm();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(549);
				((NonArithmeticTermContext)_localctx).lt = listTerm();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctorTermContext extends ParserRuleContext {
		public Token n;
		public List<TermContext> term() {
			return getRuleContexts(TermContext.class);
		}
		public TermContext term(int i) {
			return getRuleContext(TermContext.class,i);
		}
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public FunctorTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functorTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterFunctorTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitFunctorTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitFunctorTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctorTermContext functorTerm() throws RecognitionException {
		FunctorTermContext _localctx = new FunctorTermContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_functorTerm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(553);
			_la = _input.LA(1);
			if (_la==IDENTIFIER) {
				{
				setState(552);
				((FunctorTermContext)_localctx).n = match(IDENTIFIER);
				}
			}

			setState(555);
			match(T__1);
			setState(556);
			term();
			setState(561);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(557);
				match(T__3);
				setState(558);
				term();
				}
				}
				setState(563);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(564);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListTermContext extends ParserRuleContext {
		public ListTermArgumentsContext listTermArguments() {
			return getRuleContext(ListTermArgumentsContext.class,0);
		}
		public ListTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterListTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitListTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitListTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListTermContext listTerm() throws RecognitionException {
		ListTermContext _localctx = new ListTermContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_listTerm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(566);
			match(T__6);
			setState(568);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << T__6) | (1L << T__22) | (1L << T__25) | (1L << STRING) | (1L << DATA_TYPE) | (1L << SORT_ORDER) | (1L << MODULE) | (1L << DATABASE) | (1L << EXPORT) | (1L << INDEX) | (1L << KEY) | (1L << IF) | (1L << THEN) | (1L << ELSE) | (1L << TRUE) | (1L << FALSE) | (1L << BEGIN) | (1L << END) | (1L << CHOICE) | (1L << MULTI) | (1L << SINGLE) | (1L << EMPTY) | (1L << RETURN) | (1L << MOD) | (1L << OPC) | (1L << LOG) | (1L << EXP) | (1L << STEP) | (1L << AGGREGATE_FSMAX) | (1L << AGGREGATE_FSMIN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (AGGREGATE_FSCNT - 64)) | (1L << (AGGREGATE_FSSUM - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (VARIABLENAME - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)))) != 0)) {
				{
				setState(567);
				listTermArguments();
				}
			}

			setState(570);
			match(T__7);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListTermArgumentsContext extends ParserRuleContext {
		public TermContext term() {
			return getRuleContext(TermContext.class,0);
		}
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public ListTermArgumentsContext listTermArguments() {
			return getRuleContext(ListTermArgumentsContext.class,0);
		}
		public ListTermArgumentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listTermArguments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterListTermArguments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitListTermArguments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitListTermArguments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ListTermArgumentsContext listTermArguments() throws RecognitionException {
		ListTermArgumentsContext _localctx = new ListTermArgumentsContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_listTermArguments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(572);
			term();
			setState(583);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,54,_ctx) ) {
			case 1:
				{
				setState(573);
				match(T__8);
				setState(574);
				variable();
				}
				break;
			case 2:
				{
				setState(575);
				match(T__8);
				setState(576);
				match(T__6);
				setState(578);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << T__6) | (1L << T__22) | (1L << T__25) | (1L << STRING) | (1L << DATA_TYPE) | (1L << SORT_ORDER) | (1L << MODULE) | (1L << DATABASE) | (1L << EXPORT) | (1L << INDEX) | (1L << KEY) | (1L << IF) | (1L << THEN) | (1L << ELSE) | (1L << TRUE) | (1L << FALSE) | (1L << BEGIN) | (1L << END) | (1L << CHOICE) | (1L << MULTI) | (1L << SINGLE) | (1L << EMPTY) | (1L << RETURN) | (1L << MOD) | (1L << OPC) | (1L << LOG) | (1L << EXP) | (1L << STEP) | (1L << AGGREGATE_FSMAX) | (1L << AGGREGATE_FSMIN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (AGGREGATE_FSCNT - 64)) | (1L << (AGGREGATE_FSSUM - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (VARIABLENAME - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)))) != 0)) {
					{
					setState(577);
					listTermArguments();
					}
				}

				setState(580);
				match(T__7);
				}
				break;
			case 3:
				{
				setState(581);
				match(T__3);
				setState(582);
				listTermArguments();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryFormContext extends ParserRuleContext {
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public QueryFormArgumentsContext queryFormArguments() {
			return getRuleContext(QueryFormArgumentsContext.class,0);
		}
		public QueryFormContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryForm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterQueryForm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitQueryForm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitQueryForm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryFormContext queryForm() throws RecognitionException {
		QueryFormContext _localctx = new QueryFormContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_queryForm);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(585);
			match(IDENTIFIER);
			setState(591);
			_la = _input.LA(1);
			if (_la==T__1) {
				{
				setState(586);
				match(T__1);
				setState(588);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << T__6) | (1L << T__22) | (1L << T__25) | (1L << STRING) | (1L << DATA_TYPE) | (1L << SORT_ORDER) | (1L << MODULE) | (1L << DATABASE) | (1L << EXPORT) | (1L << INDEX) | (1L << KEY) | (1L << IF) | (1L << THEN) | (1L << ELSE) | (1L << TRUE) | (1L << FALSE) | (1L << BEGIN) | (1L << END) | (1L << CHOICE) | (1L << MULTI) | (1L << SINGLE) | (1L << EMPTY) | (1L << RETURN) | (1L << MOD) | (1L << OPC) | (1L << LOG) | (1L << EXP) | (1L << STEP) | (1L << AGGREGATE_FSMAX) | (1L << AGGREGATE_FSMIN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (AGGREGATE_FSCNT - 64)) | (1L << (AGGREGATE_FSSUM - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (VARIABLENAME - 64)) | (1L << (INPUT_VARIABLE - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)))) != 0)) {
					{
					setState(587);
					queryFormArguments();
					}
				}

				setState(590);
				match(T__5);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryFormArgumentsContext extends ParserRuleContext {
		public List<QueryFormTermContext> queryFormTerm() {
			return getRuleContexts(QueryFormTermContext.class);
		}
		public QueryFormTermContext queryFormTerm(int i) {
			return getRuleContext(QueryFormTermContext.class,i);
		}
		public QueryFormArgumentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryFormArguments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterQueryFormArguments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitQueryFormArguments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitQueryFormArguments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryFormArgumentsContext queryFormArguments() throws RecognitionException {
		QueryFormArgumentsContext _localctx = new QueryFormArgumentsContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_queryFormArguments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(593);
			queryFormTerm();
			setState(598);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(594);
				match(T__3);
				setState(595);
				queryFormTerm();
				}
				}
				setState(600);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class QueryFormTermContext extends ParserRuleContext {
		public Token fn;
		public QueryFormArgumentsContext qfa;
		public QueryFormArgumentsContext qfaList;
		public VariableContext vt;
		public Token ivt;
		public SignedDecimalContext dv;
		public SignedIntegerContext iv;
		public AnyStringContext s;
		public QueryFormArgumentsContext queryFormArguments() {
			return getRuleContext(QueryFormArgumentsContext.class,0);
		}
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public VariableContext variable() {
			return getRuleContext(VariableContext.class,0);
		}
		public TerminalNode INPUT_VARIABLE() { return getToken(DeALParser.INPUT_VARIABLE, 0); }
		public SignedDecimalContext signedDecimal() {
			return getRuleContext(SignedDecimalContext.class,0);
		}
		public SignedIntegerContext signedInteger() {
			return getRuleContext(SignedIntegerContext.class,0);
		}
		public AnyStringContext anyString() {
			return getRuleContext(AnyStringContext.class,0);
		}
		public QueryFormTermContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_queryFormTerm; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterQueryFormTerm(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitQueryFormTerm(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitQueryFormTerm(this);
			else return visitor.visitChildren(this);
		}
	}

	public final QueryFormTermContext queryFormTerm() throws RecognitionException {
		QueryFormTermContext _localctx = new QueryFormTermContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_queryFormTerm);
		int _la;
		try {
			setState(618);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,60,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(602);
				_la = _input.LA(1);
				if (_la==IDENTIFIER) {
					{
					setState(601);
					((QueryFormTermContext)_localctx).fn = match(IDENTIFIER);
					}
				}

				setState(604);
				match(T__1);
				setState(605);
				((QueryFormTermContext)_localctx).qfa = queryFormArguments();
				setState(606);
				match(T__5);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(608);
				match(T__6);
				setState(610);
				_la = _input.LA(1);
				if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__1) | (1L << T__6) | (1L << T__22) | (1L << T__25) | (1L << STRING) | (1L << DATA_TYPE) | (1L << SORT_ORDER) | (1L << MODULE) | (1L << DATABASE) | (1L << EXPORT) | (1L << INDEX) | (1L << KEY) | (1L << IF) | (1L << THEN) | (1L << ELSE) | (1L << TRUE) | (1L << FALSE) | (1L << BEGIN) | (1L << END) | (1L << CHOICE) | (1L << MULTI) | (1L << SINGLE) | (1L << EMPTY) | (1L << RETURN) | (1L << MOD) | (1L << OPC) | (1L << LOG) | (1L << EXP) | (1L << STEP) | (1L << AGGREGATE_FSMAX) | (1L << AGGREGATE_FSMIN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (AGGREGATE_FSCNT - 64)) | (1L << (AGGREGATE_FSSUM - 64)) | (1L << (IDENTIFIER - 64)) | (1L << (VARIABLENAME - 64)) | (1L << (INPUT_VARIABLE - 64)) | (1L << (INTEGER - 64)) | (1L << (DECIMAL - 64)))) != 0)) {
					{
					setState(609);
					((QueryFormTermContext)_localctx).qfaList = queryFormArguments();
					}
				}

				setState(612);
				match(T__7);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(613);
				((QueryFormTermContext)_localctx).vt = variable();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(614);
				((QueryFormTermContext)_localctx).ivt = match(INPUT_VARIABLE);
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(615);
				((QueryFormTermContext)_localctx).dv = signedDecimal();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(616);
				((QueryFormTermContext)_localctx).iv = signedInteger();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(617);
				((QueryFormTermContext)_localctx).s = anyString();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NameContext extends ParserRuleContext {
		public TerminalNode VARIABLENAME() { return getToken(DeALParser.VARIABLENAME, 0); }
		public TerminalNode IDENTIFIER() { return getToken(DeALParser.IDENTIFIER, 0); }
		public KeywordContext keyword() {
			return getRuleContext(KeywordContext.class,0);
		}
		public NameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NameContext name() throws RecognitionException {
		NameContext _localctx = new NameContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_name);
		try {
			setState(623);
			switch (_input.LA(1)) {
			case VARIABLENAME:
				enterOuterAlt(_localctx, 1);
				{
				setState(620);
				match(VARIABLENAME);
				}
				break;
			case IDENTIFIER:
				enterOuterAlt(_localctx, 2);
				{
				setState(621);
				match(IDENTIFIER);
				}
				break;
			case DATA_TYPE:
			case SORT_ORDER:
			case MODULE:
			case DATABASE:
			case EXPORT:
			case INDEX:
			case KEY:
			case IF:
			case THEN:
			case ELSE:
			case TRUE:
			case FALSE:
			case BEGIN:
			case END:
			case CHOICE:
			case MULTI:
			case SINGLE:
			case EMPTY:
			case RETURN:
			case MOD:
			case OPC:
			case LOG:
			case EXP:
			case STEP:
			case AGGREGATE_FSMAX:
			case AGGREGATE_FSMIN:
			case AGGREGATE_FSCNT:
			case AGGREGATE_FSSUM:
				enterOuterAlt(_localctx, 3);
				{
				setState(622);
				keyword();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnyStringContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(DeALParser.STRING, 0); }
		public NameContext name() {
			return getRuleContext(NameContext.class,0);
		}
		public AnyStringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anyString; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterAnyString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitAnyString(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitAnyString(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnyStringContext anyString() throws RecognitionException {
		AnyStringContext _localctx = new AnyStringContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_anyString);
		try {
			setState(627);
			switch (_input.LA(1)) {
			case STRING:
				enterOuterAlt(_localctx, 1);
				{
				setState(625);
				match(STRING);
				}
				break;
			case DATA_TYPE:
			case SORT_ORDER:
			case MODULE:
			case DATABASE:
			case EXPORT:
			case INDEX:
			case KEY:
			case IF:
			case THEN:
			case ELSE:
			case TRUE:
			case FALSE:
			case BEGIN:
			case END:
			case CHOICE:
			case MULTI:
			case SINGLE:
			case EMPTY:
			case RETURN:
			case MOD:
			case OPC:
			case LOG:
			case EXP:
			case STEP:
			case AGGREGATE_FSMAX:
			case AGGREGATE_FSMIN:
			case AGGREGATE_FSCNT:
			case AGGREGATE_FSSUM:
			case IDENTIFIER:
			case VARIABLENAME:
				enterOuterAlt(_localctx, 2);
				{
				setState(626);
				name();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AnnotationsContext extends ParserRuleContext {
		public AnyStringContext anyString() {
			return getRuleContext(AnyStringContext.class,0);
		}
		public AnnotationsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_annotations; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterAnnotations(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitAnnotations(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitAnnotations(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AnnotationsContext annotations() throws RecognitionException {
		AnnotationsContext _localctx = new AnnotationsContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_annotations);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(629);
			match(T__24);
			setState(630);
			anyString();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VariablesContext extends ParserRuleContext {
		public List<VariableContext> variable() {
			return getRuleContexts(VariableContext.class);
		}
		public VariableContext variable(int i) {
			return getRuleContext(VariableContext.class,i);
		}
		public VariablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterVariables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitVariables(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitVariables(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariablesContext variables() throws RecognitionException {
		VariablesContext _localctx = new VariablesContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_variables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(632);
			variable();
			setState(637);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__3) {
				{
				{
				setState(633);
				match(T__3);
				setState(634);
				variable();
				}
				}
				setState(639);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class VariableContext extends ParserRuleContext {
		public TerminalNode VARIABLENAME() { return getToken(DeALParser.VARIABLENAME, 0); }
		public VariableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_variable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterVariable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitVariable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitVariable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final VariableContext variable() throws RecognitionException {
		VariableContext _localctx = new VariableContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_variable);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(640);
			_la = _input.LA(1);
			if ( !(_la==T__25 || _la==VARIABLENAME) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UdaPredicatesContext extends ParserRuleContext {
		public TerminalNode EMPTY() { return getToken(DeALParser.EMPTY, 0); }
		public TerminalNode SINGLE() { return getToken(DeALParser.SINGLE, 0); }
		public TerminalNode MULTI() { return getToken(DeALParser.MULTI, 0); }
		public TerminalNode RETURN() { return getToken(DeALParser.RETURN, 0); }
		public UdaPredicatesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_udaPredicates; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterUdaPredicates(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitUdaPredicates(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitUdaPredicates(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UdaPredicatesContext udaPredicates() throws RecognitionException {
		UdaPredicatesContext _localctx = new UdaPredicatesContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_udaPredicates);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(642);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MULTI) | (1L << SINGLE) | (1L << EMPTY) | (1L << RETURN))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeywordContext extends ParserRuleContext {
		public TerminalNode EMPTY() { return getToken(DeALParser.EMPTY, 0); }
		public TerminalNode SINGLE() { return getToken(DeALParser.SINGLE, 0); }
		public TerminalNode MULTI() { return getToken(DeALParser.MULTI, 0); }
		public TerminalNode RETURN() { return getToken(DeALParser.RETURN, 0); }
		public TerminalNode AGGREGATE_FSMAX() { return getToken(DeALParser.AGGREGATE_FSMAX, 0); }
		public TerminalNode AGGREGATE_FSMIN() { return getToken(DeALParser.AGGREGATE_FSMIN, 0); }
		public TerminalNode AGGREGATE_FSCNT() { return getToken(DeALParser.AGGREGATE_FSCNT, 0); }
		public TerminalNode AGGREGATE_FSSUM() { return getToken(DeALParser.AGGREGATE_FSSUM, 0); }
		public TerminalNode DATA_TYPE() { return getToken(DeALParser.DATA_TYPE, 0); }
		public TerminalNode SORT_ORDER() { return getToken(DeALParser.SORT_ORDER, 0); }
		public TerminalNode MOD() { return getToken(DeALParser.MOD, 0); }
		public TerminalNode OPC() { return getToken(DeALParser.OPC, 0); }
		public TerminalNode LOG() { return getToken(DeALParser.LOG, 0); }
		public TerminalNode EXP() { return getToken(DeALParser.EXP, 0); }
		public TerminalNode STEP() { return getToken(DeALParser.STEP, 0); }
		public TerminalNode MODULE() { return getToken(DeALParser.MODULE, 0); }
		public TerminalNode DATABASE() { return getToken(DeALParser.DATABASE, 0); }
		public TerminalNode EXPORT() { return getToken(DeALParser.EXPORT, 0); }
		public TerminalNode INDEX() { return getToken(DeALParser.INDEX, 0); }
		public TerminalNode KEY() { return getToken(DeALParser.KEY, 0); }
		public TerminalNode IF() { return getToken(DeALParser.IF, 0); }
		public TerminalNode THEN() { return getToken(DeALParser.THEN, 0); }
		public TerminalNode ELSE() { return getToken(DeALParser.ELSE, 0); }
		public TerminalNode TRUE() { return getToken(DeALParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(DeALParser.FALSE, 0); }
		public TerminalNode BEGIN() { return getToken(DeALParser.BEGIN, 0); }
		public TerminalNode END() { return getToken(DeALParser.END, 0); }
		public TerminalNode CHOICE() { return getToken(DeALParser.CHOICE, 0); }
		public KeywordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyword; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterKeyword(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitKeyword(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitKeyword(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeywordContext keyword() throws RecognitionException {
		KeywordContext _localctx = new KeywordContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_keyword);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(644);
			_la = _input.LA(1);
			if ( !(((((_la - 32)) & ~0x3f) == 0 && ((1L << (_la - 32)) & ((1L << (DATA_TYPE - 32)) | (1L << (SORT_ORDER - 32)) | (1L << (MODULE - 32)) | (1L << (DATABASE - 32)) | (1L << (EXPORT - 32)) | (1L << (INDEX - 32)) | (1L << (KEY - 32)) | (1L << (IF - 32)) | (1L << (THEN - 32)) | (1L << (ELSE - 32)) | (1L << (TRUE - 32)) | (1L << (FALSE - 32)) | (1L << (BEGIN - 32)) | (1L << (END - 32)) | (1L << (CHOICE - 32)) | (1L << (MULTI - 32)) | (1L << (SINGLE - 32)) | (1L << (EMPTY - 32)) | (1L << (RETURN - 32)) | (1L << (MOD - 32)) | (1L << (OPC - 32)) | (1L << (LOG - 32)) | (1L << (EXP - 32)) | (1L << (STEP - 32)) | (1L << (AGGREGATE_FSMAX - 32)) | (1L << (AGGREGATE_FSMIN - 32)) | (1L << (AGGREGATE_FSCNT - 32)) | (1L << (AGGREGATE_FSSUM - 32)))) != 0)) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SignedIntegerContext extends ParserRuleContext {
		public TerminalNode INTEGER() { return getToken(DeALParser.INTEGER, 0); }
		public SignedIntegerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signedInteger; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterSignedInteger(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitSignedInteger(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitSignedInteger(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SignedIntegerContext signedInteger() throws RecognitionException {
		SignedIntegerContext _localctx = new SignedIntegerContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_signedInteger);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(647);
			_la = _input.LA(1);
			if (_la==T__22) {
				{
				setState(646);
				match(T__22);
				}
			}

			setState(649);
			match(INTEGER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SignedDecimalContext extends ParserRuleContext {
		public TerminalNode DECIMAL() { return getToken(DeALParser.DECIMAL, 0); }
		public SignedDecimalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signedDecimal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).enterSignedDecimal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof DeALListener ) ((DeALListener)listener).exitSignedDecimal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof DeALVisitor ) return ((DeALVisitor<? extends T>)visitor).visitSignedDecimal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SignedDecimalContext signedDecimal() throws RecognitionException {
		SignedDecimalContext _localctx = new SignedDecimalContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_signedDecimal);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(652);
			_la = _input.LA(1);
			if (_la==T__22) {
				{
				setState(651);
				match(T__22);
				}
			}

			setState(654);
			match(DECIMAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3H\u0293\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\3\2\3\2\5\2o\n\2\3\2\3\2\3\3\3\3\3\3\3\3\3\4\3"+
		"\4\3\4\3\5\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7\3\7\3\b\7\b\u0085\n\b\f\b\16"+
		"\b\u0088\13\b\3\b\3\b\3\t\3\t\5\t\u008e\n\t\3\t\6\t\u0091\n\t\r\t\16\t"+
		"\u0092\5\t\u0095\n\t\3\n\3\n\3\n\3\n\3\n\5\n\u009c\n\n\3\13\3\13\3\13"+
		"\3\13\3\13\6\13\u00a3\n\13\r\13\16\13\u00a4\3\13\3\13\3\13\3\13\3\f\3"+
		"\f\3\f\3\f\3\f\3\f\7\f\u00b1\n\f\f\f\16\f\u00b4\13\f\3\f\3\f\3\f\3\f\3"+
		"\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\7\16\u00c3\n\16\f\16\16\16\u00c6"+
		"\13\16\3\17\5\17\u00c9\n\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3\17\3"+
		"\17\3\17\3\17\5\17\u00d6\n\17\3\20\3\20\5\20\u00da\n\20\3\21\3\21\3\21"+
		"\3\21\3\21\5\21\u00e1\n\21\3\21\3\21\3\21\5\21\u00e6\n\21\3\22\3\22\3"+
		"\22\3\22\3\22\7\22\u00ed\n\22\f\22\16\22\u00f0\13\22\3\22\3\22\3\23\3"+
		"\23\3\23\5\23\u00f7\n\23\3\23\3\23\5\23\u00fb\n\23\3\23\3\23\3\23\3\23"+
		"\7\23\u0101\n\23\f\23\16\23\u0104\13\23\3\23\3\23\3\23\3\23\3\23\5\23"+
		"\u010b\n\23\3\23\3\23\3\23\3\23\7\23\u0111\n\23\f\23\16\23\u0114\13\23"+
		"\3\23\3\23\5\23\u0118\n\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3\24\7\24"+
		"\u0122\n\24\f\24\16\24\u0125\13\24\3\24\3\24\3\24\3\24\3\25\3\25\3\25"+
		"\3\25\3\26\3\26\3\26\5\26\u0132\n\26\3\26\3\26\5\26\u0136\n\26\3\27\3"+
		"\27\3\27\3\27\3\27\3\27\3\27\7\27\u013f\n\27\f\27\16\27\u0142\13\27\3"+
		"\27\3\27\3\27\5\27\u0147\n\27\3\30\3\30\3\30\7\30\u014c\n\30\f\30\16\30"+
		"\u014f\13\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\5\31\u015a\n"+
		"\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\5"+
		"\31\u0169\n\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31"+
		"\3\31\3\31\3\31\3\31\7\31\u017a\n\31\f\31\16\31\u017d\13\31\3\31\3\31"+
		"\3\31\3\31\3\31\3\31\3\31\5\31\u0186\n\31\3\31\5\31\u0189\n\31\3\32\3"+
		"\32\3\32\3\32\3\32\7\32\u0190\n\32\f\32\16\32\u0193\13\32\3\32\3\32\5"+
		"\32\u0197\n\32\3\32\3\32\3\32\5\32\u019c\n\32\3\33\3\33\3\34\3\34\3\34"+
		"\3\34\7\34\u01a4\n\34\f\34\16\34\u01a7\13\34\5\34\u01a9\n\34\3\34\3\34"+
		"\3\35\3\35\3\35\3\35\3\35\7\35\u01b2\n\35\f\35\16\35\u01b5\13\35\3\35"+
		"\3\35\5\35\u01b9\n\35\3\36\3\36\3\36\3\36\5\36\u01bf\n\36\3\37\3\37\3"+
		"\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u01d0"+
		"\n\37\3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3"+
		" \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \3 \5 \u01f7\n \3!\3!\5!\u01fb"+
		"\n!\3\"\3\"\3\"\3\"\3\"\5\"\u0202\n\"\3#\3#\3#\3#\3#\3#\3#\5#\u020b\n"+
		"#\3$\3$\3$\7$\u0210\n$\f$\16$\u0213\13$\3$\3$\5$\u0217\n$\3%\3%\3%\7%"+
		"\u021c\n%\f%\16%\u021f\13%\3&\3&\3&\5&\u0224\n&\3\'\3\'\3\'\5\'\u0229"+
		"\n\'\3(\5(\u022c\n(\3(\3(\3(\3(\7(\u0232\n(\f(\16(\u0235\13(\3(\3(\3)"+
		"\3)\5)\u023b\n)\3)\3)\3*\3*\3*\3*\3*\3*\5*\u0245\n*\3*\3*\3*\5*\u024a"+
		"\n*\3+\3+\3+\5+\u024f\n+\3+\5+\u0252\n+\3,\3,\3,\7,\u0257\n,\f,\16,\u025a"+
		"\13,\3-\5-\u025d\n-\3-\3-\3-\3-\3-\3-\5-\u0265\n-\3-\3-\3-\3-\3-\3-\5"+
		"-\u026d\n-\3.\3.\3.\5.\u0272\n.\3/\3/\5/\u0276\n/\3\60\3\60\3\60\3\61"+
		"\3\61\3\61\7\61\u027e\n\61\f\61\16\61\u0281\13\61\3\62\3\62\3\63\3\63"+
		"\3\64\3\64\3\65\5\65\u028a\n\65\3\65\3\65\3\66\5\66\u028f\n\66\3\66\3"+
		"\66\3\66\2\2\67\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*,.\60\62"+
		"\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhj\2\13\3\2\'(\3\2\17\26\3\2@A\3\2BC\5"+
		"\2\27\30:;??\3\2\31\32\4\2\34\34EE\3\2\62\65\6\2\"-/\65:>@C\u02c0\2l\3"+
		"\2\2\2\4r\3\2\2\2\6v\3\2\2\2\by\3\2\2\2\n}\3\2\2\2\f\u0080\3\2\2\2\16"+
		"\u0086\3\2\2\2\20\u0094\3\2\2\2\22\u009b\3\2\2\2\24\u009d\3\2\2\2\26\u00aa"+
		"\3\2\2\2\30\u00b9\3\2\2\2\32\u00bf\3\2\2\2\34\u00d5\3\2\2\2\36\u00d7\3"+
		"\2\2\2 \u00e5\3\2\2\2\"\u00e7\3\2\2\2$\u0117\3\2\2\2&\u0119\3\2\2\2(\u012a"+
		"\3\2\2\2*\u012e\3\2\2\2,\u0146\3\2\2\2.\u0148\3\2\2\2\60\u0188\3\2\2\2"+
		"\62\u019b\3\2\2\2\64\u019d\3\2\2\2\66\u019f\3\2\2\28\u01ac\3\2\2\2:\u01be"+
		"\3\2\2\2<\u01cf\3\2\2\2>\u01f6\3\2\2\2@\u01fa\3\2\2\2B\u0201\3\2\2\2D"+
		"\u020a\3\2\2\2F\u0216\3\2\2\2H\u0218\3\2\2\2J\u0223\3\2\2\2L\u0228\3\2"+
		"\2\2N\u022b\3\2\2\2P\u0238\3\2\2\2R\u023e\3\2\2\2T\u024b\3\2\2\2V\u0253"+
		"\3\2\2\2X\u026c\3\2\2\2Z\u0271\3\2\2\2\\\u0275\3\2\2\2^\u0277\3\2\2\2"+
		"`\u027a\3\2\2\2b\u0282\3\2\2\2d\u0284\3\2\2\2f\u0286\3\2\2\2h\u0289\3"+
		"\2\2\2j\u028e\3\2\2\2ln\5T+\2mo\7\3\2\2nm\3\2\2\2no\3\2\2\2op\3\2\2\2"+
		"pq\7\2\2\3q\3\3\2\2\2rs\5\62\32\2st\7\3\2\2tu\7\2\2\3u\5\3\2\2\2vw\5*"+
		"\26\2wx\7\2\2\3x\7\3\2\2\2yz\5\34\17\2z{\7\3\2\2{|\7\2\2\3|\t\3\2\2\2"+
		"}~\5\30\r\2~\177\7\2\2\3\177\13\3\2\2\2\u0080\u0081\5\20\t\2\u0081\u0082"+
		"\7\2\2\3\u0082\r\3\2\2\2\u0083\u0085\5\30\r\2\u0084\u0083\3\2\2\2\u0085"+
		"\u0088\3\2\2\2\u0086\u0084\3\2\2\2\u0086\u0087\3\2\2\2\u0087\u0089\3\2"+
		"\2\2\u0088\u0086\3\2\2\2\u0089\u008a\7\2\2\3\u008a\17\3\2\2\2\u008b\u008d"+
		"\5\24\13\2\u008c\u008e\5\20\t\2\u008d\u008c\3\2\2\2\u008d\u008e\3\2\2"+
		"\2\u008e\u0095\3\2\2\2\u008f\u0091\5\22\n\2\u0090\u008f\3\2\2\2\u0091"+
		"\u0092\3\2\2\2\u0092\u0090\3\2\2\2\u0092\u0093\3\2\2\2\u0093\u0095\3\2"+
		"\2\2\u0094\u008b\3\2\2\2\u0094\u0090\3\2\2\2\u0095\21\3\2\2\2\u0096\u009c"+
		"\5\26\f\2\u0097\u009c\5&\24\2\u0098\u009c\5(\25\2\u0099\u009c\5\30\r\2"+
		"\u009a\u009c\5*\26\2\u009b\u0096\3\2\2\2\u009b\u0097\3\2\2\2\u009b\u0098"+
		"\3\2\2\2\u009b\u0099\3\2\2\2\u009b\u009a\3\2\2\2\u009c\23\3\2\2\2\u009d"+
		"\u009e\7/\2\2\u009e\u009f\7$\2\2\u009f\u00a0\5\\/\2\u00a0\u00a2\7\3\2"+
		"\2\u00a1\u00a3\5\22\n\2\u00a2\u00a1\3\2\2\2\u00a3\u00a4\3\2\2\2\u00a4"+
		"\u00a2\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00a6\3\2\2\2\u00a6\u00a7\7\60"+
		"\2\2\u00a7\u00a8\7$\2\2\u00a8\u00a9\7\3\2\2\u00a9\25\3\2\2\2\u00aa\u00ab"+
		"\7%\2\2\u00ab\u00ac\7\4\2\2\u00ac\u00ad\7\5\2\2\u00ad\u00b2\5\"\22\2\u00ae"+
		"\u00af\7\6\2\2\u00af\u00b1\5\"\22\2\u00b0\u00ae\3\2\2\2\u00b1\u00b4\3"+
		"\2\2\2\u00b2\u00b0\3\2\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b5\3\2\2\2\u00b4"+
		"\u00b2\3\2\2\2\u00b5\u00b6\7\7\2\2\u00b6\u00b7\7\b\2\2\u00b7\u00b8\7\3"+
		"\2\2\u00b8\27\3\2\2\2\u00b9\u00ba\7D\2\2\u00ba\u00bb\7\4\2\2\u00bb\u00bc"+
		"\5\32\16\2\u00bc\u00bd\7\b\2\2\u00bd\u00be\7\3\2\2\u00be\31\3\2\2\2\u00bf"+
		"\u00c4\5\34\17\2\u00c0\u00c1\7\6\2\2\u00c1\u00c3\5\34\17\2\u00c2\u00c0"+
		"\3\2\2\2\u00c3\u00c6\3\2\2\2\u00c4\u00c2\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5"+
		"\33\3\2\2\2\u00c6\u00c4\3\2\2\2\u00c7\u00c9\7D\2\2\u00c8\u00c7\3\2\2\2"+
		"\u00c8\u00c9\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca\u00cb\7\4\2\2\u00cb\u00cc"+
		"\5\32\16\2\u00cc\u00cd\7\b\2\2\u00cd\u00d6\3\2\2\2\u00ce\u00cf\7\t\2\2"+
		"\u00cf\u00d0\5\36\20\2\u00d0\u00d1\7\n\2\2\u00d1\u00d6\3\2\2\2\u00d2\u00d6"+
		"\5j\66\2\u00d3\u00d6\5h\65\2\u00d4\u00d6\5\\/\2\u00d5\u00c8\3\2\2\2\u00d5"+
		"\u00ce\3\2\2\2\u00d5\u00d2\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d5\u00d4\3\2"+
		"\2\2\u00d6\35\3\2\2\2\u00d7\u00d9\5\34\17\2\u00d8\u00da\5 \21\2\u00d9"+
		"\u00d8\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\37\3\2\2\2\u00db\u00dc\7\13\2"+
		"\2\u00dc\u00e6\5\34\17\2\u00dd\u00de\7\13\2\2\u00de\u00e0\7\t\2\2\u00df"+
		"\u00e1\5\36\20\2\u00e0\u00df\3\2\2\2\u00e0\u00e1\3\2\2\2\u00e1\u00e2\3"+
		"\2\2\2\u00e2\u00e6\7\n\2\2\u00e3\u00e4\7\6\2\2\u00e4\u00e6\5\36\20\2\u00e5"+
		"\u00db\3\2\2\2\u00e5\u00dd\3\2\2\2\u00e5\u00e3\3\2\2\2\u00e6!\3\2\2\2"+
		"\u00e7\u00e8\7D\2\2\u00e8\u00e9\7\4\2\2\u00e9\u00ee\5$\23\2\u00ea\u00eb"+
		"\7\6\2\2\u00eb\u00ed\5$\23\2\u00ec\u00ea\3\2\2\2\u00ed\u00f0\3\2\2\2\u00ee"+
		"\u00ec\3\2\2\2\u00ee\u00ef\3\2\2\2\u00ef\u00f1\3\2\2\2\u00f0\u00ee\3\2"+
		"\2\2\u00f1\u00f2\7\b\2\2\u00f2#\3\2\2\2\u00f3\u00f4\5Z.\2\u00f4\u00f5"+
		"\7\f\2\2\u00f5\u00f7\3\2\2\2\u00f6\u00f3\3\2\2\2\u00f6\u00f7\3\2\2\2\u00f7"+
		"\u00f8\3\2\2\2\u00f8\u0118\7\"\2\2\u00f9\u00fb\5Z.\2\u00fa\u00f9\3\2\2"+
		"\2\u00fa\u00fb\3\2\2\2\u00fb\u00fc\3\2\2\2\u00fc\u00fd\7\4\2\2\u00fd\u0102"+
		"\5$\23\2\u00fe\u00ff\7\6\2\2\u00ff\u0101\5$\23\2\u0100\u00fe\3\2\2\2\u0101"+
		"\u0104\3\2\2\2\u0102\u0100\3\2\2\2\u0102\u0103\3\2\2\2\u0103\u0105\3\2"+
		"\2\2\u0104\u0102\3\2\2\2\u0105\u0106\7\b\2\2\u0106\u0118\3\2\2\2\u0107"+
		"\u0108\5Z.\2\u0108\u0109\7\f\2\2\u0109\u010b\3\2\2\2\u010a\u0107\3\2\2"+
		"\2\u010a\u010b\3\2\2\2\u010b\u010c\3\2\2\2\u010c\u010d\7\t\2\2\u010d\u0112"+
		"\5$\23\2\u010e\u010f\7\6\2\2\u010f\u0111\5$\23\2\u0110\u010e\3\2\2\2\u0111"+
		"\u0114\3\2\2\2\u0112\u0110\3\2\2\2\u0112\u0113\3\2\2\2\u0113\u0115\3\2"+
		"\2\2\u0114\u0112\3\2\2\2\u0115\u0116\7\n\2\2\u0116\u0118\3\2\2\2\u0117"+
		"\u00f6\3\2\2\2\u0117\u00fa\3\2\2\2\u0117\u010a\3\2\2\2\u0118%\3\2\2\2"+
		"\u0119\u011a\t\2\2\2\u011a\u011b\7\4\2\2\u011b\u011c\7D\2\2\u011c\u011d"+
		"\7\6\2\2\u011d\u011e\7\t\2\2\u011e\u0123\7G\2\2\u011f\u0120\7\6\2\2\u0120"+
		"\u0122\7G\2\2\u0121\u011f\3\2\2\2\u0122\u0125\3\2\2\2\u0123\u0121\3\2"+
		"\2\2\u0123\u0124\3\2\2\2\u0124\u0126\3\2\2\2\u0125\u0123\3\2\2\2\u0126"+
		"\u0127\7\n\2\2\u0127\u0128\7\b\2\2\u0128\u0129\7\3\2\2\u0129\'\3\2\2\2"+
		"\u012a\u012b\7&\2\2\u012b\u012c\5T+\2\u012c\u012d\7\3\2\2\u012d)\3\2\2"+
		"\2\u012e\u0131\5,\27\2\u012f\u0130\7\r\2\2\u0130\u0132\5.\30\2\u0131\u012f"+
		"\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0133\3\2\2\2\u0133\u0135\7\3\2\2\u0134"+
		"\u0136\5^\60\2\u0135\u0134\3\2\2\2\u0135\u0136\3\2\2\2\u0136+\3\2\2\2"+
		"\u0137\u0138\5d\63\2\u0138\u0139\7\4\2\2\u0139\u013a\7D\2\2\u013a\u013b"+
		"\7\6\2\2\u013b\u0140\5@!\2\u013c\u013d\7\6\2\2\u013d\u013f\5@!\2\u013e"+
		"\u013c\3\2\2\2\u013f\u0142\3\2\2\2\u0140\u013e\3\2\2\2\u0140\u0141\3\2"+
		"\2\2\u0141\u0143\3\2\2\2\u0142\u0140\3\2\2\2\u0143\u0144\7\b\2\2\u0144"+
		"\u0147\3\2\2\2\u0145\u0147\58\35\2\u0146\u0137\3\2\2\2\u0146\u0145\3\2"+
		"\2\2\u0147-\3\2\2\2\u0148\u014d\5\60\31\2\u0149\u014a\7\6\2\2\u014a\u014c"+
		"\5\60\31\2\u014b\u0149\3\2\2\2\u014c\u014f\3\2\2\2\u014d\u014b\3\2\2\2"+
		"\u014d\u014e\3\2\2\2\u014e/\3\2\2\2\u014f\u014d\3\2\2\2\u0150\u0189\7"+
		",\2\2\u0151\u0189\7-\2\2\u0152\u0153\7)\2\2\u0153\u0154\7\4\2\2\u0154"+
		"\u0155\5.\30\2\u0155\u0156\7*\2\2\u0156\u0159\5.\30\2\u0157\u0158\7+\2"+
		"\2\u0158\u015a\5.\30\2\u0159\u0157\3\2\2\2\u0159\u015a\3\2\2\2\u015a\u015b"+
		"\3\2\2\2\u015b\u015c\7\b\2\2\u015c\u0189\3\2\2\2\u015d\u015e\7\61\2\2"+
		"\u015e\u015f\7\4\2\2\u015f\u0160\5\66\34\2\u0160\u0161\7\6\2\2\u0161\u0162"+
		"\5\66\34\2\u0162\u0163\7\b\2\2\u0163\u0189\3\2\2\2\u0164\u0165\7\66\2"+
		"\2\u0165\u0168\7\4\2\2\u0166\u0169\5b\62\2\u0167\u0169\7G\2\2\u0168\u0166"+
		"\3\2\2\2\u0168\u0167\3\2\2\2\u0169\u016a\3\2\2\2\u016a\u0189\7\b\2\2\u016b"+
		"\u016c\7\67\2\2\u016c\u016d\7\4\2\2\u016d\u016e\7\4\2\2\u016e\u016f\5"+
		"b\62\2\u016f\u0170\7\6\2\2\u0170\u0171\7#\2\2\u0171\u017b\7\b\2\2\u0172"+
		"\u0173\7\6\2\2\u0173\u0174\7\4\2\2\u0174\u0175\5b\62\2\u0175\u0176\7\6"+
		"\2\2\u0176\u0177\7#\2\2\u0177\u0178\7\b\2\2\u0178\u017a\3\2\2\2\u0179"+
		"\u0172\3\2\2\2\u017a\u017d\3\2\2\2\u017b\u0179\3\2\2\2\u017b\u017c\3\2"+
		"\2\2\u017c\u017e\3\2\2\2\u017d\u017b\3\2\2\2\u017e\u017f\7\b\2\2\u017f"+
		"\u0189\3\2\2\2\u0180\u0181\5@!\2\u0181\u0182\5\64\33\2\u0182\u0183\5@"+
		"!\2\u0183\u0189\3\2\2\2\u0184\u0186\7\16\2\2\u0185\u0184\3\2\2\2\u0185"+
		"\u0186\3\2\2\2\u0186\u0187\3\2\2\2\u0187\u0189\5\62\32\2\u0188\u0150\3"+
		"\2\2\2\u0188\u0151\3\2\2\2\u0188\u0152\3\2\2\2\u0188\u015d\3\2\2\2\u0188"+
		"\u0164\3\2\2\2\u0188\u016b\3\2\2\2\u0188\u0180\3\2\2\2\u0188\u0185\3\2"+
		"\2\2\u0189\61\3\2\2\2\u018a\u0196\7D\2\2\u018b\u018c\7\4\2\2\u018c\u0191"+
		"\5@!\2\u018d\u018e\7\6\2\2\u018e\u0190\5@!\2\u018f\u018d\3\2\2\2\u0190"+
		"\u0193\3\2\2\2\u0191\u018f\3\2\2\2\u0191\u0192\3\2\2\2\u0192\u0194\3\2"+
		"\2\2\u0193\u0191\3\2\2\2\u0194\u0195\7\b\2\2\u0195\u0197\3\2\2\2\u0196"+
		"\u018b\3\2\2\2\u0196\u0197\3\2\2\2\u0197\u019c\3\2\2\2\u0198\u0199\7D"+
		"\2\2\u0199\u019a\7\4\2\2\u019a\u019c\7\b\2\2\u019b\u018a\3\2\2\2\u019b"+
		"\u0198\3\2\2\2\u019c\63\3\2\2\2\u019d\u019e\t\3\2\2\u019e\65\3\2\2\2\u019f"+
		"\u01a8\7\4\2\2\u01a0\u01a5\5b\62\2\u01a1\u01a2\7\6\2\2\u01a2\u01a4\5b"+
		"\62\2\u01a3\u01a1\3\2\2\2\u01a4\u01a7\3\2\2\2\u01a5\u01a3\3\2\2\2\u01a5"+
		"\u01a6\3\2\2\2\u01a6\u01a9\3\2\2\2\u01a7\u01a5\3\2\2\2\u01a8\u01a0\3\2"+
		"\2\2\u01a8\u01a9\3\2\2\2\u01a9\u01aa\3\2\2\2\u01aa\u01ab\7\b\2\2\u01ab"+
		"\67\3\2\2\2\u01ac\u01b8\7D\2\2\u01ad\u01ae\7\4\2\2\u01ae\u01b3\5:\36\2"+
		"\u01af\u01b0\7\6\2\2\u01b0\u01b2\5:\36\2\u01b1\u01af\3\2\2\2\u01b2\u01b5"+
		"\3\2\2\2\u01b3\u01b1\3\2\2\2\u01b3\u01b4\3\2\2\2\u01b4\u01b6\3\2\2\2\u01b5"+
		"\u01b3\3\2\2\2\u01b6\u01b7\7\b\2\2\u01b7\u01b9\3\2\2\2\u01b8\u01ad\3\2"+
		"\2\2\u01b8\u01b9\3\2\2\2\u01b99\3\2\2\2\u01ba\u01bf\5H%\2\u01bb\u01bf"+
		"\5<\37\2\u01bc\u01bf\5L\'\2\u01bd\u01bf\7.\2\2\u01be\u01ba\3\2\2\2\u01be"+
		"\u01bb\3\2\2\2\u01be\u01bc\3\2\2\2\u01be\u01bd\3\2\2\2\u01bf;\3\2\2\2"+
		"\u01c0\u01c1\7D\2\2\u01c1\u01c2\7\22\2\2\u01c2\u01c3\5@!\2\u01c3\u01c4"+
		"\7\21\2\2\u01c4\u01d0\3\2\2\2\u01c5\u01c6\t\4\2\2\u01c6\u01c7\7\22\2\2"+
		"\u01c7\u01c8\5b\62\2\u01c8\u01c9\7\21\2\2\u01c9\u01d0\3\2\2\2\u01ca\u01cb"+
		"\t\5\2\2\u01cb\u01cc\7\22\2\2\u01cc\u01cd\5> \2\u01cd\u01ce\7\21\2\2\u01ce"+
		"\u01d0\3\2\2\2\u01cf\u01c0\3\2\2\2\u01cf\u01c5\3\2\2\2\u01cf\u01ca\3\2"+
		"\2\2\u01d0=\3\2\2\2\u01d1\u01d2\7\4\2\2\u01d2\u01d3\5b\62\2\u01d3\u01d4"+
		"\7\6\2\2\u01d4\u01d5\7\4\2\2\u01d5\u01d6\5b\62\2\u01d6\u01d7\7\6\2\2\u01d7"+
		"\u01d8\5`\61\2\u01d8\u01d9\7\b\2\2\u01d9\u01da\7\b\2\2\u01da\u01f7\3\2"+
		"\2\2\u01db\u01dc\7\4\2\2\u01dc\u01dd\5b\62\2\u01dd\u01de\7\6\2\2\u01de"+
		"\u01df\5J&\2\u01df\u01e0\7\b\2\2\u01e0\u01f7\3\2\2\2\u01e1\u01e2\7\4\2"+
		"\2\u01e2\u01e3\5b\62\2\u01e3\u01e4\7\6\2\2\u01e4\u01e5\7\4\2\2\u01e5\u01e6"+
		"\5b\62\2\u01e6\u01e7\7\b\2\2\u01e7\u01e8\7\b\2\2\u01e8\u01f7\3\2\2\2\u01e9"+
		"\u01ea\7\4\2\2\u01ea\u01eb\7\4\2\2\u01eb\u01ec\5b\62\2\u01ec\u01ed\7\6"+
		"\2\2\u01ed\u01ee\5J&\2\u01ee\u01ef\7\b\2\2\u01ef\u01f0\7\b\2\2\u01f0\u01f7"+
		"\3\2\2\2\u01f1\u01f7\5J&\2\u01f2\u01f3\7\4\2\2\u01f3\u01f4\5J&\2\u01f4"+
		"\u01f5\7\b\2\2\u01f5\u01f7\3\2\2\2\u01f6\u01d1\3\2\2\2\u01f6\u01db\3\2"+
		"\2\2\u01f6\u01e1\3\2\2\2\u01f6\u01e9\3\2\2\2\u01f6\u01f1\3\2\2\2\u01f6"+
		"\u01f2\3\2\2\2\u01f7?\3\2\2\2\u01f8\u01fb\5H%\2\u01f9\u01fb\5L\'\2\u01fa"+
		"\u01f8\3\2\2\2\u01fa\u01f9\3\2\2\2\u01fbA\3\2\2\2\u01fc\u0202\5J&\2\u01fd"+
		"\u01fe\7\4\2\2\u01fe\u01ff\5H%\2\u01ff\u0200\7\b\2\2\u0200\u0202\3\2\2"+
		"\2\u0201\u01fc\3\2\2\2\u0201\u01fd\3\2\2\2\u0202C\3\2\2\2\u0203\u020b"+
		"\5B\"\2\u0204\u0205\7<\2\2\u0205\u020b\5B\"\2\u0206\u0207\7=\2\2\u0207"+
		"\u020b\5B\"\2\u0208\u0209\7>\2\2\u0209\u020b\5B\"\2\u020a\u0203\3\2\2"+
		"\2\u020a\u0204\3\2\2\2\u020a\u0206\3\2\2\2\u020a\u0208\3\2\2\2\u020bE"+
		"\3\2\2\2\u020c\u0211\5D#\2\u020d\u020e\t\6\2\2\u020e\u0210\5D#\2\u020f"+
		"\u020d\3\2\2\2\u0210\u0213\3\2\2\2\u0211\u020f\3\2\2\2\u0211\u0212\3\2"+
		"\2\2\u0212\u0217\3\2\2\2\u0213\u0211\3\2\2\2\u0214\u0215\7\31\2\2\u0215"+
		"\u0217\5D#\2\u0216\u020c\3\2\2\2\u0216\u0214\3\2\2\2\u0217G\3\2\2\2\u0218"+
		"\u021d\5F$\2\u0219\u021a\t\7\2\2\u021a\u021c\5F$\2\u021b\u0219\3\2\2\2"+
		"\u021c\u021f\3\2\2\2\u021d\u021b\3\2\2\2\u021d\u021e\3\2\2\2\u021eI\3"+
		"\2\2\2\u021f\u021d\3\2\2\2\u0220\u0224\5b\62\2\u0221\u0224\5j\66\2\u0222"+
		"\u0224\5h\65\2\u0223\u0220\3\2\2\2\u0223\u0221\3\2\2\2\u0223\u0222\3\2"+
		"\2\2\u0224K\3\2\2\2\u0225\u0229\5\\/\2\u0226\u0229\5N(\2\u0227\u0229\5"+
		"P)\2\u0228\u0225\3\2\2\2\u0228\u0226\3\2\2\2\u0228\u0227\3\2\2\2\u0229"+
		"M\3\2\2\2\u022a\u022c\7D\2\2\u022b\u022a\3\2\2\2\u022b\u022c\3\2\2\2\u022c"+
		"\u022d\3\2\2\2\u022d\u022e\7\4\2\2\u022e\u0233\5@!\2\u022f\u0230\7\6\2"+
		"\2\u0230\u0232\5@!\2\u0231\u022f\3\2\2\2\u0232\u0235\3\2\2\2\u0233\u0231"+
		"\3\2\2\2\u0233\u0234\3\2\2\2\u0234\u0236\3\2\2\2\u0235\u0233\3\2\2\2\u0236"+
		"\u0237\7\b\2\2\u0237O\3\2\2\2\u0238\u023a\7\t\2\2\u0239\u023b\5R*\2\u023a"+
		"\u0239\3\2\2\2\u023a\u023b\3\2\2\2\u023b\u023c\3\2\2\2\u023c\u023d\7\n"+
		"\2\2\u023dQ\3\2\2\2\u023e\u0249\5@!\2\u023f\u0240\7\13\2\2\u0240\u024a"+
		"\5b\62\2\u0241\u0242\7\13\2\2\u0242\u0244\7\t\2\2\u0243\u0245\5R*\2\u0244"+
		"\u0243\3\2\2\2\u0244\u0245\3\2\2\2\u0245\u0246\3\2\2\2\u0246\u024a\7\n"+
		"\2\2\u0247\u0248\7\6\2\2\u0248\u024a\5R*\2\u0249\u023f\3\2\2\2\u0249\u0241"+
		"\3\2\2\2\u0249\u0247\3\2\2\2\u0249\u024a\3\2\2\2\u024aS\3\2\2\2\u024b"+
		"\u0251\7D\2\2\u024c\u024e\7\4\2\2\u024d\u024f\5V,\2\u024e\u024d\3\2\2"+
		"\2\u024e\u024f\3\2\2\2\u024f\u0250\3\2\2\2\u0250\u0252\7\b\2\2\u0251\u024c"+
		"\3\2\2\2\u0251\u0252\3\2\2\2\u0252U\3\2\2\2\u0253\u0258\5X-\2\u0254\u0255"+
		"\7\6\2\2\u0255\u0257\5X-\2\u0256\u0254\3\2\2\2\u0257\u025a\3\2\2\2\u0258"+
		"\u0256\3\2\2\2\u0258\u0259\3\2\2\2\u0259W\3\2\2\2\u025a\u0258\3\2\2\2"+
		"\u025b\u025d\7D\2\2\u025c\u025b\3\2\2\2\u025c\u025d\3\2\2\2\u025d\u025e"+
		"\3\2\2\2\u025e\u025f\7\4\2\2\u025f\u0260\5V,\2\u0260\u0261\7\b\2\2\u0261"+
		"\u026d\3\2\2\2\u0262\u0264\7\t\2\2\u0263\u0265\5V,\2\u0264\u0263\3\2\2"+
		"\2\u0264\u0265\3\2\2\2\u0265\u0266\3\2\2\2\u0266\u026d\7\n\2\2\u0267\u026d"+
		"\5b\62\2\u0268\u026d\7F\2\2\u0269\u026d\5j\66\2\u026a\u026d\5h\65\2\u026b"+
		"\u026d\5\\/\2\u026c\u025c\3\2\2\2\u026c\u0262\3\2\2\2\u026c\u0267\3\2"+
		"\2\2\u026c\u0268\3\2\2\2\u026c\u0269\3\2\2\2\u026c\u026a\3\2\2\2\u026c"+
		"\u026b\3\2\2\2\u026dY\3\2\2\2\u026e\u0272\7E\2\2\u026f\u0272\7D\2\2\u0270"+
		"\u0272\5f\64\2\u0271\u026e\3\2\2\2\u0271\u026f\3\2\2\2\u0271\u0270\3\2"+
		"\2\2\u0272[\3\2\2\2\u0273\u0276\7!\2\2\u0274\u0276\5Z.\2\u0275\u0273\3"+
		"\2\2\2\u0275\u0274\3\2\2\2\u0276]\3\2\2\2\u0277\u0278\7\33\2\2\u0278\u0279"+
		"\5\\/\2\u0279_\3\2\2\2\u027a\u027f\5b\62\2\u027b\u027c\7\6\2\2\u027c\u027e"+
		"\5b\62\2\u027d\u027b\3\2\2\2\u027e\u0281\3\2\2\2\u027f\u027d\3\2\2\2\u027f"+
		"\u0280\3\2\2\2\u0280a\3\2\2\2\u0281\u027f\3\2\2\2\u0282\u0283\t\b\2\2"+
		"\u0283c\3\2\2\2\u0284\u0285\t\t\2\2\u0285e\3\2\2\2\u0286\u0287\t\n\2\2"+
		"\u0287g\3\2\2\2\u0288\u028a\7\31\2\2\u0289\u0288\3\2\2\2\u0289\u028a\3"+
		"\2\2\2\u028a\u028b\3\2\2\2\u028b\u028c\7G\2\2\u028ci\3\2\2\2\u028d\u028f"+
		"\7\31\2\2\u028e\u028d\3\2\2\2\u028e\u028f\3\2\2\2\u028f\u0290\3\2\2\2"+
		"\u0290\u0291\7H\2\2\u0291k\3\2\2\2Dn\u0086\u008d\u0092\u0094\u009b\u00a4"+
		"\u00b2\u00c4\u00c8\u00d5\u00d9\u00e0\u00e5\u00ee\u00f6\u00fa\u0102\u010a"+
		"\u0112\u0117\u0123\u0131\u0135\u0140\u0146\u014d\u0159\u0168\u017b\u0185"+
		"\u0188\u0191\u0196\u019b\u01a5\u01a8\u01b3\u01b8\u01be\u01cf\u01f6\u01fa"+
		"\u0201\u020a\u0211\u0216\u021d\u0223\u0228\u022b\u0233\u023a\u0244\u0249"+
		"\u024e\u0251\u0258\u025c\u0264\u026c\u0271\u0275\u027f\u0289\u028e";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}