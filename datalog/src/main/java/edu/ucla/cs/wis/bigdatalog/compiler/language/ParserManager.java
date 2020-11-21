package edu.ucla.cs.wis.bigdatalog.compiler.language;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.tree.ParseTree;

import edu.ucla.cs.wis.bigdatalog.compiler.ModuleDeclaration;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;

public class ParserManager implements Serializable {
	private static final long serialVersionUID = 1L;
	private int 					ruleIndex;
	private int						choiceCount;
	private CompilerVariableList 	currentRuleVariableList;

	public ParserManager() {
		this.ruleIndex					= 0;
		this.choiceCount				= 0;
		this.currentRuleVariableList 	= new CompilerVariableList();
	}
	
	public CompilerVariable getVariable(String variableName) {
		return this.currentRuleVariableList.getVariable(variableName);
	}
	
	public void resetVariableList() {
		this.currentRuleVariableList.reset();
	}
	
	public int getRuleIndex() {
		return ++this.ruleIndex;
	}

	public int getNextChoiceCount() {
		return ++this.choiceCount;
	}

	public void reset() {
		this.currentRuleVariableList.reset();
	}

	private DeALParser initializeParser(String text) {
		DeALLexer lexer = new DeALLexer(new ANTLRInputStream(text));
		CommonTokenStream cts = new CommonTokenStream(lexer);
		DeALParser parser = new DeALParser(cts);
		//parser.setTrace(true);
		parser.setParserManager(this);
		// APS 5/8/2014 - SLL is faster than LL
		parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
		parser.removeErrorListeners();
		parser.addErrorListener(new VerboseListener());
		//parser.setErrorHandler(new BailErrorStrategy());
		parser.setErrorHandler(new DefaultErrorStrategy());
		//System.out.println(parser.getInterpreter().getPredictionMode());
		return parser;
	}
	
	public ParseResult parseQueryForm(String queryForm) {
		return this.parse(queryForm, ParseMode.QUERY_FORM);
	}

	public ParseResult parseQuery(String query) {
		return this.parse(query, ParseMode.QUERY);
	}

	public ParseResult parseRule(String rule) {		
		return this.parse(rule, ParseMode.RULE);
	}

	public ParseResult parseTerm(String term) {
		return this.parse(term, ParseMode.TERM);
	}

	public ParseResult parseGroundPredicate(String groundPredicate) {
		return this.parse(groundPredicate, ParseMode.GROUND_PREDICATE);
	}
	
	public ParseResult parseFacts(String facts) {
		return this.parse(facts, ParseMode.FACT);
	}

	public ParseResult parseDatabaseObjects(String modules) {
		return this.parse(modules, ParseMode.DATABASE_OBJECTS);
	}

	public ParseResult parse(String input, ParseMode mode) {
		ReturnStatus status = ReturnStatus.FAIL;
		this.reset();
		//VariableList.beginVariableListTracking(this.allVariableList, tempVariableList);	
		
		CompilerTypeBase 	output = null;				
		DeALParser 			parser = this.initializeParser(input);
		ParseTree			parseTree = null;		
		StringBuilder 		message = new StringBuilder();
				
		try {
			switch (mode) {
				case QUERY_FORM:
					parseTree = parser.parseQueryForm();
				break;
				
				case QUERY:
					parseTree = parser.parseQuery();
				break;
				
				case RULE:
					parseTree = parser.parseRule();
				break;
				
				case TERM:
					parseTree = parser.parseGroundTerm();
				break;
					
				case GROUND_PREDICATE:
					parseTree = parser.parseGroundPredicate();
				break;
					
				case DATABASE_OBJECTS:
					parseTree = parser.parseLoadDatabaseObjects();
				break;
					
				case FACT:
					parseTree = parser.parseLoadDatabaseFacts();
				break;
			}
		} catch (Exception e) {
			message.append(e.getMessage());
			status = ReturnStatus.ERROR;
			//e.printStackTrace();	
		}
				
		// APS 3/1/2013
		// ANTLR doesn't fail on parse errors.  We might get a null pointer when trying to load one of our objects, but not always.
		// Therefore, checking failed() and the # of errors is the best way I've found 
		if (parser.getNumberOfSyntaxErrors() == 0) {
			Visitor visitor = new Visitor();
			visitor.setParserManager(parser.getParserManager());
			output = visitor.visit(parseTree);
			
			switch (mode) {
				case RULE:
					status = ReturnStatus.SUCCESS;
					// mark base predicates because the parsing doesn't catch this
					break;
				case DATABASE_OBJECTS:
					// mark base predicates because the parsing doesn't catch this
					CompilerTypeList list = (CompilerTypeList)output;
					for (int i = 0; i < list.size(); i++)
						setBasePredicateLiterals((ModuleDeclaration)list.get(i));
				default:
					status = ReturnStatus.SUCCESS;
			}
		} else {
			message.append(parser.getNumberOfSyntaxErrors() + " syntax error");
			if (parser.getNumberOfSyntaxErrors() > 1)
				message.append("s");
			
			message.append(" found.\n");
			
			List<?> errorListeners = parser.getErrorListeners();
			for (int i = 0; i < errorListeners.size(); i++) {
				if (errorListeners.get(i) instanceof VerboseListener) {
					List<String> syntaxErrors = ((VerboseListener)errorListeners.get(i)).getSyntaxErrors();
					for (String error : syntaxErrors)
						message.append(error + "\n");
				}
			}
		}

		//VariableList.endVariableListTracking(tempVariableList);
		return new ParseResult(status, output, message.toString());
	}
	
	public static void setBasePredicateLiterals(ModuleDeclaration module) {
		for (Rule rule : module.getRules()) {
			for (Predicate literal : rule.getBody()) {
				if (literal.isDerived())
					if (module.isBasePredicate(literal.getPredicateName()))
						literal.setAsBasePredicate();
			}
		}	
	}
	
	public static class VerboseListener extends BaseErrorListener {
		private List<String> syntaxErrors;
		public VerboseListener() {
			super();
			this.syntaxErrors = new ArrayList<>();
		}
		
		public List<String> getSyntaxErrors() { return this.syntaxErrors; }
		
		@Override
		public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
				int line, int charPositionInLine, String msg, RecognitionException e) {
			List<String> stack = ((Parser)recognizer).getRuleInvocationStack();
			Collections.reverse(stack);
			String errorText = "Syntax error on line " + line + ", index " + charPositionInLine + " at " + offendingSymbol + ": " + msg;
			//System.err.println(errorText);
			this.syntaxErrors.add(errorText);			
		}	
	}	
}
