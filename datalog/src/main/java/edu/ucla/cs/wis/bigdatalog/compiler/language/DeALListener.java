// Generated from DeAL.g4 by ANTLR 4.5.3
package edu.ucla.cs.wis.bigdatalog.compiler.language;

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

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link DeALParser}.
 */
public interface DeALListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link DeALParser#parseQueryForm}.
	 * @param ctx the parse tree
	 */
	void enterParseQueryForm(DeALParser.ParseQueryFormContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#parseQueryForm}.
	 * @param ctx the parse tree
	 */
	void exitParseQueryForm(DeALParser.ParseQueryFormContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#parseQuery}.
	 * @param ctx the parse tree
	 */
	void enterParseQuery(DeALParser.ParseQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#parseQuery}.
	 * @param ctx the parse tree
	 */
	void exitParseQuery(DeALParser.ParseQueryContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#parseRule}.
	 * @param ctx the parse tree
	 */
	void enterParseRule(DeALParser.ParseRuleContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#parseRule}.
	 * @param ctx the parse tree
	 */
	void exitParseRule(DeALParser.ParseRuleContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#parseGroundTerm}.
	 * @param ctx the parse tree
	 */
	void enterParseGroundTerm(DeALParser.ParseGroundTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#parseGroundTerm}.
	 * @param ctx the parse tree
	 */
	void exitParseGroundTerm(DeALParser.ParseGroundTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#parseGroundPredicate}.
	 * @param ctx the parse tree
	 */
	void enterParseGroundPredicate(DeALParser.ParseGroundPredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#parseGroundPredicate}.
	 * @param ctx the parse tree
	 */
	void exitParseGroundPredicate(DeALParser.ParseGroundPredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#parseLoadDatabaseObjects}.
	 * @param ctx the parse tree
	 */
	void enterParseLoadDatabaseObjects(DeALParser.ParseLoadDatabaseObjectsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#parseLoadDatabaseObjects}.
	 * @param ctx the parse tree
	 */
	void exitParseLoadDatabaseObjects(DeALParser.ParseLoadDatabaseObjectsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#parseLoadDatabaseFacts}.
	 * @param ctx the parse tree
	 */
	void enterParseLoadDatabaseFacts(DeALParser.ParseLoadDatabaseFactsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#parseLoadDatabaseFacts}.
	 * @param ctx the parse tree
	 */
	void exitParseLoadDatabaseFacts(DeALParser.ParseLoadDatabaseFactsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#databaseObjectDeclarations}.
	 * @param ctx the parse tree
	 */
	void enterDatabaseObjectDeclarations(DeALParser.DatabaseObjectDeclarationsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#databaseObjectDeclarations}.
	 * @param ctx the parse tree
	 */
	void exitDatabaseObjectDeclarations(DeALParser.DatabaseObjectDeclarationsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#databaseDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterDatabaseDeclaration(DeALParser.DatabaseDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#databaseDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitDatabaseDeclaration(DeALParser.DatabaseDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#moduleDeclaration}.
	 * @param ctx the parse tree
	 */
	void enterModuleDeclaration(DeALParser.ModuleDeclarationContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#moduleDeclaration}.
	 * @param ctx the parse tree
	 */
	void exitModuleDeclaration(DeALParser.ModuleDeclarationContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#databaseBasePredicates}.
	 * @param ctx the parse tree
	 */
	void enterDatabaseBasePredicates(DeALParser.DatabaseBasePredicatesContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#databaseBasePredicates}.
	 * @param ctx the parse tree
	 */
	void exitDatabaseBasePredicates(DeALParser.DatabaseBasePredicatesContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#fact}.
	 * @param ctx the parse tree
	 */
	void enterFact(DeALParser.FactContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#fact}.
	 * @param ctx the parse tree
	 */
	void exitFact(DeALParser.FactContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#groundArguments}.
	 * @param ctx the parse tree
	 */
	void enterGroundArguments(DeALParser.GroundArgumentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#groundArguments}.
	 * @param ctx the parse tree
	 */
	void exitGroundArguments(DeALParser.GroundArgumentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#groundTerm}.
	 * @param ctx the parse tree
	 */
	void enterGroundTerm(DeALParser.GroundTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#groundTerm}.
	 * @param ctx the parse tree
	 */
	void exitGroundTerm(DeALParser.GroundTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#groundListTerms}.
	 * @param ctx the parse tree
	 */
	void enterGroundListTerms(DeALParser.GroundListTermsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#groundListTerms}.
	 * @param ctx the parse tree
	 */
	void exitGroundListTerms(DeALParser.GroundListTermsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#groundListTerms2}.
	 * @param ctx the parse tree
	 */
	void enterGroundListTerms2(DeALParser.GroundListTerms2Context ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#groundListTerms2}.
	 * @param ctx the parse tree
	 */
	void exitGroundListTerms2(DeALParser.GroundListTerms2Context ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#basePredicate}.
	 * @param ctx the parse tree
	 */
	void enterBasePredicate(DeALParser.BasePredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#basePredicate}.
	 * @param ctx the parse tree
	 */
	void exitBasePredicate(DeALParser.BasePredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#basePredicateStructuralAttribute}.
	 * @param ctx the parse tree
	 */
	void enterBasePredicateStructuralAttribute(DeALParser.BasePredicateStructuralAttributeContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#basePredicateStructuralAttribute}.
	 * @param ctx the parse tree
	 */
	void exitBasePredicateStructuralAttribute(DeALParser.BasePredicateStructuralAttributeContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#basePredicateKeyOrIndex}.
	 * @param ctx the parse tree
	 */
	void enterBasePredicateKeyOrIndex(DeALParser.BasePredicateKeyOrIndexContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#basePredicateKeyOrIndex}.
	 * @param ctx the parse tree
	 */
	void exitBasePredicateKeyOrIndex(DeALParser.BasePredicateKeyOrIndexContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#export}.
	 * @param ctx the parse tree
	 */
	void enterExport(DeALParser.ExportContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#export}.
	 * @param ctx the parse tree
	 */
	void exitExport(DeALParser.ExportContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#predicateRule}.
	 * @param ctx the parse tree
	 */
	void enterPredicateRule(DeALParser.PredicateRuleContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#predicateRule}.
	 * @param ctx the parse tree
	 */
	void exitPredicateRule(DeALParser.PredicateRuleContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#ruleHead}.
	 * @param ctx the parse tree
	 */
	void enterRuleHead(DeALParser.RuleHeadContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#ruleHead}.
	 * @param ctx the parse tree
	 */
	void exitRuleHead(DeALParser.RuleHeadContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#ruleBody}.
	 * @param ctx the parse tree
	 */
	void enterRuleBody(DeALParser.RuleBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#ruleBody}.
	 * @param ctx the parse tree
	 */
	void exitRuleBody(DeALParser.RuleBodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#literal}.
	 * @param ctx the parse tree
	 */
	void enterLiteral(DeALParser.LiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#literal}.
	 * @param ctx the parse tree
	 */
	void exitLiteral(DeALParser.LiteralContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#predicate}.
	 * @param ctx the parse tree
	 */
	void enterPredicate(DeALParser.PredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#predicate}.
	 * @param ctx the parse tree
	 */
	void exitPredicate(DeALParser.PredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#binaryOperator}.
	 * @param ctx the parse tree
	 */
	void enterBinaryOperator(DeALParser.BinaryOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#binaryOperator}.
	 * @param ctx the parse tree
	 */
	void exitBinaryOperator(DeALParser.BinaryOperatorContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#choiceArgument}.
	 * @param ctx the parse tree
	 */
	void enterChoiceArgument(DeALParser.ChoiceArgumentContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#choiceArgument}.
	 * @param ctx the parse tree
	 */
	void exitChoiceArgument(DeALParser.ChoiceArgumentContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#headPredicate}.
	 * @param ctx the parse tree
	 */
	void enterHeadPredicate(DeALParser.HeadPredicateContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#headPredicate}.
	 * @param ctx the parse tree
	 */
	void exitHeadPredicate(DeALParser.HeadPredicateContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#headTerm}.
	 * @param ctx the parse tree
	 */
	void enterHeadTerm(DeALParser.HeadTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#headTerm}.
	 * @param ctx the parse tree
	 */
	void exitHeadTerm(DeALParser.HeadTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#headAggregateTerm}.
	 * @param ctx the parse tree
	 */
	void enterHeadAggregateTerm(DeALParser.HeadAggregateTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#headAggregateTerm}.
	 * @param ctx the parse tree
	 */
	void exitHeadAggregateTerm(DeALParser.HeadAggregateTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#headFscntSubterms}.
	 * @param ctx the parse tree
	 */
	void enterHeadFscntSubterms(DeALParser.HeadFscntSubtermsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#headFscntSubterms}.
	 * @param ctx the parse tree
	 */
	void exitHeadFscntSubterms(DeALParser.HeadFscntSubtermsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#term}.
	 * @param ctx the parse tree
	 */
	void enterTerm(DeALParser.TermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#term}.
	 * @param ctx the parse tree
	 */
	void exitTerm(DeALParser.TermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#basicExpression}.
	 * @param ctx the parse tree
	 */
	void enterBasicExpression(DeALParser.BasicExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#basicExpression}.
	 * @param ctx the parse tree
	 */
	void exitBasicExpression(DeALParser.BasicExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#unaryArithmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterUnaryArithmeticExpression(DeALParser.UnaryArithmeticExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#unaryArithmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitUnaryArithmeticExpression(DeALParser.UnaryArithmeticExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#multiplicativeArithmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterMultiplicativeArithmeticExpression(DeALParser.MultiplicativeArithmeticExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#multiplicativeArithmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitMultiplicativeArithmeticExpression(DeALParser.MultiplicativeArithmeticExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#arithmeticExpression}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticExpression(DeALParser.ArithmeticExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#arithmeticExpression}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticExpression(DeALParser.ArithmeticExpressionContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#arithmeticTerm}.
	 * @param ctx the parse tree
	 */
	void enterArithmeticTerm(DeALParser.ArithmeticTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#arithmeticTerm}.
	 * @param ctx the parse tree
	 */
	void exitArithmeticTerm(DeALParser.ArithmeticTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#nonArithmeticTerm}.
	 * @param ctx the parse tree
	 */
	void enterNonArithmeticTerm(DeALParser.NonArithmeticTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#nonArithmeticTerm}.
	 * @param ctx the parse tree
	 */
	void exitNonArithmeticTerm(DeALParser.NonArithmeticTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#functorTerm}.
	 * @param ctx the parse tree
	 */
	void enterFunctorTerm(DeALParser.FunctorTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#functorTerm}.
	 * @param ctx the parse tree
	 */
	void exitFunctorTerm(DeALParser.FunctorTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#listTerm}.
	 * @param ctx the parse tree
	 */
	void enterListTerm(DeALParser.ListTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#listTerm}.
	 * @param ctx the parse tree
	 */
	void exitListTerm(DeALParser.ListTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#listTermArguments}.
	 * @param ctx the parse tree
	 */
	void enterListTermArguments(DeALParser.ListTermArgumentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#listTermArguments}.
	 * @param ctx the parse tree
	 */
	void exitListTermArguments(DeALParser.ListTermArgumentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#queryForm}.
	 * @param ctx the parse tree
	 */
	void enterQueryForm(DeALParser.QueryFormContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#queryForm}.
	 * @param ctx the parse tree
	 */
	void exitQueryForm(DeALParser.QueryFormContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#queryFormArguments}.
	 * @param ctx the parse tree
	 */
	void enterQueryFormArguments(DeALParser.QueryFormArgumentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#queryFormArguments}.
	 * @param ctx the parse tree
	 */
	void exitQueryFormArguments(DeALParser.QueryFormArgumentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#queryFormTerm}.
	 * @param ctx the parse tree
	 */
	void enterQueryFormTerm(DeALParser.QueryFormTermContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#queryFormTerm}.
	 * @param ctx the parse tree
	 */
	void exitQueryFormTerm(DeALParser.QueryFormTermContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#name}.
	 * @param ctx the parse tree
	 */
	void enterName(DeALParser.NameContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#name}.
	 * @param ctx the parse tree
	 */
	void exitName(DeALParser.NameContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#anyString}.
	 * @param ctx the parse tree
	 */
	void enterAnyString(DeALParser.AnyStringContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#anyString}.
	 * @param ctx the parse tree
	 */
	void exitAnyString(DeALParser.AnyStringContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#annotations}.
	 * @param ctx the parse tree
	 */
	void enterAnnotations(DeALParser.AnnotationsContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#annotations}.
	 * @param ctx the parse tree
	 */
	void exitAnnotations(DeALParser.AnnotationsContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#variables}.
	 * @param ctx the parse tree
	 */
	void enterVariables(DeALParser.VariablesContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#variables}.
	 * @param ctx the parse tree
	 */
	void exitVariables(DeALParser.VariablesContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#variable}.
	 * @param ctx the parse tree
	 */
	void enterVariable(DeALParser.VariableContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#variable}.
	 * @param ctx the parse tree
	 */
	void exitVariable(DeALParser.VariableContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#udaPredicates}.
	 * @param ctx the parse tree
	 */
	void enterUdaPredicates(DeALParser.UdaPredicatesContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#udaPredicates}.
	 * @param ctx the parse tree
	 */
	void exitUdaPredicates(DeALParser.UdaPredicatesContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#keyword}.
	 * @param ctx the parse tree
	 */
	void enterKeyword(DeALParser.KeywordContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#keyword}.
	 * @param ctx the parse tree
	 */
	void exitKeyword(DeALParser.KeywordContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#signedInteger}.
	 * @param ctx the parse tree
	 */
	void enterSignedInteger(DeALParser.SignedIntegerContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#signedInteger}.
	 * @param ctx the parse tree
	 */
	void exitSignedInteger(DeALParser.SignedIntegerContext ctx);
	/**
	 * Enter a parse tree produced by {@link DeALParser#signedDecimal}.
	 * @param ctx the parse tree
	 */
	void enterSignedDecimal(DeALParser.SignedDecimalContext ctx);
	/**
	 * Exit a parse tree produced by {@link DeALParser#signedDecimal}.
	 * @param ctx the parse tree
	 */
	void exitSignedDecimal(DeALParser.SignedDecimalContext ctx);
}