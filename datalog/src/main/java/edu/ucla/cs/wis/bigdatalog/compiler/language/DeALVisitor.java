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

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link DeALParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface DeALVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link DeALParser#parseQueryForm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParseQueryForm(DeALParser.ParseQueryFormContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#parseQuery}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParseQuery(DeALParser.ParseQueryContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#parseRule}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParseRule(DeALParser.ParseRuleContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#parseGroundTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParseGroundTerm(DeALParser.ParseGroundTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#parseGroundPredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParseGroundPredicate(DeALParser.ParseGroundPredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#parseLoadDatabaseObjects}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParseLoadDatabaseObjects(DeALParser.ParseLoadDatabaseObjectsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#parseLoadDatabaseFacts}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParseLoadDatabaseFacts(DeALParser.ParseLoadDatabaseFactsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#databaseObjectDeclarations}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatabaseObjectDeclarations(DeALParser.DatabaseObjectDeclarationsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#databaseDeclaration}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatabaseDeclaration(DeALParser.DatabaseDeclarationContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#moduleDeclaration}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitModuleDeclaration(DeALParser.ModuleDeclarationContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#databaseBasePredicates}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDatabaseBasePredicates(DeALParser.DatabaseBasePredicatesContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#fact}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFact(DeALParser.FactContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#groundArguments}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroundArguments(DeALParser.GroundArgumentsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#groundTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroundTerm(DeALParser.GroundTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#groundListTerms}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroundListTerms(DeALParser.GroundListTermsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#groundListTerms2}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitGroundListTerms2(DeALParser.GroundListTerms2Context ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#basePredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBasePredicate(DeALParser.BasePredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#basePredicateStructuralAttribute}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBasePredicateStructuralAttribute(DeALParser.BasePredicateStructuralAttributeContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#basePredicateKeyOrIndex}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBasePredicateKeyOrIndex(DeALParser.BasePredicateKeyOrIndexContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#export}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExport(DeALParser.ExportContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#predicateRule}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicateRule(DeALParser.PredicateRuleContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#ruleHead}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRuleHead(DeALParser.RuleHeadContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#ruleBody}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRuleBody(DeALParser.RuleBodyContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteral(DeALParser.LiteralContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#predicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPredicate(DeALParser.PredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#binaryOperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBinaryOperator(DeALParser.BinaryOperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#choiceArgument}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitChoiceArgument(DeALParser.ChoiceArgumentContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#headPredicate}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHeadPredicate(DeALParser.HeadPredicateContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#headTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHeadTerm(DeALParser.HeadTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#headAggregateTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHeadAggregateTerm(DeALParser.HeadAggregateTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#headFscntSubterms}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHeadFscntSubterms(DeALParser.HeadFscntSubtermsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#term}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTerm(DeALParser.TermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#basicExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBasicExpression(DeALParser.BasicExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#unaryArithmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUnaryArithmeticExpression(DeALParser.UnaryArithmeticExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#multiplicativeArithmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMultiplicativeArithmeticExpression(DeALParser.MultiplicativeArithmeticExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#arithmeticExpression}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticExpression(DeALParser.ArithmeticExpressionContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#arithmeticTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArithmeticTerm(DeALParser.ArithmeticTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#nonArithmeticTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonArithmeticTerm(DeALParser.NonArithmeticTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#functorTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitFunctorTerm(DeALParser.FunctorTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#listTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitListTerm(DeALParser.ListTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#listTermArguments}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitListTermArguments(DeALParser.ListTermArgumentsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#queryForm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryForm(DeALParser.QueryFormContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#queryFormArguments}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryFormArguments(DeALParser.QueryFormArgumentsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#queryFormTerm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQueryFormTerm(DeALParser.QueryFormTermContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#name}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitName(DeALParser.NameContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#anyString}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnyString(DeALParser.AnyStringContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#annotations}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnnotations(DeALParser.AnnotationsContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#variables}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariables(DeALParser.VariablesContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#variable}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitVariable(DeALParser.VariableContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#udaPredicates}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitUdaPredicates(DeALParser.UdaPredicatesContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#keyword}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitKeyword(DeALParser.KeywordContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#signedInteger}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSignedInteger(DeALParser.SignedIntegerContext ctx);
	/**
	 * Visit a parse tree produced by {@link DeALParser#signedDecimal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSignedDecimal(DeALParser.SignedDecimalContext ctx);
}