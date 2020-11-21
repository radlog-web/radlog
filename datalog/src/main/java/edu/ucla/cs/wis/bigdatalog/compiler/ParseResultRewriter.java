package edu.ucla.cs.wis.bigdatalog.compiler;

import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.Aggregate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFunctor;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Rewriter rewrites the ParseResult, which is a rewriting process happens
 * before any of the later rewritings. The reason of doing rewritings here is mainly
 * because the Datalog compiler logic is too complex to change for implementing some features.
 * E.g. the sample function used with mmin and mmax.
 */
public class ParseResultRewriter {
  private static final Logger logger = LoggerFactory.getLogger(ParseResultRewriter.class);

  public static final String SAMPLE_FUNCTION_NAME = "sample";
  public static final String SAMPLE_VAR_SUFFIX = "_" + SAMPLE_FUNCTION_NAME;

  public static final String CMAX_FUNCTION_NAME = "cmax";
  public static final String CMAX_VAR_SUFFIX = "_" + CMAX_FUNCTION_NAME;

  public static final String CMIN_FUNCTION_NAME = "cmin";
  public static final String CMIN_VAR_SUFFIX = "_" + CMIN_FUNCTION_NAME;

  public static final String MAVG_FUNCTION_NAME = "mavg";
  public static final String MAVG_VAR_SUFFIX = "_" + MAVG_FUNCTION_NAME;

  private static void rewriteSampleFunctionToVariable(Predicate head) {
    // [Hack] we rewrite the sample function in ParseResult to its argument variable
    // so the Datalog compiler will treat it as a normal variable during analysis
    // thus avoid it to be mistakenly considered as an aggregate function
    rewriteFunctionToVariable(head, SAMPLE_FUNCTION_NAME, SAMPLE_VAR_SUFFIX);
  }

  private static void rewriteMAvgFunctionToVariable(Predicate head) {
    rewriteFunctionToVariable(head, MAVG_FUNCTION_NAME, MAVG_VAR_SUFFIX);
  }

  private static void rewriteCMaxFunctionToVariable(Predicate head) {
    // [Hack] we rewrite the cmax function in ParseResult to its argument variable
    // so the Datalog compiler will treat it as a normal variable during analysis
    // thus avoid it to be mistakenly considered as an aggregate function
    rewriteFunctionToVariable(head, CMAX_FUNCTION_NAME, CMAX_VAR_SUFFIX);
  }

  private static void rewriteCMinFunctionToVariable(Predicate head) {
    // [Hack] we rewrite the cmin function in ParseResult to its argument variable
    // so the Datalog compiler will treat it as a normal variable during analysis
    // thus avoid it to be mistakenly considered as an aggregate function
    rewriteFunctionToVariable(head, CMIN_FUNCTION_NAME, CMIN_VAR_SUFFIX);
  }

  private static void rewriteFunctionToVariable(Predicate head, String funcName, String funcSuffix) {
    // [Hack] we rewrite the chain function in ParseResult to its argument variable
    // so the Datalog compiler will treat it as a normal variable during analysis
    // thus avoid it to be mistakenly considered as an aggregate function
    CompilerTypeList arguments = head.getArguments();
    int pos = 0;
    for (CompilerTypeBase arg: arguments) {
      if (arg instanceof Aggregate &&
              ((Aggregate) arg).getAggregateName().toLowerCase().equals(funcName)) {
        if(((Aggregate) arg).getAggregateTerm() instanceof CompilerFunctor){
          CompilerFunctor functor = (CompilerFunctor) ((Aggregate) arg).getAggregateTerm();
          CompilerVariable v = new CompilerVariable("");
          for(int i = 0; i< functor.getArguments().size(); i++){
            if(v.getVariableName().equals("")){
              v.rename(functor.getArguments().get(i).toString());
            } else {
              v.rename(v.getVariableName()+ "#_#" + functor.getArguments().get(i));
            }

            arguments.set(pos, new CompilerVariable(v.getVariableName() + funcSuffix));
          }
        } else {
          CompilerVariable v = (CompilerVariable) ((Aggregate) arg).getAggregateTerm();
          arguments.set(pos, new CompilerVariable(v.getVariableName() + funcSuffix));
        }

      }
      pos += 1;
    }
  }

  private static void rewriteRule(Rule rule) {
    String prev = rule.toString();
    Predicate head = rule.head;
    rewriteSampleFunctionToVariable(head);
    rewriteCMaxFunctionToVariable(head);
    rewriteCMinFunctionToVariable(head);
//    rewriteMAvgFunctionToVariable(head);
    logger.info("Parsed Rule is rewritten from: " + prev + "\nto: " + rule.toString());
  }

  public static void rewrite(ModuleDeclaration module) {
    for (Rule r: module.rules) {
      rewriteRule(r);
    }
  }
}
