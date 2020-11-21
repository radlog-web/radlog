package edu.ucla.cs.wis.bigdatalog.compiler.compilation;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.CompilerException;
import edu.ucla.cs.wis.bigdatalog.compiler.Export;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.ProgramRules;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.adornment.BottomUpAdorner;
import edu.ucla.cs.wis.bigdatalog.compiler.adornment.TopDownAdorner;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.RuleAnalyzer;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.XYCliqueAnalyzer;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.Extractor;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.Rewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeInferrer;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYStageArgumentEliminator;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class Compiler {
	private static Logger logger = LoggerFactory.getLogger(Compiler.class.getName());

	protected DeALSContext deALSContext;
	protected Module module;
	protected PrintWriter outputWriter;
	
	public Compiler(DeALSContext deALSContext, Module module) {
		if (module == null)
			throw new CompilerException("Module must be provided to Compiler.");
		
		this.deALSContext = deALSContext;
		this.module = module;
	}	
	
	public CompilationResult compile(QueryForm queryForm) {
		if (queryForm == null)
			throw new CompilerException("Can not compile null query form.");
		
		if (this.deALSContext.isTraceEnabled())
			this.deALSContext.logTrace(logger, "Entering compile for {}", queryForm.toString());
				
		if (this.module.isModified()) {
			new PreCompiler(this.deALSContext).prepareForCompilation(this.module);
			this.module.setIsModified(false);
			
			new XYCliqueAnalyzer(this.deALSContext, this.module).mergeSubordinateXYCliques(queryForm);
		}
		
		QueryForm exportQueryForm = queryForm.copy();

		if (!TypeInferrer.assignQueryForm(exportQueryForm, this.module))
			return new CompilationResult("No predicate matching query form for '" + queryForm.getPredicateName() + "|" + queryForm.getArity() + "' could be found.");

		// if didn't find it, but the settings allow us to continue without defined export, 
		// recompile and create an export
		CompilationResult result = this.doCompile(exportQueryForm);
		if (!result.isSuccess())
			exportQueryForm = null;	
		else
			this.module.addExport(new Export(exportQueryForm));
			//this.deALSConfiguration.getModuleManager().addExport(new Export(exportQueryForm));
	
		if (this.deALSContext.isTraceEnabled())
			this.deALSContext.logTrace(logger, "Exiting compile with {}", exportQueryForm);
				
		return result;
	}
	
	protected CompilationResult doCompile(QueryForm queryForm) {
		this.deALSContext.logTrace(logger, "Entering doCompile for {}", queryForm);		
		this.deALSContext.logInfo(logger, "[BEGIN Compile Query Form '{}' BEGIN]", queryForm);
		
		PCGOrNode 			queryFormRootOrNode 		= null;
		List<CliqueBase>	adornedCliqueList 			= new ArrayList<>();
		List<CliqueBase> 	rewrittenCliqueList 		= new ArrayList<>();
		List<Clique> 		finalCliqueList 			= new ArrayList<>();
		RuleAnalyzer		ruleAnalyzer 				= new RuleAnalyzer(this.deALSContext);
		StringBuilder		message						= new StringBuilder();
		
		queryForm.clearProgram();
		queryForm.variableList = new CompilerTypeList();
		
		//VariableList.beginVariableListTracking(queryForm.variableList, tempVariableList);		
		message.append("Compiling " + queryForm.toString());

		// STEP 1 - extract relevant pcg
		Pair<PCGOrNode, ProgramRules> extractorResult = 
				new Extractor(this.deALSContext).extractRelevantPredicateConnectionGraph(queryForm, this.module);
		
		queryFormRootOrNode = extractorResult.getFirst();
				
		if (queryFormRootOrNode != null) {
			if (this.deALSContext.isInfoEnabled()) {
				queryFormRootOrNode.resetIsDescribed();
				this.deALSContext.logInfo(logger, "[BEGIN Step 1 - Relevant Predicate Connection Graph for '{}' BEGIN]{}", queryForm, queryFormRootOrNode.toStringTree());
				this.deALSContext.logInfo(logger, "[END Step 1 - Relevant Predicate Connection Graph for '{}' END]\n", queryForm);
			}
			
			// STEP 2 - Remove XY Stage Arguments
			queryFormRootOrNode = new XYStageArgumentEliminator(this.deALSContext).eliminateStageVariables(queryFormRootOrNode);

			if (this.deALSContext.isInfoEnabled()) {
				queryFormRootOrNode.resetIsDescribed();
				this.deALSContext.logInfo(logger, "[BEGIN Step 2 - Relevant Predicate Connection Graph with XY Stage Arguments Removed BEGIN]{}", queryFormRootOrNode.toStringTree());
				this.deALSContext.logInfo(logger, "[END Step 2 - Relevant Predicate Connection Graph with XY Stage Arguments Removed END]\n");
			}

			// STEP 3 - adorn the relevant pcg
			queryFormRootOrNode = new TopDownAdorner(this.deALSContext).adornQueryForm(queryFormRootOrNode, adornedCliqueList);
			
			if (this.deALSContext.isInfoEnabled()) {
				queryFormRootOrNode.resetIsDescribed();
				this.deALSContext.logInfo(logger, "[BEGIN Step 3 - Top-Down Adorned Query Form '{}' BEGIN]{}", queryForm, queryFormRootOrNode.toStringTree());	
				this.deALSContext.logInfo(logger, "[END Step 3 - Top-Down Adorned Query Form '{}' END]\n", queryForm);
			}				

			// STEP 4 - regenerate adorned cliques
			if (adornedCliqueList.size() == 0) {
				this.deALSContext.logDebug(logger, "[Step 4 Skipped - no adorned cliques to regenerate]");
			} else {
				CliqueBase baseClique;
				for (int i = adornedCliqueList.size() - 1; i >= 0; i--) {
					baseClique = adornedCliqueList.remove(i);
					ruleAnalyzer.regenerateBaseClique(baseClique, null);
				}
				
				if (this.deALSContext.isInfoEnabled()) {
					queryFormRootOrNode.resetIsDescribed();
					this.deALSContext.logInfo(logger, "[BEGIN Step 4 - Regenerated Adorned Cliques for Query Form '{}' BEGIN]{}", queryForm, queryFormRootOrNode.toStringTree());	
					this.deALSContext.logInfo(logger, "[END Step 4 - Regenerated Adorned Cliques for Query Form '{}' END]\n", queryForm);
				}
			}
			
			new TypeInferrer(this.deALSContext).inferTypes(queryFormRootOrNode);
			
			// STEP 5 - rewrite recursive cliques in the adorned pcg
			new Rewriter(this.deALSContext).rewriteQueryForm(queryFormRootOrNode, rewrittenCliqueList);

			if (this.deALSContext.isInfoEnabled()) {
				queryFormRootOrNode.resetIsDescribed();
				this.deALSContext.logInfo(logger, "[BEGIN Step 5 - Rewritten Cliques for Query Form '{}' BEGIN]{}", queryForm, queryFormRootOrNode.toStringTree());
				this.deALSContext.logInfo(logger, "[END Step 5 - Rewritten Cliques for Query Form '{}' END]\n", queryForm);
			}
			
			// STEP 6 - regenerate the rewritten cliques
			if (rewrittenCliqueList.size() == 0) {
				this.deALSContext.logDebug(logger, "[Step 6 - Skipped - no rewritten cliques to regenerate]");
			} else {				
				CliqueBase baseClique;
				for (int i = rewrittenCliqueList.size() - 1; i >= 0; i--) {
					baseClique = rewrittenCliqueList.remove(i);
					ruleAnalyzer.regenerateBaseClique(baseClique, finalCliqueList);
				}
				
				if (this.deALSContext.isInfoEnabled()) {
					queryFormRootOrNode.resetIsDescribed();					
					this.deALSContext.logInfo(logger, "[BEGIN Step 6 - Regenerated Rewritten Cliques for Query Form '{}' BEGIN]{}", 
							queryForm.toString(), queryFormRootOrNode.toStringTree());
					this.deALSContext.logInfo(logger, "[END Step 6 - Rewritten Cliques for Query Form '{}' END]\n", queryForm);
				}
			}

			// STEP 7 - adorn the rewritten pcg with the execution adornment		
			new BottomUpAdorner(this.deALSContext).adornExecutionQueryForm(queryFormRootOrNode);
			
			if (this.deALSContext.isInfoEnabled()) {
				queryFormRootOrNode.resetIsDescribed();				
				this.deALSContext.logInfo(logger, "[BEGIN Step 7 - Bottom-Up Execution Adornment for Query Form '{}' BEGIN]{}", queryForm, queryFormRootOrNode.toStringTree());
				this.deALSContext.logInfo(logger, "[END Step 7 - Bottom-Up Execution Adornment for Query Form '{}' END]\n", queryForm);
			}
			
			finalCliqueList.clear();
		}

		this.deALSContext.logInfo(logger, "[END Compile Query Form '{}' END]\n", queryForm);

		//VariableList.endVariableListTracking(tempVariableList);

		CompilationResult result = new CompilationResult(queryForm, 
				extractorResult.getSecond(), 
				queryFormRootOrNode, 
				message.toString());
		
		this.deALSContext.logTrace(logger, "Exiting doCompile with status = {}", result.isSuccess());
		
		return result;
	}
	
	public boolean isQueryFormDeclarationRequried(Module module) {
		if (this.deALSContext.getConfiguration().compareProperty("deals.enforceQueryFormExports", "false"))
			return false;
			
		return (this.deALSContext.getConfiguration().compareProperty("deals.enforceQueryFormExports", "true") && !module.isDefaultModule());
	}
}