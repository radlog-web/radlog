package edu.ucla.cs.wis.bigdatalog.compiler.analysis;

import java.util.List;
import java.util.Stack;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class RuleAnalyzer {
	
	private static Logger logger = LoggerFactory.getLogger(RuleAnalyzer.class.getName());
	
	protected DeALSContext 			deALSContext;
	protected Stack<DerivedPredicateNode> 	derivedPredicateNodeStack;
	protected CliqueGenerator				cliqueGenerator;

	public RuleAnalyzer(DeALSContext deALSContext) { 
		this.deALSContext = deALSContext;
	}

	public void reset() { 	}

	public void generateCliques(Module module) {
		this.deALSContext.logTrace(logger, "Entering generateCliques for Module {}", module.getModuleName());
		
		this.deALSContext.logInfo(logger, "\n[BEGIN Rule Analyzer Generate Cliques for Module '{}' BEGIN]", module.getModuleName());
		
		this.reset();

		RuleDependencyGraph dependencyGraph = new RuleDependencyGraph(module);
		dependencyGraph.generate();
		
		this.deALSContext.logInfo(logger, "\n[BEGIN STEP 0.1 - Generated Dependency Graph BEGIN]");
		this.deALSContext.logInfo(logger, dependencyGraph.toString());
		this.deALSContext.logInfo(logger, "[END STEP 0.1 - Generated Dependency Graph END]\n");
		
		this.cliqueGenerator = new CliqueGenerator(this.deALSContext, dependencyGraph);
		this.cliqueGenerator.generateCliques();

		if (this.deALSContext.isInfoEnabled()) {
			this.deALSContext.logInfo(logger, "\n[BEGIN STEP 0.2 - Generated Cliques BEGIN]");

			StringBuilder output = new StringBuilder();
			int counter = 0;
			for (BasicClique clique : this.cliqueGenerator.getCliques()) {
				if (counter > 0) output.append("\n");
				output.append(clique.toString() + "\n");
				counter++;
			}
			if (output.length() > 0)
				this.deALSContext.logInfo(logger, output.toString());
			else
				this.deALSContext.logInfo(logger, "NO CLIQUES GENERATED!");
			
			this.deALSContext.logInfo(logger, "[END STEP 0.2 - Generated Cliques END]\n");	
		}

		// XY rules don't like this - APS 6/10/2013
		//Runtime.getTracer().traceRcaGenerateStage(3,"Checking Rca Cliques");  
		//this.checkCliques();
		//if (Runtime.getTracer().isTraceOn(EnvTracer.ENV_TRACE_LEVEL3))
		//	this.displayRcaCliques(Runtime.getTracer().outs());

		// this is broken and apparently unnecessary
		//Runtime.getTracer().traceRcaGenerateStage(4,"Generating Non-Recursive Derived Predicates");
		//List<DerivedPredicate> derivedPredicates = this.cliqueGenerator.getNonRecursivePredicates();
		//System.out.println(derivedPredicates.toString());
		
		//this.generateNonRecursiveDerivedPredicates();
		//if (Runtime.isTraceLevel_3_Enabled())
			//this.displayNonRecursiveDerivedPredicates(Runtime.getTracer().outs());

		this.deALSContext.logInfo(logger, "[END Rule Analyzer Generate Cliques for Module '{}' END]\n", module.getModuleName()); 

		//this.derivedPredicateNodes.clear();
		//this.derivedPredicates = null;
		//this.cliques = null;
		
		this.deALSContext.logTrace(logger, "Exiting generateCliques for Module {}", module.getModuleName());		
	}

	public void regenerateBaseClique(CliqueBase oldClique, List<Clique> newCliques) {
		switch (oldClique.getType())
		{
		case CLIQUE:
		{
			this.reset();
			if (this.cliqueGenerator == null)
				this.cliqueGenerator = new CliqueGenerator(this.deALSContext);
				
			this.cliqueGenerator.regenerateClique((Clique)oldClique, newCliques);
		}
		break;

		case XY_CLIQUE:
		{
			// no regeneration, just add to list
			if (newCliques != null)
				newCliques.add((Clique)oldClique);
		}
		break;
		}
	}
}
