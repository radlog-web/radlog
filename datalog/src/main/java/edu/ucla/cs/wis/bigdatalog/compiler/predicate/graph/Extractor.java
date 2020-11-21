package edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.aggregate.FSAggregateType;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.XYCliqueAnalyzer;
import edu.ucla.cs.wis.bigdatalog.compiler.analysis.BasicClique;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BuiltInPredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenElsePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.IfThenPredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateBase;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliqueBase;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.Clique;
import edu.ucla.cs.wis.bigdatalog.compiler.recursion.CliquePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerString;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariable;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYClique;
import edu.ucla.cs.wis.bigdatalog.compiler.xy.XYCliquePredicate;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class Extractor {
	private static Logger logger = LoggerFactory.getLogger(Extractor.class.getName());
	
	public static int AGGREGATE_COMPUTE_ARITY = 3;

	protected DeALSContext	deALSContext;
	protected Module				module;
	protected List<CliqueBase> 	constructedCliques;
	protected CompilerTypeList 	visitedCliques;
	protected ProgramRules programRules;
	
	public Extractor(DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
		this.module = null;
		this.constructedCliques = new ArrayList<>();
		this.visitedCliques = new CompilerTypeList();
		this.programRules = new ProgramRules();
	}

	private void reset() {
		this.module = null;
		this.constructedCliques.clear();
		this.visitedCliques.clear();
		this.programRules = null;
	}

	// given a module and query form, this method will extract the relevant PCG for the query form 
	public Pair<PCGOrNode, ProgramRules> extractRelevantPredicateConnectionGraph(Predicate queryForm, Module module) {
		this.deALSContext.logTrace(logger, "Entering extractRelevantPredicateConnectionGraph for {}", queryForm.toString());
			
		this.reset();
		this.module = module;
		this.programRules = new ProgramRules();
		
		CompilerVariableList variableList = new CompilerVariableList();
		CompilerVariableList previousVariableList = new CompilerVariableList();

		PCGOrNode orNode = this.constructOrNode(queryForm, variableList, previousVariableList, null, null);

		if ((orNode != null) && !this.extendOrNode(orNode)) {
			logger.error("No Predicate Connection Graph can be extracted for " + queryForm.toString());
			throw new CompilerException("No Predicate Connection Graph can be extracted for " + queryForm.toString());
		}

		ProgramRules pr = this.programRules;
		
		this.reset();

		String output = "FAIL";
		if (orNode != null)
			output = orNode.toString();
		
		this.deALSContext.logTrace(logger, "Exiting extractRelevantPredicateConnectionGraph for {} \n\twith {}", queryForm.toString(), output);
		
		return new Pair<>(orNode, pr);
	}

	/*BEGIN CONSTRUCT METHODS*/
	/* These methods manage the construction of subtrees of the PCG */	
	private PCGAndNode constructAndNodeFromRule(Rule rule) {
		CompilerVariableList variableList = new CompilerVariableList();
		return constructAndNodeFromRule(rule, variableList);
	}
	
	private PCGAndNode constructAndNodeFromRule(Rule rule, CompilerVariableList variableList) {
		if (rule.getHead().containsBuiltInAggregate() || rule.getHead().containsBuiltInAggregate()) {
			logger.error("No aggregate in head of rule allowed at this point!  Should have already been rewritten.");
			throw new RuntimeException("No aggregate in head of rule allowed at this point!  Should have already been rewritten.");
		}

		this.deALSContext.logTrace(logger, "Entering constructAndNodeFromRule for \n\t{}\n", rule.toString());

		CompilerVariableList previousVariableList = new CompilerVariableList();
		Predicate head = rule.getHead();

		PCGAndNode andNode = new PCGAndNode(head.getPredicateName(), head.getArguments().copy(variableList));
		andNode.setRuleId(rule.getRuleId());
		andNode.setRule(rule);
		andNode.predicate.setFSAggregateType(head.getFSAggregateType());
		andNode.predicate.setArgumentTypeAdornment(head.getArgumentTypeAdornment());

		Utilities.getVariables(head, previousVariableList);

		PCGOrNode orNode;
		for (Predicate literal : rule.getBody()) {
			if ((orNode = this.constructOrNode(literal, variableList, previousVariableList, null, null)) != null) {
				andNode.addChild(orNode);

				if (Extractor.hasLastFalseNode(orNode))
					break;

				Utilities.getVariables(literal, previousVariableList);
			}
		}
		
		for (int i = 0; i < variableList.size(); i++)
			if (variableList.get(i).isAnonymous())
				variableList.get(i).renameVariableName();
		
		this.deALSContext.logTrace(logger, "Exiting constructAndNodeFromRule with {}", andNode.toString());
		
		return andNode;
	}

	private PCGOrNode constructOrNode(Predicate literal, CompilerVariableList variableList, CompilerVariableList previousVariableList, 
			CliqueBase clique, BasicClique basicClique) {
		this.deALSContext.logTrace(logger, "Entering constructOrNode for {}", literal.toString());
		
		PCGOrNode orNode = null;

		if (literal.isBuiltIn()) {
			orNode = this.constructBuiltInOrNode(literal, variableList, previousVariableList, clique, basicClique);
		} else {
			orNode = this.createOrNode(literal, variableList);
		
			CliqueBase baseClique;
			
			// if we have a derived literal and a clique that needs constructing, construct the clique 
			if ((basicClique != null) && literal.isDerived() 
					&& (basicClique.getDerivedPredicate(literal.getPredicateName(), literal.getArity()) != null)) 
				baseClique = clique;
			else
				baseClique = this.constructClique(orNode);
	
			// APS 12/17/2013 - we might be ok using the queryform setting, we might not, take the better of the two
			// between the query form and the matching clique, if one exists, which is the clique's 
			//if ((orNode != null) && !orNode.getPredicate().hasArgumentTypeAdornment() && (baseClique != null)) {
			if ((orNode != null) && (baseClique != null) && (literal instanceof QueryForm)) {
				for (PCGOrNode parent : baseClique.getParents()) {
					if (parent.getPredicateName().equals(orNode.getPredicateName())) {
						if (orNode.getPredicate().getFSAggregateType() == FSAggregateType.NONE)
							orNode.getPredicate().setFSAggregateType(parent.getPredicate().getFSAggregateType());
						
						orNode.getPredicate().setArgumentTypeAdornment(parent.getPredicate().getArgumentTypeAdornment());
						break;
					}
				}
			}
			
			if (baseClique != null) {
				orNode.setBaseClique(baseClique);
				orNode.setAsRecursive();
				baseClique.addParent(orNode);
				/* Propagate stage mark (set during xyCliqueP) to pcg OR node. --HW */
				if (baseClique.getType() == CompilerType.XY_CLIQUE)
					orNode.getPredicate().setXYPredicateType(literal.getXYPredicateType());
			}
		}
		
		String output = "FAIL";
		if (orNode != null)
			output = orNode.toString();
								
		this.deALSContext.logTrace(logger, "Exiting constructOrNode with {}", output);

		return orNode;
	}

	private PCGOrNode constructBuiltInOrNode(Predicate literal, CompilerVariableList variableList, CompilerVariableList previousVariableList, 
			CliqueBase clique, BasicClique basicClique) {
		this.deALSContext.logTrace(logger, "Entering constructBuiltInOrNode for {}", literal.toString());

		if (!(literal instanceof BuiltInPredicate)) {
			logger.error("asked to construct built-in predicate from regular predicate");
			throw new CompilerException("asked to construct built-in predicate from regular predicate");
		}
		
		PCGOrNode orNode = null;
		BuiltInPredicate bip = (BuiltInPredicate)literal;

		switch (bip.getBuiltInPredicateType())
		{
		case TRUE:
		case FALSE:
		case BINARY:
		case CHOICE:
		case GENERIC:
		//case BI_READ_ACCESS_PRED_TYPE: //APS 3/7/2013
		//case READ_AGGREGATE:
		case WRITE_USER_DEFINED_AGGREGATE: //APS 3/7/2013
			orNode = this.createOrNode(bip, variableList);
			break;
		case WRITE_USER_DEFINED_AGGREGATE_FS: 	// @DATALOGFS APS 3/20/2013
			orNode = this.createOrNode(bip, variableList);
			//orNode.setNoBacktrack(true);
		break;

		case IFTHENELSE:
			orNode = this.constructBuiltInIfThenElseOrNode(bip, variableList, previousVariableList, clique, basicClique);
		break;

		case IFTHEN:
			orNode = this.constructBuiltInIfThenOrNode(bip, variableList, previousVariableList, clique, basicClique);
		break;
		
		case AGGREGATE:
		case AGGREGATE_FS:
		case READ_USER_DEFINED_AGGREGATE_FS:
		case READ_USER_DEFINED_AGGREGATE:
		{
			if ((basicClique != null)
					&& (basicClique.getDerivedPredicate(bip.getPredicateName(), bip.getArity()) != null)) {
				orNode = this.createOrNode(bip, variableList);

				orNode.setBaseClique(clique);
				orNode.setAsRecursive();
				clique.addParent(orNode);
				
				/* Propagate stage mark (set during xyCliqueP) to pcg OR node. --HW */
				if (clique.getType() == CompilerType.XY_CLIQUE)
					orNode.getPredicate().setXYPredicateType(bip.getXYPredicateType());
			} else {
				switch (bip.getBuiltInPredicateType()) {
				case AGGREGATE:
					orNode = this.createOrNode(bip, variableList);
					orNode.getPredicate().setArgumentTypeAdornment(bip.getArgumentTypeAdornment());
					orNode.setNoBacktrack(true);
					break;
				case AGGREGATE_FS:
					orNode = this.createOrNode(bip, variableList);
					//orNode.setNoBacktrack(true);
					orNode.getPredicate().setArgumentTypeAdornment(bip.getArgumentTypeAdornment());
					orNode.getPredicate().setFSAggregateType(bip.getFSAggregateType());
					break;
				case READ_USER_DEFINED_AGGREGATE:
					orNode = this.createOrNode(bip, variableList);
					//orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), 
					//		BuiltInPredicateType.READ_AGGREGATE);					
					//this.extendDerivedOrNode(orNode);
					break;
				case READ_USER_DEFINED_AGGREGATE_FS:					
					orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.READ_USER_DEFINED_AGGREGATE_FS);
					orNode.getPredicate().setArgumentTypeAdornment(bip.getArgumentTypeAdornment());
					orNode.getPredicate().setFSAggregateType(bip.getFSAggregateType());
					this.extendDerivedOrNode(orNode);
					break;
				}
			}
		}
		break;

		case SINGLE:
			// APS added 3/4/2013
			orNode = new PCGOrNode(BuiltInPredicate.SINGLE_PREDICATE_NAME, bip.getArguments().copy(variableList), 
					BuiltInPredicateType.SINGLE_MULTI_USER_DEFINED_AGGREGATE);
		break;
		case MULTI:
			// APS added 3/4/2013
			orNode = new PCGOrNode(BuiltInPredicate.MULTI_PREDICATE_NAME, bip.getArguments().copy(variableList),
					BuiltInPredicateType.SINGLE_MULTI_USER_DEFINED_AGGREGATE);
		break;
		case RETURN:
			// APS added 3/4/2013
			orNode = new PCGOrNode(BuiltInPredicate.RETURN_PREDICATE_NAME, bip.getArguments().copy(variableList),
					BuiltInPredicateType.SINGLE_MULTI_USER_DEFINED_AGGREGATE);
		break;
		case FS_IS_NEW_MAX:
			// APS added 5/27/2013
			orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.FS_IS_NEW_MAX);
			break;
		case FS_MAX:
			// APS added 10/11/2013
			orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.FS_MAX);
			//orNode.setNoBacktrack(true);
			break;
		case FS_MAX_MANY:
			// APS added 10/17/2013
			orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.FS_MAX_MANY);
			break;
		case FS_MIN:
			// APS added 10/17/2013
			orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.FS_MIN);
			break;
		case FS_COUNT:
			// APS added 10/11/2013
			orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.FS_COUNT);
			break;
		case SORT:
			orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.SORT);
			this.extendDerivedOrNode(orNode);
			break;
		case LIMIT:
			orNode = new PCGOrNode(BuiltInPredicate.LIMIT_PREDICATE_NAME, bip.getArguments().copy(variableList), BuiltInPredicateType.LIMIT);
			break;
		case TOPK:
			orNode = new PCGOrNode(bip.getPredicateName(), bip.getArguments().copy(variableList), BuiltInPredicateType.TOPK);
			this.extendDerivedOrNode(orNode);
			// remove last two argument in andNode - these are only necessary in topKNode
			int arity = ((PCGAndNode)orNode.getChild(0)).getArity();
			((PCGAndNode)orNode.getChild(0)).getArguments().remove(arity - 1);
			((PCGAndNode)orNode.getChild(0)).getArguments().remove(arity - 2);
			break;
		case UNKNOWN:
		default:
			logger.error("unknown built-in type!");
			throw new CompilerException("unknown built-in type!");
		}

		String output = "FAIL";
		if (orNode != null)
			output = orNode.toString();			
		
		this.deALSContext.logTrace(logger, "Exiting constructBuiltInOrNode with {}", output);

		return orNode;
	}
	
	private PCGOrNode constructBuiltInIfThenElseOrNode(Predicate literal, CompilerVariableList variableList, 
			CompilerVariableList s1VariableList, CliqueBase clique, BasicClique basicClique) {

		this.deALSContext.logTrace(logger, "Entering constructBuiltInIfThenElseOrNode for {}", literal.toString());
				
		CompilerVariableList s2VariableList = new CompilerVariableList();
		CompilerVariableList tempVariableList = new CompilerVariableList();
		CompilerVariableList ifVariableList = new CompilerVariableList();
		CompilerVariableList thenVariableList = new CompilerVariableList();
		CompilerVariableList elseVariableList = new CompilerVariableList();
		
		IfThenElsePredicate itePredicate = (IfThenElsePredicate)literal;
		List<Predicate> ifLiterals 	= itePredicate.getIfLiterals();
		List<Predicate> thenLiterals = itePredicate.getThenLiterals();
		List<Predicate> elseLiterals = itePredicate.getElseLiterals();

		Utilities.getVariables(ifLiterals, ifVariableList);
		Utilities.getVariables(thenLiterals, thenVariableList);
		Utilities.getVariables(elseLiterals, elseVariableList);

		// + means unions
		// x means intersection
		// s1 = (Head + Previous)
		// s2 = (If + Then + Else)
		// s3 = (Then x Else)

		// Computing s1 - provided as input parameter

		// Computing s2
		Utilities.mergeVariableLists(s2VariableList, ifVariableList);
		Utilities.mergeVariableLists(s2VariableList, thenVariableList);
		Utilities.mergeVariableLists(s2VariableList, elseVariableList);				

		// Computing s3
		CompilerVariableList s3VariableList = new CompilerVariableList();
		Utilities.getIntersectingVariables(thenVariableList, elseVariableList, s3VariableList);

		// Computing iteArguments = (s1 x s2) + s3
		CompilerVariableList iteArguments = new CompilerVariableList();
		Utilities.getIntersectingVariables(s1VariableList, s2VariableList, iteArguments);
		Utilities.mergeVariableLists(iteArguments, s3VariableList);

		// Computing ifArguments = (s1 + Then ) x If)
		Utilities.mergeVariableLists(tempVariableList, s1VariableList);
		Utilities.mergeVariableLists(tempVariableList, thenVariableList);
		CompilerVariableList ifArguments = new CompilerVariableList();
		Utilities.getIntersectingVariables(tempVariableList, ifVariableList, ifArguments);

		// Computing thenArguments = ((s1 + If) x Then) + s3
		tempVariableList.clear();
		Utilities.mergeVariableLists(tempVariableList, s1VariableList);
		Utilities.mergeVariableLists(tempVariableList, ifVariableList);
		
		CompilerVariableList thenArguments = new CompilerVariableList();
		Utilities.getIntersectingVariables(tempVariableList, thenVariableList, thenArguments);
		Utilities.mergeVariableLists(thenArguments, s3VariableList);

		// Computing elseArguments = (s1 x Else) + s3
		CompilerVariableList elseArguments = new CompilerVariableList();
		Utilities.getIntersectingVariables(s1VariableList, elseVariableList, elseArguments);
		Utilities.mergeVariableLists(elseArguments, s3VariableList);

		if (this.deALSContext.isDebugEnabled()) {
			this.deALSContext.logDebug(logger, "if literals = ");
			for (int i = 0; i < ifLiterals.size(); i++)
				this.deALSContext.logInfo(logger, i + ") " + ifLiterals.get(i).toString());
			
			logger.debug("then literals ="); 
			for (int i = 0; i < thenLiterals.size(); i++)
				logger.debug(i + ") " + thenLiterals.get(i).toString());
			
			logger.debug("else literals =");
			for (int i = 0; i < elseLiterals.size(); i++)
				logger.debug(i + ") " + elseLiterals.get(i).toString());
			
			logger.debug("ifVariableList = ");
			for (int i = 0; i < ifVariableList.size(); i++)
				logger.debug(i + ") " + ifVariableList.get(i).toString());
			
			logger.debug("thenVariableList = ");
			for (int i = 0; i < thenVariableList.size(); i++)
				logger.debug(i + ") " + thenVariableList.get(i).toString());
			
			logger.debug("elseVariableList = ");
			for (int i = 0; i < elseVariableList.size(); i++)
				logger.debug(i + ") " + elseVariableList.get(i).toString());
			
			logger.debug("s1VariableList = ");
			for (int i = 0; i < s1VariableList.size(); i++)
				logger.debug(i + ") " + s1VariableList.get(i).toString());

			logger.debug("s2VariableList = ");
			for (int i = 0; i < s2VariableList.size(); i++)
				logger.debug(i + ") " + s2VariableList.get(i).toString());
			
			logger.debug("s3VariableList = ");
			for (int i = 0; i < s3VariableList.size(); i++)
				logger.debug(i + ") " + s3VariableList.get(i).toString());
			
			logger.debug("iteArguments = ");
			for (int i = 0; i < iteArguments.size(); i++)
				logger.debug(i + ") " + iteArguments.get(i).toString());
			
			logger.debug("ifArguments = ");
			for (int i = 0; i < ifArguments.size(); i++)
				logger.debug(i + ") " + ifArguments.get(i).toString());
			
			logger.debug("thenArguments = ");
			for (int i = 0; i < thenArguments.size(); i++)
				logger.debug(i + ") " + thenArguments.get(i).toString());
			
			logger.debug("elseArguments = ");
			for (int i = 0; i < elseArguments.size(); i++)
				logger.debug(i + ") " + elseArguments.get(i).toString());
		}

		//PCGOrNode orNode = new PCGOrNode(literal.getPredicateName(), iteArguments.copy(variableList), BuiltInPredicateType.IFTHENELSE);
		PCGOrNode orNode = this.createBuiltInOrNode(literal.getPredicateName(), iteArguments.toCompilerTypeList(), BuiltInPredicateType.IFTHENELSE, variableList);
		//PCGOrNode orNode = new PCGOrNode(literal.getPredicateName(), iteArguments.toCompilerTypeList().copy(variableList), BuiltInPredicateType.IFTHENELSE);	
		PCGAndNode ifAndNode = this.constructBuiltInAndNode(BuiltInPredicate.IF_AND_NODE_PREDICATE_NAME, ifArguments.toCompilerTypeList(), ifLiterals, variableList, clique, basicClique);
		PCGAndNode thenAndNode = this.constructBuiltInAndNode(BuiltInPredicate.THEN_AND_NODE_PREDICATE_NAME, thenArguments.toCompilerTypeList(), thenLiterals, variableList, clique, basicClique);	
		PCGAndNode elseAndNode = this.constructBuiltInAndNode(BuiltInPredicate.ELSE_AND_NODE_PREDICATE_NAME, elseArguments.toCompilerTypeList(), elseLiterals, variableList, clique, basicClique);

		if ((ifAndNode != null) && (thenAndNode != null) && (elseAndNode != null)) {
			orNode.addChild(ifAndNode);
			orNode.addChild(thenAndNode);
			orNode.addChild(elseAndNode); 
		} else {
			orNode.delete();
			if (ifAndNode != null) ifAndNode.delete();
			if (thenAndNode != null) thenAndNode.delete();
			if (elseAndNode != null) elseAndNode.delete();
			orNode = null;
			logger.error("Failure to construct if-then-else andNode in Extractor");
			throw new CompilerException("Failure to construct if-then-else andNode in Extractor");
		}

		this.deALSContext.logTrace(logger, "Exiting constructBuiltInIfThenElseOrNode with {}", orNode.toString());

		return orNode;
	}
	
	private PCGOrNode constructBuiltInIfThenOrNode(Predicate literal, CompilerVariableList variableList, CompilerVariableList s1VariableList, 
			CliqueBase clique, BasicClique basicClique) {
		
		this.deALSContext.logTrace(logger, "Entering constructBuiltInIfThenOrNode for {}", literal.toString());
		
		CompilerVariableList s2VariableList = new CompilerVariableList();
		CompilerVariableList tempVariableList = new CompilerVariableList();
		CompilerVariableList ifVariableList = new CompilerVariableList();
		CompilerVariableList thenVariableList = new CompilerVariableList();

		IfThenPredicate itPredicate = (IfThenPredicate)literal;
		List<Predicate> ifLiterals = itPredicate.getIfLiterals();
		List<Predicate> thenLiterals = itPredicate.getThenLiterals();

		Utilities.getVariables(ifLiterals, ifVariableList);
		Utilities.getVariables(thenLiterals, thenVariableList);

		// + means unions
		// x means intersection
		// s1 = (Head + Previous)
		// s2 = (If + Then)

		// Computing s1 - provided as input parameter

		// Computing s2
		Utilities.mergeVariableLists(s2VariableList, ifVariableList);
		Utilities.mergeVariableLists(s2VariableList, thenVariableList);

		// Computing itArguments = (s1 x s2)
		CompilerVariableList itArguments = new CompilerVariableList(); 
		Utilities.getIntersectingVariables(s1VariableList, s2VariableList, itArguments);

		// Computing ifArguments = (s1 + Then ) x If)
		Utilities.mergeVariableLists(tempVariableList, s1VariableList);
		Utilities.mergeVariableLists(tempVariableList, thenVariableList);
		CompilerVariableList ifArguments = new CompilerVariableList();
		Utilities.getIntersectingVariables(tempVariableList, ifVariableList, ifArguments);

		// Computing thenArguments = ((s1 + If) x Then) + s3
		tempVariableList.clear();
		Utilities.mergeVariableLists(tempVariableList, s1VariableList);
		Utilities.mergeVariableLists(tempVariableList, ifVariableList);
		CompilerVariableList thenArguments = new CompilerVariableList();
		Utilities.getIntersectingVariables(tempVariableList, thenVariableList, thenArguments);

		//PCGOrNode orNode = new PCGOrNode(literal.getPredicateName(), itArguments.copy(variableList), BuiltInPredicateType.IFTHEN);		
		//PCGOrNode orNode = new PCGOrNode(literal.getPredicateName(), itArguments.toCompilerTypeList().copy(variableList), BuiltInPredicateType.IFTHEN);
		PCGOrNode orNode = this.createBuiltInOrNode(literal.getPredicateName(), itArguments.toCompilerTypeList(), BuiltInPredicateType.IFTHEN, variableList);
		PCGAndNode ifAndNode = this.constructBuiltInAndNode(BuiltInPredicate.IF_AND_NODE_PREDICATE_NAME, ifArguments.toCompilerTypeList(), ifLiterals, variableList, clique, basicClique);
		PCGAndNode thenAndNode = this.constructBuiltInAndNode(BuiltInPredicate.THEN_AND_NODE_PREDICATE_NAME, thenArguments.toCompilerTypeList(), thenLiterals, variableList, clique, basicClique);

		if ((ifAndNode != null) && (thenAndNode != null)) {
			orNode.addChild(ifAndNode);
			orNode.addChild(thenAndNode);
		} else {
			orNode.delete();
			if (ifAndNode != null) ifAndNode.delete();
			if (thenAndNode != null) thenAndNode.delete();
			orNode = null;
			logger.error("Failure to construct if-then and node in Extractor");
			throw new CompilerException("Failure to construct if-then and node in Extractor");
		}

		this.deALSContext.logTrace(logger, "Exiting constructBuiltInIfThenOrNode with {}", orNode.toString());

		return orNode;
	}
	
	private PCGAndNode constructBuiltInAndNode(String predicateName, CompilerTypeList arguments, List<Predicate> literals, 
			CompilerVariableList variableList, CliqueBase clique, BasicClique basicClique) {
		this.deALSContext.logTrace(logger, "Entering constructBuiltInAndNode for {}", predicateName);

		CompilerVariableList previousVariableList = new CompilerVariableList();
		PCGOrNode orNode;
		PCGAndNode andNode = new PCGAndNode(predicateName, arguments.copy(variableList));

		Utilities.getVariables(arguments, previousVariableList);

		for (Predicate literal : literals) {
			orNode = this.constructOrNode(literal, variableList, previousVariableList, clique, basicClique);
			if ((literal != null) && (orNode != null)) {
				andNode.addChild(orNode);
				
				if (Extractor.hasLastFalseNode(orNode))
					break;

				Utilities.getVariables(literal, previousVariableList);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting constructBuiltInAndNode with {}", andNode.toString());

		return andNode;
	}
	
	private CliqueBase constructClique(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering constructClique for {}", orNode.toString());
				
		String 	predicateName 	= orNode.getPredicateName();
		int				arity 			= orNode.getArity();
		CliqueBase		baseClique 		= null;
				
		// if we find this clique is already constructed, we do nothing
		for (CliqueBase constructedClique : this.constructedCliques) {
			if (constructedClique instanceof Clique) {
				if (constructedClique.getCliquePredicate(predicateName, arity) != null) { 
					baseClique = constructedClique;
					break;
				}
			} else if (constructedClique instanceof XYClique) {
				// have to subtract 1 from the arity because of stage variable
				if (constructedClique.getCliquePredicate(predicateName, arity) != null) { 
					baseClique = constructedClique;
					break;
				}
			}
		}
		
		if (baseClique == null) {
			BasicClique clique = this.module.getBasicClique(predicateName, arity);
			if (clique != null) {
				// APS 3/5/2013 - reworked to match output on cheetah
				/*if ((this.rcaGenerator.checkRCAClique(basicClique)
						&& ((baseClique = (BaseClique) this.constructCliqueFromRcaClique(basicClique)) != null))
					|| ((baseClique = (BaseClique) constructXYCliqueFromRcaClique(basicClique)) != null)) {
						this.constructedCliques.appendElement(baseClique);	
					} //this.rcaGenerator.checkRCAClique(basicClique) &&
				*/
				
				if (((baseClique = constructXYCliqueFromBasicClique(clique)) != null) 
					|| ((baseClique = this.constructCliqueFromBasicClique(clique)) != null)) {	
					this.constructedCliques.add(baseClique);
					this.programRules.addClique(clique);
				}
				/*
				if ((baseClique = constructXYCliqueFromBasicClique(clique)) == null)
					baseClique = this.constructCliqueFromBasicClique(clique);*/
			}
		}

		this.deALSContext.logTrace(logger, "Exiting constructClique with status = {}", (baseClique != null));
				
		return baseClique;
	}
	
	private Clique constructCliqueFromBasicClique(BasicClique basicClique) {
		this.deALSContext.logTrace(logger, "Entering constructCliqueFromBasicClique for {}", basicClique.toString());
		
		Clique clique = new Clique(basicClique.getCliqueId());
		
		//this.constructedCliques.add(clique);
		
		CliquePredicate	cliquePredicate;
		PCGAndNode 		andNode;
		boolean		hasRules			= false;
		boolean		isRecursive 		= false;
		boolean		isBadClique			= false;

		for (DerivedPredicate derivedPredicate : basicClique.getDerivedPredicates()) {		
			cliquePredicate = new CliquePredicate(derivedPredicate.getPredicateName(), derivedPredicate.getArity());
			
			for (Rule rule : derivedPredicate.getRules()) {
				Pair<PCGAndNode, Boolean> retvalPair = this.constructCliqueAndNodeFromRule(rule, basicClique, clique);
				andNode = retvalPair.getFirst();
				isRecursive = retvalPair.getSecond();

				if (andNode != null) {
					if (isRecursive)
						cliquePredicate.addRecursiveRule(andNode);
					else
						cliquePredicate.addExitRule(andNode);

					hasRules = true;
				} else {
					isBadClique = true;
					break;
				}
			}
			
			if (isBadClique)
				break;
			
			clique.addCliquePredicate(cliquePredicate);
		}

		// there must be at least some rules in the clique
		if (!hasRules || isBadClique) {
			//this.constructedCliques.remove(clique);
			clique = null;
		}
		
		this.deALSContext.logTrace(logger, "Exiting constructCliqueFromBasicClique with status = {}", (clique != null));
		
		return clique;
	}
	
	private Pair<PCGAndNode, Boolean> constructCliqueAndNodeFromRule(Rule rule, BasicClique basicClique, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering constructCliqueAndNodeFromRule");
		this.deALSContext.logTrace(logger, rule.toString());
		
		PCGAndNode	andNode = null;
		boolean isRecursive = false;

		if (this.isStratified(rule.getBody(), basicClique, rule, true, false)) {
			CompilerVariableList		variableList 			= new CompilerVariableList();
			CompilerVariableList		previousVariableList 	= new CompilerVariableList();
			Predicate			head 					= rule.getHead();
			String				predicateName 			= head.getPredicateName();
			CompilerTypeList	arguments 				= head.getArguments().copy(variableList);
			DerivedPredicate	derivedPredicate;
			PCGOrNode			orNode;

			// APS 6/11/2013
			// Here, we need to make sure there's no stratified aggregate in the rule head
			// recursive aggregates allowed in head of FS rules
			if (head.getPredicateName().startsWith(AggregateRewriter.STRATIFIED_AGGREGATE_NODE_NAME_PREFIX)) {
				logger.error("Stratified aggregate found in recursive rule head.");
				throw new CompilerException("Stratified aggregate found in recursive rule head.");
			}
			
			andNode = new PCGAndNode(predicateName, arguments);
			andNode.setRuleId(rule.getRuleId());
			andNode.setRule(rule);
			andNode.predicate.setFSAggregateType(head.getFSAggregateType());
			andNode.predicate.setArgumentTypeAdornment(head.getArgumentTypeAdornment());

			Utilities.getVariables(head, previousVariableList);

			for (Predicate literal : rule.getBody()) {					
				boolean isFSAggregate = false;
				if (literal instanceof BuiltInPredicate) {
					BuiltInPredicate bip = (BuiltInPredicate)literal;
					isFSAggregate = bip.isReadAggregateFS();
					if (!isFSAggregate)
						isFSAggregate = bip.isFSAggregate();
				}
				
				derivedPredicate = basicClique.getDerivedPredicate(literal.getPredicateName(), literal.getArity());
				if ((literal.isDerived() || isFSAggregate) && (derivedPredicate != null)) {
					isRecursive = true;
				}

				orNode = this.constructOrNode(literal, variableList, previousVariableList, clique, basicClique);
				if (orNode != null) {
					andNode.addChild(orNode);					
					
					if (Extractor.hasLastFalseNode(orNode))
						break;
				}

				Utilities.getVariables(literal, previousVariableList);
			}
			
			for (int i = 0; i < variableList.size(); i++)
				if (variableList.get(i).isAnonymous())
					variableList.get(i).renameVariableName();
		}

		this.deALSContext.logTrace(logger, "Exiting constructCliqueAndNodeFromRule");
				
		return new Pair<>(andNode, isRecursive);
	}
	
	private PCGAndNode constructXYCliqueAndNodeFromRule(Rule rule, BasicClique basicClique, XYClique xyClique) {
		if (rule.getHead().containsAnyAggregate()) {
			logger.error("No aggregate in head of rule at this point!  Should have already been rewritten.");
			throw new RuntimeException("No aggregate in head of rule at this point!  Should have already been rewritten.");
			//return this.constructPcgAndNodeForAggregateRule(rule, null, null);
		}

		this.deALSContext.logTrace(logger, "Entering constructXYCliqueAndNodeFromRule for\n{}\n", rule.toString());
				
		CompilerVariableList variableList = new CompilerVariableList();
		/*if (parentOrNode != null) {
			VariableList parentArgumentVariables = new VariableList();
			Utilities.getVariables(basicClique.getArguments(), parentArgumentVariables);
			variableList.appendList(parentArgumentVariables);
		}*/
		Predicate head = rule.getHead();	

		PCGAndNode andNode = new PCGAndNode(head.getPredicateName(), head.getArguments().copy(variableList));
		andNode.setRuleId(rule.getRuleId());
		andNode.setRule(rule);
		
		CompilerVariableList previousVariableList = new CompilerVariableList();
		Utilities.getVariables(head, previousVariableList);

		PCGOrNode orNode;		
		
		for (Predicate literal : rule.getBody()) {
			if ((orNode = this.constructOrNode(literal, variableList, previousVariableList, xyClique, basicClique)) != null) {
				andNode.addChild(orNode);

				if (Extractor.hasLastFalseNode(orNode))
					break;
			}

			Utilities.getVariables(literal, previousVariableList);
		}
		
		for (int i = 0; i < variableList.size(); i++)
			if (variableList.get(i).isAnonymous())
				variableList.get(i).renameVariableName();

		this.deALSContext.logTrace(logger, "Exiting constructXYCliqueAndNodeFromRule with {}", andNode.toString());
				
		return andNode;
	}
	
	private PCGAndNode constructXYCliqueAndNodeFromDeleteRule(Rule rule, BasicClique basicClique, XYClique xyClique, 
			XYCliquePredicate xyCliquePredicate) {

		this.deALSContext.logTrace(logger, "Entering constructXYCliqueAndNodeFromDeleteRule");
		this.deALSContext.logTrace(logger, rule.toString());
				
		PCGAndNode andNode = null;
		CompilerVariableList variableList;
		CompilerVariableList previousVariableList;
		Predicate head = rule.getHead();
		PCGOrNode orNode;

		for (Predicate literal : rule.getBody()) {
			if (!literal.isNegative())
				continue;

			variableList = new CompilerVariableList();
			previousVariableList = new CompilerVariableList();
			andNode = new PCGAndNode(head.getPredicateName(), head.getArguments().copy(variableList));
			andNode.setRuleId(rule.getRuleId());
			andNode.setRule(rule);
			Utilities.getVariables(head, previousVariableList);

			if ((orNode = this.constructOrNode(literal, variableList, previousVariableList, xyClique, basicClique)) != null) {
				orNode.getPredicate().setAsPositive();
				andNode.addChild(orNode);
			}

			for (Predicate literal2 : rule.getBody()) {
				if (literal2.isNegative())
					continue;

				if ((orNode = this.constructOrNode(literal2, variableList, previousVariableList, xyClique, basicClique)) != null) {
					if (head.getPredicateName().equals(literal2.getPredicateName()))
						andNode.insertChild(1, orNode);
					else 
						andNode.addChild(orNode);
				}
				Utilities.getVariables(literal2, previousVariableList);
			}

			xyCliquePredicate.addDeleteRule(andNode);
		}
		
		this.deALSContext.logTrace(logger, "Exiting constructXYCliqueAndNodeFromDeleteRule with {}", andNode.toString());
		
		return andNode;
	}
	
	private XYClique constructXYCliqueFromBasicClique(BasicClique basicClique) {
		this.deALSContext.logTrace(logger, "Entering constructXYCliqueFromBasicClique");
		this.deALSContext.logTrace(logger, basicClique.toString());

		XYClique xyClique = null;

		List<Rule> 		exitRules = new ArrayList<>();
		List<Rule> 		xRules = new ArrayList<>();
		List<Rule> 		yRules = new ArrayList<>();
		List<Rule> 		copyRules = new ArrayList<>();
		List<Rule> 		deleteRules = new ArrayList<>();
		List<PredicateBase> 	sortedPrimedPredicates = new ArrayList<>();

		XYCliqueAnalyzer xyCliqueAnalyzer = new XYCliqueAnalyzer(this.deALSContext, this.module);
		if (xyCliqueAnalyzer.isXYClique(basicClique, exitRules, xRules, yRules, copyRules, deleteRules, sortedPrimedPredicates)) {
			xyClique = this.createXYClique(basicClique, exitRules, xRules, yRules, copyRules, deleteRules, sortedPrimedPredicates);
			//this.constructedCliques.add(xyClique);
		}

		exitRules.clear();
		xRules.clear();
		yRules.clear();
		copyRules.clear();
		deleteRules.clear();
		sortedPrimedPredicates.clear();
	
		if (xyClique != null)
			this.deALSContext.logTrace(logger, xyClique.toString());

		this.deALSContext.logTrace(logger, "Exiting constructXYCliqueFromBasicClique with status = {}", (xyClique != null));
		
		return xyClique;
	}	
	/*END CONSTRUCT METHODS */
	
	/*BEGIN EXTEND METHODS*/
	/*These methods extend the PCG subtree by adding child nodes to the subtree*/
	
	public boolean extendOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering extendOrNode for {}", orNode.toString());
		
		boolean status = true;
		
		if (!orNode.getExtended()) {
			if (orNode.isBuiltInPredicate()) {
				status = this.extendBuiltInOrNode(orNode);
			} else if (orNode.isRecursive()) {
				status = this.extendRecursiveOrNode(orNode);
			} else {
				status = this.extendBaseOrNode(orNode);
	
				if (status)
					orNode.addUsedBasePredicate(orNode.getPredicate());
				else
					status = this.extendDerivedOrNode(orNode);				
			}
			
			if (status)
				orNode.setExtended(true);
		
			this.deALSContext.logTrace(logger, orNode.toStringUsedBasePredicates());
			this.deALSContext.logTrace(logger, "Exiting extendOrNode for {} with status = {}", orNode.toString(), status);
		} else {
			this.deALSContext.logTrace(logger, "Exiting extendOrNode for {}.  Node already extended.");			
		}

		return status;
	}

	private boolean extendBaseOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering extendBaseOrNode for {}", orNode.toString());
				
		boolean status = false;
		BasePredicate basePredicate = this.module.getBasePredicate(orNode.getPredicateName());

		if (basePredicate != null) {
			if (!orNode.getChildren().contains(basePredicate)) {
				orNode.setBasePredicate(basePredicate);
				orNode.getPredicate().setAsBasePredicate();
				
				this.programRules.addBasePredicate(basePredicate);
			}
			status = true;
		}

		this.deALSContext.logTrace(logger, "Exiting extendBaseOrNode for {} with status = {}", orNode.toString(), status);
		
		return status;
	}

	private boolean extendDerivedOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering extendDerivedOrNode for {}", orNode.toString());
		
		boolean status = false;
		
		DerivedPredicate derivedPredicate = this.module.getDerivedPredicate(orNode.getPredicateName(), orNode.getArity());

		if ((derivedPredicate != null) && derivedPredicate.getNumberOfRules() > 0) {
			PCGAndNode andNode;
			
			this.programRules.addDerivedPredicate(derivedPredicate);
			
			for (Rule rule : derivedPredicate.getRules()) {
				andNode = this.constructAndNodeFromRule(rule);

				if (Unifier.unifyTerms(andNode.getArguments(), orNode.getArguments(), andNode)) {
					this.extendAndNode(andNode);
					
					orNode.addChild(andNode);
					
					this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);

					if (andNode.noBacktrack())
						orNode.setNoBacktrack(true);

				} else {
					andNode.delete();
				}
			}

			if (orNode.getNumberOfChildren() > 0) {
				// we found at least one derived rule
				status = true;
			} else if (orNode.getPredicateName().equals(BuiltInPredicate.RETURN_PREDICATE_NAME) 
					&& orNode.getArity() == 4) {
				status = Extractor.extendWithDefaultReturnRule(orNode);
			} else {
				this.deALSContext.logError(logger, "Predicate can not be extracted because there is no matching rule: {}", orNode.toStringAsPredicate());
				status = false;
			}
		} else if (orNode.getPredicateName().equals(BuiltInPredicate.RETURN_PREDICATE_NAME) 
				&& orNode.getArity() == 4) {
			status = Extractor.extendWithDefaultReturnRule(orNode);
		} else {
			throw new CompilerException("Predicate can not be extracted or is undefined. OrNode = " + orNode.toStringAsPredicate());
		}

		this.deALSContext.logTrace(logger, "Exiting extendDerivedOrNode for {} with status = {}", orNode.toString(), status);
		
		return status;
	}
	
	private boolean extendDerivedOrNode(PCGOrNode orNode, CompilerVariableList variableList) {
		this.deALSContext.logTrace(logger, "Entering extendDerivedOrNode for {}", orNode.toString());
		
		boolean status = false;
		
		DerivedPredicate derivedPredicate = this.module.getDerivedPredicate(orNode.getPredicateName(), orNode.getArity());

		if ((derivedPredicate != null) && derivedPredicate.getNumberOfRules() > 0) {
			PCGAndNode andNode;
			
			this.programRules.addDerivedPredicate(derivedPredicate);
			
			for (Rule rule : derivedPredicate.getRules()) {
				andNode = this.constructAndNodeFromRule(rule, variableList);

				if (Unifier.unifyTerms(andNode.getArguments(), orNode.getArguments(), andNode)) {
					this.extendAndNode(andNode);
					
					orNode.addChild(andNode);
					
					this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);

					if (andNode.noBacktrack())
						orNode.setNoBacktrack(true);

				} else {
					andNode.delete();
				}
			}

			if (orNode.getNumberOfChildren() > 0) {
				// we found at least one derived rule
				status = true;
			} else if (orNode.getPredicateName().equals(BuiltInPredicate.RETURN_PREDICATE_NAME) 
					&& orNode.getArity() == 4) {
				status = Extractor.extendWithDefaultReturnRule(orNode);
			} else {
				this.deALSContext.logError(logger, "Predicate can not be extracted because there is no matching rule: {}", orNode.toStringAsPredicate());
				status = false;
			}
		} else if (orNode.getPredicateName().equals(BuiltInPredicate.RETURN_PREDICATE_NAME) 
				&& orNode.getArity() == 4) {
			status = Extractor.extendWithDefaultReturnRule(orNode);
		} else {
			throw new CompilerException("Predicate can not be extracted or is undefined. OrNode = " + orNode.toStringAsPredicate());
		}

		this.deALSContext.logTrace(logger, "Exiting extendDerivedOrNode for {} with status = {}", orNode.toString(), status);
		
		return status;
	}

	private void extendAndNode(PCGAndNode andNode) {
		this.deALSContext.logTrace(logger, "Entering extendAndNode for {}", andNode.toString());

		//int count = 0;
		for (PCGOrNode orNode : andNode.getChildren()) {
			this.extendOrNode(orNode);
			if (orNode.noBacktrack())
				andNode.setNoBacktrack(true);
		}

		this.deALSContext.logTrace(logger, "Exiting extendAndNode for {}", andNode.toString());
	}

	private void extendUsedBaseAndUpdatedPredicates(PCGOrNode orNode, PCGAndNode andNode) {
		this.deALSContext.logTrace(logger, "Entering extendUsedBaseAndUpdatedPredicates for {}", orNode.toString());
		
		orNode.collectUsedBasePredicates(andNode);

		this.deALSContext.logTrace(logger, "Exiting extendUsedBaseAndUpdatedPredicates for {}", orNode.toString());
	}

	private boolean extendBuiltInOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering extendBuiltInOrNode for {}", orNode.toString());
		
		boolean status = true;

		if (orNode.getPredicate() instanceof BuiltInPredicate) {
			switch (orNode.getBuiltInPredicateType())
			{
			case TRUE:
			case FALSE:
			case BINARY:
			case SINGLE:
			case MULTI:
			case RETURN:
			case CHOICE:
			case GENERIC:
				break;

			/*case AGGREGATE:
			{
				PCGAndNode aggregateAndNode = (PCGAndNode)orNode.getChild(0);

				if (Unifier.unifyTerms(aggregateAndNode.getArguments(), orNode.getArguments(), aggregateAndNode)) {
					this.extendAndNode(aggregateAndNode);
					Extractor.extendUsedBaseAndUpdatedPredicates(orNode, aggregateAndNode);
				} else {
					aggregateAndNode.delete();
					throw new CompilerException("Can not create aggregate node in extraction");
				}
			}
			break;*/
			case IFTHEN:			
			{
				PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0);
				PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1);

				// We should be able to do something more sophisticated such that we
				//  rewrite the if-then-else when we can not construct the if or then or else
				this.extendAndNode(ifAndNode);
				this.extendUsedBaseAndUpdatedPredicates(orNode, ifAndNode);

				this.extendAndNode(thenAndNode);
				this.extendUsedBaseAndUpdatedPredicates(orNode, thenAndNode);
			}
			break;
			
			case IFTHENELSE:
			{
				PCGAndNode ifAndNode = (PCGAndNode)orNode.getChild(0);
				PCGAndNode thenAndNode = (PCGAndNode)orNode.getChild(1);
				PCGAndNode elseAndNode = (PCGAndNode)orNode.getChild(2);
				
				// We should be able to do something more sophisticated such that we
				//  rewrite the if-then-else when we can not construct the if or then or else
				this.extendAndNode(ifAndNode);
				this.extendUsedBaseAndUpdatedPredicates(orNode, ifAndNode);

				this.extendAndNode(thenAndNode);
				this.extendUsedBaseAndUpdatedPredicates(orNode, thenAndNode);

				this.extendAndNode(elseAndNode);
				this.extendUsedBaseAndUpdatedPredicates(orNode, elseAndNode);			
			}
			break;
			
			case AGGREGATE: // APS 6/17/2014
			case AGGREGATE_FS: //APS 6/17/2014
			case READ_USER_DEFINED_AGGREGATE: //HW
			case READ_USER_DEFINED_AGGREGATE_FS: //@DATALOGFS APS 3/20/2013
			{
				if (orNode.children.size() > 0) {
					this.extendAndNode((PCGAndNode)orNode.getChild(0));
				} else {
					CompilerVariableList variableList = new CompilerVariableList();
					Utilities.getVariables(orNode, variableList);
					status = this.extendDerivedOrNode(orNode, variableList);
				}
				//PCGAndNode andNode = new PCGAndNode(orNode.getPredicateName(), orNode.getArguments().copy());
				//andNode.setPredicateType(PredicateType.BUILT_IN);
				//PCGAndNode andNode = (PCGAndNode)orNode.getChild(0);
				//this.extendAndNode(andNode);
				//Extractor.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}
				break;
			//case READ_AGGREGATE_FS:
			case WRITE_USER_DEFINED_AGGREGATE:
			case SINGLE_MULTI_USER_DEFINED_AGGREGATE: //HW
			case WRITE_USER_DEFINED_AGGREGATE_FS: //@DATALOGFS APS 3/20/2013			
			//case LAST_STAGE_FS: //APS 4/5/2013 @DATALOGFS
			case FS_IS_NEW_MAX: //APS 5/27/2013 @DATALOGFS
			case FS_MAX: //APS 10/11/2013 @DATALOGFS
			case FS_MAX_MANY: //APS 10/17/2013 @DATALOGFS
			case FS_COUNT: //APS 10/11/2013 @DATALOGFS
			case FS_MIN: //APS 10/17/2013 @DATALOGFS
			case LIMIT: // 10/25/2014
			case SORT: // 10/25/2014		
			case TOPK: // 3/31/2015 
				break;
			case UNKNOWN:
			default:
			{
				status = false;
				logger.error("unknown built-in type in extraction!!!");
				throw new CompilerException("unknown built-in type in extraction!!!");				
			}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting extendBuiltInOrNode for {} with status = {}", orNode.toString(), status);

		return status;
	}
	
	private boolean extendRecursiveOrNode(PCGOrNode orNode) {
		this.deALSContext.logTrace(logger, "Entering extendRecursiveOrNode for {}", orNode.toString());

		boolean status = true;
		CliqueBase baseClique = orNode.getBaseClique();

		if (baseClique == null) {
			status = false;
		} else if (!this.visitedCliques.contains(baseClique)) {
			
			this.visitedCliques.add(baseClique);
			
			switch (baseClique.getType())
			{
				case CLIQUE: {
					this.extendClique(orNode, (Clique)baseClique);
				}
				break;
	
				case XY_CLIQUE: {
					this.extendXYClique(orNode, (XYClique)baseClique);
				}
				break;
	
				default: {
					status = false;
				}
				break;
			}
		} else {
			// Find a parent with some used base or update relations
			// if there is none, it is NOT an error because that is potentially possible.
			// if there is one, then we can be sure that it is not a local pcg orNode within the clique.
			for (PCGOrNode parent : baseClique.getParents()) {
				if (parent.getNumberOfUsedBasePredicates() > 0/* || parent.getNumberOfUsedUpdatedPredicates() > 0*/) {
					for (PredicateBase basePredicate : parent.getUsedBasePredicates())
						orNode.addUsedBasePredicate(basePredicate);					
					//for (PredicateBase updatedPredicate : parent.getUsedUpdatedPredicates())
					//	orNode.addUsedUpdatedPredicate(updatedPredicate);
					break;
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting extendRecursiveOrNode for {} with status = {}", orNode.toString(), status);
		
		return status;
	}

	private void extendClique(PCGOrNode orNode, Clique clique) {
		this.deALSContext.logTrace(logger, "Entering extendClique for {}", orNode.toString());
		
		for (CliquePredicate cliquePredicate : clique.getCliquePredicates()) {
			for (PCGAndNode andNode : cliquePredicate.getExitRules()) {
				this.extendCliqueAndNode(andNode, clique);
				this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}

			for (PCGAndNode andNode : cliquePredicate.getRecursiveRules()) {
				this.extendCliqueAndNode(andNode, clique);
				this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting extendClique for {}", orNode.toString());
	}

	private void extendXYClique(PCGOrNode orNode, XYClique xyClique) {
		this.deALSContext.logTrace(logger, "Entering extendXYClique for {}", orNode.toString());
				
		for (XYCliquePredicate xyCliquePredicate : xyClique.getCliquePredicates()){

			for (PCGAndNode andNode : xyCliquePredicate.getExitRules()) {
				this.extendCliqueAndNode(andNode, xyClique);
				this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}

			for (PCGAndNode andNode : xyCliquePredicate.getXRules()) {
				this.extendCliqueAndNode(andNode, xyClique);
				this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}

			for (PCGAndNode andNode : xyCliquePredicate.getYRules()) {
				this.extendCliqueAndNode(andNode, xyClique);
				this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}

			for (PCGAndNode andNode : xyCliquePredicate.getCopyRules()) {
				this.extendCliqueAndNode(andNode, xyClique);
				this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}

			for (PCGAndNode andNode : xyCliquePredicate.getDeleteRules()) {
				this.extendCliqueAndNode(andNode, xyClique);
				this.extendUsedBaseAndUpdatedPredicates(orNode, andNode);
			}
		}

		this.deALSContext.logTrace(logger, "Exiting extendXYClique for {}", orNode.toString());
	}

	private void extendCliqueAndNode(PCGAndNode andNode, CliqueBase baseClique) {
		this.deALSContext.logTrace(logger, "Entering extendCliqueAndNode for {}", andNode.toString());
				
		int count = 0;
		for (PCGOrNode orNode : andNode.getChildren()) {
			if (!orNode.isRecursiveLiteral(baseClique) && !this.extendOrNode(orNode)) {
				// when we fail to generate pcg or node, we delete this pcg or node
				//  and those after this and we create a false predicate in its place
				for (int j = andNode.getNumberOfChildren() - 1; j >= count; j--)
					andNode.removeChild(j).delete();
				
				orNode = new PCGOrNode(BuiltInPredicate.FALSE_PREDICATE_NAME, new CompilerTypeList(), BuiltInPredicateType.FALSE);
				andNode.addChild(orNode);
				break;
			}

			count++;
		}

		this.deALSContext.logTrace(logger, "Exiting extendCliqueAndNode for {}", andNode.toString());
	}
	
	private static boolean extendWithDefaultReturnRule(PCGOrNode orNode) {
		CompilerTypeList args = new CompilerTypeList();
		args.add(orNode.getArguments().get(0).copy());
		args.add(new CompilerString(CompilerTypeBase.NIL));
		CompilerVariable x = new CompilerVariable("X");
		args.add(x);
		args.add(x);
		
		PCGAndNode andNode = new PCGAndNode(BuiltInPredicate.RETURN_PREDICATE_NAME, args);

		if (Unifier.unifyTerms(andNode.getArguments(), orNode.getArguments(), andNode)) {
			orNode.addChild(andNode);
			return true;
		}
		
		return false;		
	}
	
	/*END EXTEND METHODS*/
	
	/*BEGIN CREATE METHODS */	
	private PCGOrNode createOrNode(Predicate literal, CompilerVariableList variableList) {
		this.deALSContext.logTrace(logger, "Entering createOrNode for {}", literal.toString());

		PCGOrNode orNode = new PCGOrNode(literal.getPredicateName(), literal.getArguments().copy(variableList));
		
		orNode.setPredicateType(literal.getPredicateType());
		
		if (literal.getPredicateType() == PredicateType.BUILT_IN) {
			BuiltInPredicate bip = (BuiltInPredicate)literal;
			orNode.setBuiltInPredicateType(bip.getBuiltInPredicateType());
		}
		
		if (literal.isRecursive())
			orNode.setAsRecursive();
		
		orNode.setPredicateOperatorType(literal.getPredicateOperatorType());
		orNode.getPredicate().setArgumentTypeAdornment(literal.getArgumentTypeAdornment());
		orNode.getPredicate().setFSAggregateType(literal.getFSAggregateType());	

		this.deALSContext.logTrace(logger, "Exiting createOrNode with {}", orNode.toString());

		return orNode;
	}
	
	private PCGOrNode createBuiltInOrNode(String predicateName, CompilerTypeList arguments, 
			BuiltInPredicateType builtInType, CompilerVariableList variableList) {
		this.deALSContext.logTrace(logger, "Entering createBuiltInOrNode for {} ({}) {}", predicateName, arguments.size(), builtInType.name());

		PCGOrNode orNode = new PCGOrNode(predicateName, arguments.copy(variableList), builtInType);

		this.deALSContext.logTrace(logger, "Exiting createBuiltInOrNode with {}", orNode.toString());
		
		return orNode;
	}

	private XYClique createXYClique(BasicClique basicClique, List<Rule> exitRules,
			List<Rule> xRules, List<Rule> yRules, List<Rule> copyRules, 
			List<Rule> deleteRules, List<PredicateBase> sortedPrimedPredicates) {
		// THIS METHOD WRAPS THE CORE createXYClique SO WE CAN CONDENSE IT
		// HAVING TO PRINT TRACE OUTPUT LEADS TO LONGER CODE
		this.deALSContext.logTrace(logger, "Entering createXYClique");
		this.deALSContext.logTrace(logger, basicClique.toString());
				
		XYClique xyClique = this.createXYClique2(basicClique, exitRules, xRules, 
				yRules, copyRules, deleteRules, sortedPrimedPredicates);
		
		this.deALSContext.logTrace(logger, basicClique.toString());
		this.deALSContext.logTrace(logger, "Exiting createXYClique with status = {}", (xyClique != null));
		
		return xyClique;
	}	
	
	private XYClique createXYClique2(BasicClique basicClique, List<Rule> exitRules,
			List<Rule> xRules, List<Rule> yRules, List<Rule> copyRules, 
			List<Rule> deleteRules, List<PredicateBase> sortedPrimedPredicates) {

		XYClique xyClique = new XYClique(basicClique.getCliqueId());
		XYCliquePredicate xyCliquePredicate;
		PCGAndNode andNode;

		for (PredicateBase relation : sortedPrimedPredicates) {
			xyCliquePredicate = new XYCliquePredicate(relation.getPredicateName(), relation.getArity());
			xyClique.addCliquePredicate(xyCliquePredicate);
		}

		for (Rule rule : exitRules) {
			if ((andNode = this.constructXYCliqueAndNodeFromRule(rule, basicClique, xyClique)) != null) {
				if ((xyCliquePredicate = xyClique.getCliquePredicate(rule.getHead().getPredicateName(), 
						rule.getHead().getArity() - 1)) != null) 
					xyCliquePredicate.addExitRule(andNode);
				else
					return null;
			} else {
				return null;
			}
		}

		for (Rule rule : xRules) {
			if ((andNode = this.constructXYCliqueAndNodeFromRule(rule, basicClique, xyClique)) != null) {				
				if ((xyCliquePredicate = xyClique.getCliquePredicate(rule.getHead().getPredicateName(), 
						rule.getHead().getArity() - 1)) != null)
					xyCliquePredicate.addXRule(andNode);
				else
					return null;
			} else {
				return null;
			}
		}

		// treat unqualified delete-rules as y-rules --HW
		censorXYCliqueDeleteRules(yRules, deleteRules, sortedPrimedPredicates);

		for (Rule rule : yRules) {
			if ((andNode = this.constructXYCliqueAndNodeFromRule(rule, basicClique, xyClique)) != null) {
				if ((xyCliquePredicate = xyClique.getCliquePredicate(rule.getHead().getPredicateName(), 
						rule.getHead().getArity() - 1)) != null)
					xyCliquePredicate.addYRule(andNode);
				else
					return null;
			} else {
				return null;
			}
		}

		for (Rule rule : copyRules) {
			if ((andNode = this.constructXYCliqueAndNodeFromRule(rule, basicClique, xyClique)) != null) {
				if ((xyCliquePredicate = xyClique.getCliquePredicate(rule.getHead().getPredicateName(), 
						rule.getHead().getArity() - 1)) != null)
					xyCliquePredicate.addCopyRule(andNode);
				else
					return null;
			} else {
				return null;
			}
		}

		for (Rule rule : deleteRules) {
			if ((xyCliquePredicate = xyClique.getCliquePredicate(rule.getHead().getPredicateName(), 
					rule.getHead().getArity() - 1)) != null) {
				if ((andNode = this.constructXYCliqueAndNodeFromDeleteRule(rule, basicClique, xyClique, xyCliquePredicate)) == null)
					return null;
			} else {
				return null;
			}
		}

		return xyClique;
	}
	
	/*END CREATE METHODS*/
	
	/*BEGIN HELPERS */
	private boolean isStratified(List<Predicate> literals, BasicClique basicClique, Rule rule, 
			boolean hasRuleLiterals, boolean doAbortOnFailure) {
		this.deALSContext.logTrace(logger, "Entering isStratified");
		
		boolean isStratified = true;

		for (Predicate literal : literals) {			
			boolean isIfThenElse = false;
			boolean isIfThen = false;
			if (literal instanceof BuiltInPredicate) {
				BuiltInPredicate bip = (BuiltInPredicate)literal;
				isIfThenElse = bip.isIfThenElse();
				isIfThen = bip.isIfThen();
			}

			if (isIfThenElse) {
				IfThenElsePredicate iteLiteral = (IfThenElsePredicate) literal;

				if ((!this.isStratified(iteLiteral.getIfLiterals(), basicClique, rule, false, doAbortOnFailure)) 
						|| (!this.isStratified(iteLiteral.getThenLiterals(), basicClique, rule, false, doAbortOnFailure))
						|| (!this.isStratified(iteLiteral.getElseLiterals(), basicClique, rule, false, doAbortOnFailure))) {
					if (doAbortOnFailure) {
						String message = "Unstratified literal " + literal.toString() + " in rule " + rule.toStringIndent(5) + " in clique " + basicClique.getCliqueId() + ".  Recursive literals are not allowed in IfThenElse rule.";
						logger.error(message);
						throw new CompilerException(message);
					}
					
					isStratified = false;
					break;
				}
			} else if (isIfThen) {
				IfThenPredicate itLiteral = (IfThenPredicate) literal;

				if (!(this.isStratified(itLiteral.getIfLiterals(), basicClique, rule, false, doAbortOnFailure)) 
						|| (!this.isStratified(itLiteral.getThenLiterals(), basicClique, rule, false, doAbortOnFailure))) {
					if (doAbortOnFailure) {
						String message = "Unstratified literal " + literal.toString() + " in rule " + rule.toStringIndent(5) + " in clique " + basicClique.getCliqueId() + ".  Recursive literals are not allowed in IfThenElse rule.";
						logger.error(message);
						throw new CompilerException(message);
					}
					isStratified = false;
					break;
				}
			} else {
				DerivedPredicate derivedPredicate = basicClique.getDerivedPredicate(literal.getPredicateName(), literal.getArity());
				if (literal.isDerived() && (derivedPredicate != null)) {
					if (literal.isNegative()) {
						if (doAbortOnFailure) {					
							String message = "Unstratified negated literal " + literal.toString() + " in rule " + rule.toStringIndent(5) + " in clique " + basicClique.getCliqueId();
							logger.error(message);
							throw new CompilerException(message);
						}
						isStratified = false;
					} 
					if (!hasRuleLiterals) {
						if (doAbortOnFailure) {
							String message = "Unstratified literal " + literal.toString() + " in rule " + rule.toStringIndent(5) + " in clique " + basicClique.getCliqueId() + ".  Recursive literals are not allowed in IfThenElse rule.";
							logger.error(message);
							throw new CompilerException(message);
						}
						
						isStratified = false;
					}
					if (!isStratified)
						break;
				}
			}
		}

		this.deALSContext.logTrace(logger, "Exiting isStratified with status = {}", isStratified);

		return isStratified;
	}	
	
	private static boolean hasLastFalseNode(PCGOrNode orNode) {
		if (orNode.getPredicate() instanceof BuiltInPredicate) {
			BuiltInPredicate bip = (BuiltInPredicate)orNode.getPredicate();
			if (bip.isFalse())
				return true;
			else if(bip.isIfThenElse()) {
				// if both (if or then) and else has false as last or node, all orNodes after are irrelevant
				// we ensure that any or nodes after false in the then else are removed as well
				PCGAndNode ifAndNode = (PCGAndNode) orNode.getChild(0);
				PCGAndNode thenAndNode = (PCGAndNode) orNode.getChild(1);
				PCGAndNode elseAndNode = (PCGAndNode) orNode.getChild(2);

				boolean lastIf = false;
				boolean lastThen = false;
				boolean lastElse = false;
				
				PCGOrNode lastIfOrNode = ifAndNode.getLastChild();							
				if (lastIfOrNode.getPredicate() instanceof BuiltInPredicate) {
					BuiltInPredicate lastIfBip = (BuiltInPredicate)lastIfOrNode.getPredicate();
					lastIf = lastIfBip.isFalse();
				}
											
				PCGOrNode lastThenOrNode = thenAndNode.getLastChild();
				if (lastThenOrNode.getPredicate() instanceof BuiltInPredicate) {
					BuiltInPredicate lastThenBip = (BuiltInPredicate)lastThenOrNode.getPredicate();
					lastThen = lastThenBip.isFalse();
				}
					
				PCGOrNode lastElseOrNode = elseAndNode.getLastChild();
				if (lastElseOrNode.getPredicate() instanceof BuiltInPredicate) {
					BuiltInPredicate lastElseBip = (BuiltInPredicate)lastElseOrNode.getPredicate();
					lastElse = lastElseBip.isFalse();
				}

				if ((lastIf || lastThen) && lastElse)
					return true;
			}
		}
		return false;
	}

	private static boolean doesRuleContainOldPredicate(String ruleName, String literalName, List<Rule> deleteRules, 
			int nextDeleteRulePosition, List<Rule> yRules) {

		int numberOfDeleteRules = deleteRules.size();

		for (int i = nextDeleteRulePosition; i < (numberOfDeleteRules + yRules.size()); i++)  { 
			Rule rule = (i < numberOfDeleteRules) ? deleteRules.get(i) : yRules.get(i - numberOfDeleteRules);
			if (rule.getHead().getPredicateName().equals(ruleName)) {
				for (Predicate literal : rule.getBody()) {
					if (literal.getPredicateName().equals(literalName) 
							&& literal.getArgument(0).getType() == CompilerType.VARIABLE)
						return true;
				}
			}
		}
		
		return false;
	}

	//If there's a delete rule R1 for predicate p, then p(I) can not appear in any
	//Y-rule or delete-rule R2 which will be fired later than rule R1. That is, the
	//head of R2 must appear before the head of R1 in sorted_primed_preds
	//list. --HW
	private static void censorXYCliqueDeleteRules(List<Rule> yRules, List<Rule> deleteRules, 
			List<PredicateBase> sortedPrimedPredicates) {
		int i = 0, j;
		Predicate head;
		PredicateBase relation;

		while (i < deleteRules.size()) {
			//a delete rule of predicate p
			head = deleteRules.get(i).getHead();

			//looking for predicate p in sortedPrimedPredicates list
			for (j = 0; j < sortedPrimedPredicates.size(); j++) {
				relation = sortedPrimedPredicates.get(i);
				if (relation.getPredicateName().equals(head.getPredicateName()))
					break;
			}

			//p(I) can not appear in other Y/Delete rule of pred p and any Y/Delete
			//rule of each pred appearing before p in sorted_primed_preds list. That
			//is, they depend on P(I).
			for (; j >= 0; j--) {
				relation = sortedPrimedPredicates.get(i);
				if (doesRuleContainOldPredicate(relation.getPredicateName(), head.getPredicateName(), deleteRules, i + 1, yRules))
					break;
			}

			//if it fails the test, move the delete rule to y rule list, otherwise go on to the next delete rule
			if (j >= 0) 
				yRules.add(deleteRules.remove(i));
			else
				i++;
		}
	}	
	/*END HELPERS*/
}
