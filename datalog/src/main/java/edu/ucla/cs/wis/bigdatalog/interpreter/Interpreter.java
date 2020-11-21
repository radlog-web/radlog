package edu.ucla.cs.wis.bigdatalog.interpreter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
// import org.apache.logging.log4j.ThreadContext;

import edu.ucla.cs.wis.bigdatalog.api.QueryResults;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateStructuralAttribute;
import edu.ucla.cs.wis.bigdatalog.compiler.type.TypeInferrer;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.RelationManager;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.QueryFormNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.statistics.StageStatisticsManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy.XYLiteralListManager;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class Interpreter implements Serializable {
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(Interpreter.class.getName());
	private static Logger loggerResults = LoggerFactory.getLogger("ResultsLogger");
	
	private static String QUERY_FORM_RELATION_PREFIX = "$QUERY_FORM_RELATION_";
	
	protected DeALSContext				deALSContext;
	protected CompilerVariableList 	queryFormVariableList;
	protected CompilerVariableList 	tempQueryVariableList;
	protected Database					database;
	protected XYLiteralListManager 	xyLiteralListManager;
		  
	public Interpreter(DeALSContext deALSContext, Database database) {
		this.deALSContext			= deALSContext;
		this.database				= database;
		this.xyLiteralListManager 	= new XYLiteralListManager();
		this.queryFormVariableList 	= new CompilerVariableList();
		this.tempQueryVariableList	= null;
	}
	
	public Database getDatabase() { return this.database; }
	
	public XYLiteralListManager getXYLiteralListManager() { return this.xyLiteralListManager; }

	public boolean isInitialized() {
		return (this.database != null);
	}
	
	public boolean initializeQuery(QueryForm queryForm) {
		if (queryForm == null)
			throw new InterpreterException("Null Query Form can not be prepared.");
		
		this.deALSContext.logTrace(logger, "Entering initializeQuery");

		RelationManager rm = this.deALSContext.getDatabase().getRelationManager();

		// initialize the program
		boolean status = queryForm.getProgram().initialize();
		
		// create the relation for query results
		Relation<?> queryFormRelation = null;
		DataType[] schema = TypeInferrer.getQueryFormSchema(queryForm, this.deALSContext.getModuleManager().getActiveModule());
		boolean useUniqueKey = true;
		
		if (queryForm.getArity() > 0) {
			// if we have been told to persist the results in a relation, the query form will use this designated relation
			if (queryForm.getPersistedResultsRelationName() != null) {
				BaseRelation<?> baseRelation = rm.getBaseRelation(queryForm.getPersistedResultsRelationName());
				if (baseRelation == null) {
					List<BasePredicateStructuralAttribute> bpsas = new ArrayList<>();
					for (DataType column : schema)
						bpsas.add(new BasePredicateStructuralAttribute(column));
					
					BasePredicate basePredicate = new BasePredicate(queryForm.getPersistedResultsRelationName(), bpsas);
					baseRelation = this.deALSContext.getModuleManager().getActiveModule().install(basePredicate);
					
					if (baseRelation != null 
							&& !this.deALSContext.getModuleManager().getActiveModule().addBasePredicate(basePredicate)) {
						this.deALSContext.getDatabase().getRelationManager().deleteBaseRelation(baseRelation);
						baseRelation = null;
					}
				} else {
					// empty relation first
					baseRelation.cleanUp();
				}
				
				queryFormRelation = baseRelation;
			} else {
				// from here, we either use a surrogate relation, from a relationNode in the tree, 
				// or create a relation for the query form
								
				// no unique index on queryformrelation if any of the following conditions are met:
				// 1) the program root is a materialized relation 
				//   - examples: RecursiveOrNode, aggregates
				// 2) a non monotonic aggregate is the root of the program
				Node<?> rootNode = (Node<?>)queryForm.getProgram().getRoot();
				
				// if the queryForm is just receiving the same values as the RelationNode, 
				// point the queryform relation at the relationnode relation
				// we can use root node relation (easily) if same number of columns
				// otherwise we need to project out columns (TODO APS 7/10/2014)
				boolean useSurrogateRelation = false;
				if (rootNode instanceof QueryFormNode) { 
					if (((RelationNode)rootNode).getArity() == queryForm.getArity()) {
						boolean hasBoundVariable = false;
						for (int i = 0; i < rootNode.getArity();i++) {
							if (rootNode.getArgument(i) instanceof InputVariable)
								hasBoundVariable = true;
							else if (rootNode.getArgument(i) instanceof DbTypeBase)
								hasBoundVariable = true;
						}
						
						if (!hasBoundVariable)
							useSurrogateRelation = true;						
					}
				}
				
				// RelationNode is a tabled ornode that we can pull results from if it is the root of the query form
				if (useSurrogateRelation) {
					QueryFormNode qfn = (QueryFormNode)rootNode;
					queryFormRelation = qfn.getRelation();
					queryForm.setIsUsingSurrogateRelation(true);
					queryForm.setQueryFormCursor(qfn.getQueryFormCursor());
					useUniqueKey = false;
					if (this.deALSContext.isInfoEnabled())
						this.deALSContext.logInfo(logger, "Using surrogate relation from " + ((OrNode)qfn).toStringNode() + " for " + queryForm.toString());
				}
			}
		
			if (queryFormRelation == null) {
				String relationName = QUERY_FORM_RELATION_PREFIX + queryForm.getPredicateName();
				queryFormRelation = rm.createQueryFormRelation(relationName, schema, useUniqueKey);
			}
			queryForm.setQueryFormRelation(queryFormRelation);
		}
				
		this.deALSContext.logTrace(logger, "Exiting initializeQuery with status = {}", status);  
		
		return status;
	}

	public boolean cleanUpQuery(QueryForm queryForm) {
		this.deALSContext.logTrace(logger, "Entering cleanUpQuery for {}", queryForm.getPredicateName());
		
		this.deALSContext.getInterpreter().getXYLiteralListManager().clearXYLiteralLists();

		Database activeDatabase = this.deALSContext.getDatabase();
		RelationManager relationManager = activeDatabase.getRelationManager();
		if (this.deALSContext.isDebugEnabled()) {
			for (DerivedRelation relation : relationManager.getDerivedRelations())
				this.deALSContext.logDebug(logger, relation.toString());
		}

		Program<?> program = queryForm.getProgram();
		if (program != null)
			program.cleanUp();
		
		Object[] derivedRelations = relationManager.getDerivedRelations().toArray();
		for (int i = 0; i < derivedRelations.length; i++)
			relationManager.deleteDerivedRelation((DerivedRelation) derivedRelations[i]);

		activeDatabase.getTypeManager().getKeyValueStoreManager().clear();

		if (this.deALSContext.isDebugEnabled()) {
			for (DerivedRelation relation : relationManager.getDerivedRelations())
				this.deALSContext.logDebug(logger, relation.toString());
		}

		queryForm.unsetQueryFormBinding();
		
		this.deALSContext.logTrace(logger, "Exiting cleanUpQuery");

		return true;
	}
	
	public QueryResults executeQuery(QueryForm queryForm, boolean returnResults) {
		this.deALSContext.logTrace(logger, "Entering QueryForm.executeQuery() for {}", queryForm.toString(true));
		this.deALSContext.logInfo(logger, "[BEGIN executeQuery for {} BEGIN]", queryForm.toString(true));
		
		ReturnStatus 	returnStatus 		= ReturnStatus.SUCCESS;
		Status 			status				= Status.FAIL;	
		Program<?>		program				= queryForm.getProgram();
		long 			start, end;
		long 			freeMemoryStart 	= 0;
		long			freeMemoryEnd 		= 0;
		
		String queryId = queryForm.toString(true) + System.currentTimeMillis();
		queryId = queryId.replace(" " , "");
				
		// start writing query log statements to file named queryId's value
		this.startQueryLog(queryId);
		
		Program.resetIndexCounters();
		
		if (queryForm.isUsingSurrogateRelation()) {
			start = System.currentTimeMillis();
			status = program.execute();
			end = System.currentTimeMillis();
		} else {
			start = System.currentTimeMillis();
			status = program.execute(queryForm.getQueryFormRelation());
			end = System.currentTimeMillis();
		}
		
		if (this.deALSContext.isStatisticsEnabled())
			StageStatisticsManager.getInstance().endCollecting();
		
		QueryForm queryFormCopy = queryForm.copyForResults();	
		
		QueryResults queryResults = new QueryResults(queryId, queryFormCopy, returnStatus,  returnResults, 
				program.getExecutionInfo(this.deALSContext),
				program.getRecursionInformationByClique(),
				status == Status.SUCCESS,
				this.deALSContext.getConfiguration().compareProperty("deals.interpreter.queryresults.verbose", "true"),
				this.database);

		queryResults.setExecutionTime(end - start);
		queryResults.setMemoryUsed(freeMemoryStart - freeMemoryEnd);
		if (this.deALSContext.getConfiguration().compareProperty("deals.interpreter.executioninfo.memory", "on"))
			queryResults.setMemoryMeasuredMessage(this.database.getRelationManager().toStringMemoryMeasured());
				
		if (this.deALSContext.getConfiguration().compareProperty("deals.interpreter.executioninfo.memory.fs", "on")) {
			queryResults.setMemoryMeasuredFS(this.database.getRelationManager().getMemoryUsageFS());
			queryResults.setMemoryMeasuredMessageFS(this.database.getRelationManager().toStringMemoryMeasuredFS());
		}
				
		if (this.deALSContext.isStatisticsEnabled())
			loggerResults.info(queryResults.toJson(true));
		
		// stop writing this query log statements to file
		this.endQueryLog();	
		
		this.deALSContext.logInfo(logger, "[END executeQuery for {} END]\n", queryForm.toString(true));
		
		this.deALSContext.logTrace(logger, "Exiting QueryForm.executeQuery() for {} with status = {}", queryForm.toString(true), returnStatus.name());

		return queryResults;
	}
	
	private void startQueryLog(String queryId) {
		// ThreadContext.put("queryLogFileName", queryId);
	}
	
	private void endQueryLog() {
		// ThreadContext.put("queryLogFileName", null);
	}
	
	private long gatherMemoryStatistics(boolean start) {
		java.lang.Runtime runtime = java.lang.Runtime.getRuntime();
		StringBuilder retval = new StringBuilder();
		if (start)
			runtime.gc();
		retval.append("[max memory: " + runtime.maxMemory() / 1024); 
		//2)to get how much memory that JVM has allocated for your application 
		retval.append(" | allocated memory: " + runtime.totalMemory() / 1024);
		//3)to get how much memory is being used by your application: 
		long freeMemory = runtime.freeMemory() / 1024;
		retval.append(" | free memory: " +  freeMemory + "]");
		System.out.println(retval.toString());
		return freeMemory;
	}
}