package edu.ucla.cs.wis.bigdatalog.system;

import edu.ucla.cs.wis.bigdatalog.api.QueryResults;
import edu.ucla.cs.wis.bigdatalog.common.FileUtilities;
import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.Export;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.ModuleDeclaration;
import edu.ucla.cs.wis.bigdatalog.compiler.ParseResultRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.Rule;
import edu.ucla.cs.wis.bigdatalog.compiler.compilation.CompilationResult;
import edu.ucla.cs.wis.bigdatalog.compiler.compilation.Compiler;
import edu.ucla.cs.wis.bigdatalog.compiler.language.ParseResult;
import edu.ucla.cs.wis.bigdatalog.compiler.language.ParserManager;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeList;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerInputVariable;
import edu.ucla.cs.wis.bigdatalog.database.BulkLoader;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Interpreter;
import edu.ucla.cs.wis.bigdatalog.interpreter.ProgramGenerationResult;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;
import edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting.ExitRuleModifier;
import edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting.GeneralizedPivotSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import org.slf4j.Logger;

public class DeALSContext implements Serializable {
	private static final long serialVersionUID = 1L;
	public static String FACTS_FILE_EXTENSION = "fac";
	public static String MODULE_FILE_EXTENSION = "deal";
	public static String DB_FILE_EXTENSION = "deals";
	public static final int DEFAULT_LOG_LEVEL = 64 + 128;
	
	private int					logLevel;
	private boolean 				timerOn;
	private DeALSConfiguration 	deALSConfiguration;
	private Database				database;
	private ModuleManager 			moduleManager;
	private ParserManager 			parser;
	private Interpreter 			interpreter;
	private boolean 				initialized;
	
	public DeALSContext() {
		this(new DeALSConfiguration());
	}
	
	public DeALSContext(DeALSConfiguration deALSConfiguration) {
		this.deALSConfiguration = deALSConfiguration;
		this.initialized = false;
	}

	public void initialize() {
		this.logLevel = DEFAULT_LOG_LEVEL;
		this.timerOn = false;
		this.database = new Database("default", this);
		this.moduleManager = new ModuleManager(this); 
		this.parser = new ParserManager();
		this.interpreter = new Interpreter(this, this.database);		
		this.initialized = true;
	}
	
	public void reset() {
		this.database.clear();
		this.database = null;
		this.moduleManager = null;
		this.parser = null;
		this.interpreter = null;
		this.initialized = false;
	}

	public DeALSConfiguration getConfiguration() { return this.deALSConfiguration; }
		
	public Database getDatabase() { return this.database; }
	
	public ModuleManager getModuleManager() { return this.moduleManager; }

	public ParserManager getParser() { return this.parser; }

	public Interpreter getInterpreter() { return this.interpreter; }

	public boolean isInitialized() { return this.initialized; }
		
	public void setTimer(boolean value) { this.timerOn = value; }
	
	public boolean isTimerOn() { return this.timerOn; }
	
	public boolean isTraceEnabled() { return (this.logLevel & 1) == 1; }
	
	public boolean isDebugEnabled() { return (this.logLevel & 2) == 2; }
	
	public boolean isInfoEnabled() { return (this.logLevel & 4) == 4; }
	
	public boolean isStatisticsEnabled() { return (this.logLevel & 8) == 8; }
	
	public boolean isDerivationTrackingEnabled() { return (this.logLevel & 16) == 16; }
	
	public boolean isWarnEnabled() { return (this.logLevel & 32) == 32; }
	
	public boolean isErrorEnabled() { return (this.logLevel & 64) == 64; }
	
	public boolean isFatalEnabled() { return (this.logLevel & 128) == 128; }
		
	public void setLogLevel(int logLevelId) { this.logLevel = logLevelId; }
	
	public int getLogLevel() { return this.logLevel; }
	
	public void logTrace(Logger logger, String message, Object...objects) {
		if (this.isTraceEnabled())
			logger.trace(message, objects);		
	}
	
	public void logTrace(Logger logger, String message) {
		if (this.isTraceEnabled())
			logger.trace(message);		
	}
	
	public void logDebug(Logger logger, String message, Object...objects) {
		if (this.isDebugEnabled())
			logger.debug(message, objects);
	}
	
	public void logDebug(Logger logger, String message) {
		if (this.isDebugEnabled())
			logger.debug(message);
	}
	
	public void logInfo(Logger logger, String message, Object...objects) {
		if (this.isInfoEnabled())
			logger.info(message, objects);		
	}
	
	public void logInfo(Logger logger, String message) {
		if (this.isInfoEnabled())
			logger.info(message);		
	}
	
	public void logDerivationTracking(Logger logger, String message, Object...objects) {
		if (this.isDerivationTrackingEnabled())
			logger.info(message, objects);		
	}
	
	public void logDerivationTracking(Logger logger, String message) {
		if (this.isDerivationTrackingEnabled())
			logger.info(message);		
	}
	
	public void logWarn(Logger logger, String message, Object...objects) {
		if (this.isWarnEnabled())
			logger.warn(message, objects);		
	}
	
	public void logWarn(Logger logger, String message) {
		if (this.isWarnEnabled())
			logger.warn(message);
	}
		
	public void logError(Logger logger, String message, Object...objects) {
		if (this.isErrorEnabled())
			logger.error(message, objects);		
	}
	
	public void logError(Logger logger, String message) {
		if (this.isErrorEnabled())
			logger.error(message);		
	}

	// each type of file is loaded differently
	// .fac is a file of facts in predicate format
	// .deal is a module file
	// .deals is a database that has been saved in binary format
	public SystemCommandResult loadFile(String fileName) {		
		if (fileName == null || fileName.length() == 0)
			throw new InterpreterException("Cannot load file.  File name missing.");
		
		if (fileName.endsWith(FACTS_FILE_EXTENSION))
			return this.loadFactsFile(fileName);
						
		String data = null;
		try {
			data = FileUtilities.getFileContents(Paths.get(fileName));
		} catch (IOException ioex) {
			throw new InterpreterException(ioex.toString());
		}
		
		if (data == null)
			throw new InterpreterException("loadFile(filename) unable to load file contents");
									
		if (fileName.endsWith(MODULE_FILE_EXTENSION)) {
			return this.loadDatabaseObjects(data);
		//} else if (fileName.endsWith(DB_FILE_EXTENSION)) {
		//	return this.loadDatabase(fileName);
		}
		
		String message = "File type unknown.  Please choose from:";
		message += "\n\t.fac - facts file";
		message += "\n\t.deal - module";
		message += "\n\t.deals - saved database";
		throw new InterpreterException(message);		
	}
	
	public SystemCommandResult loadDatabaseObjects(String data) {
		// first parse module file
		ParseResult result = this.parser.parseDatabaseObjects(data);
		ReturnStatus retval = result.getStatus();
		String message = "";

		if (retval == ReturnStatus.SUCCESS) {
			for (CompilerTypeBase module: (CompilerTypeList) result.getParserOutput()) {
				ParseResultRewriter.rewrite((ModuleDeclaration) module);
			}
		} else {
			// error message
			message = result.getMessage();
		}

		// install objects in module
		if (retval == ReturnStatus.SUCCESS) {
			// TODO: APS 5/9/2014 - create process to load module from CompilerTypeList of objects
			CompilerTypeList objects = (CompilerTypeList) result.getParserOutput();
			// put anything without a module declaration into the active module
			HashMap<String, Module> modules = new HashMap<>();
			for (int i = 0; i < objects.size() && retval == ReturnStatus.SUCCESS; i++) {
				Module module;
				if ((module = this.moduleManager.addModule((ModuleDeclaration) objects.get(i))) == null) {
					retval = ReturnStatus.FAIL;
					break;
				}
				modules.put(module.getModuleName(), module);
			}

			// now install modules
			if (retval == ReturnStatus.SUCCESS) {
				for (Module module : modules.values()) {
					if (!module.install(this)) {
						retval = ReturnStatus.FAIL;
						break;
					}
				}
			}
		}
		
		return new SystemCommandResult(retval, message);
	}
		
	// separate file for loading facts, in case it is a huge file
	public SystemCommandResult loadFactsFile(String filePath) {
		FileReader fr = null;
		BufferedReader br = null;
		ReturnStatus retval = ReturnStatus.SUCCESS;
		try {
			fr = new FileReader(filePath);
			br = new BufferedReader(fr);
			String line = br.readLine();
			while (line != null && line.trim().startsWith("%"))
				line = br.readLine();
			// check if first line (ignore commented lines) is special shorthand syntax of "all [predicatename]"
			//   to load all data into 1 relation and avoid parsing
			if (line == null)
				return new SystemCommandResult(retval); // file is empty - this is ok to return success

			br.close();
			if (line.toLowerCase().startsWith("all "))
				return new SystemCommandResult(BulkLoader.load(this, this.database, filePath, 
						Boolean.valueOf(this.deALSConfiguration.compareProperty("deals.database.tuplestores.baserelation.sort", "true"))));
			
			// otherwise, for smaller files, parse and load facts into relations
			return this.loadFacts(FileUtilities.getFileContents(Paths.get(filePath)));
		} catch (IOException e) {
			retval = ReturnStatus.ERROR;
		} finally {
			if (br != null)
				try { br.close(); } catch (IOException e) {}
			if (fr != null)
				try { fr.close(); } catch (IOException e) {}
		}
		
		return new SystemCommandResult(ReturnStatus.FAIL);
	}
	
	public SystemCommandResult loadFactsFile(String relationName, int arity, String filePath) {
		return new SystemCommandResult(BulkLoader.load(this, this.database, filePath, relationName, arity,
				Boolean.valueOf(this.deALSConfiguration.compareProperty("deals.database.tuplestores.baserelation.sort", "true"))));
	}
	
	private SystemCommandResult loadFacts(String data) {
		String message = "";
		
		// 1) parse file and get facts
		ParseResult parseResult = this.parser.parseFacts(data);
		ReturnStatus status = parseResult.getStatus();
		
		// 2) load facts into relations
		if (status == ReturnStatus.SUCCESS) {
			CompilerTypeList facts = (CompilerTypeList) parseResult.getParserOutput();
			Predicate fact;
			// track which relations to sort for later, if necessary
			HashSet<String> loadedRelations = new HashSet<>();
			for (int i = 0; i < facts.size(); i++) {				
				fact = (Predicate)facts.get(i);
				if (!this.moduleManager.addFact(fact))
					continue;
				
				loadedRelations.add(fact.getPredicateName() + "|" + fact.getArity());
			}
			
			// 3) sort if configured
			if (this.deALSConfiguration.compareProperty("deals.database.tuplestores.baserelation.sort", "true")) {				
				String[] parts;
				for (String entry : loadedRelations) {
					parts = entry.split("\\|");
					this.database.getRelationManager().getRelation(parts[0], Integer.parseInt(parts[1])).sort();
				}				
			}
		} else {
			message = parseResult.getMessage();
		}
		
		return new SystemCommandResult(status, message);
	}
		
	public SystemCommandResult load(boolean isFacts, String data) {	
		if (data == null || data.length() == 0)
			return new SystemCommandResult(ReturnStatus.ERROR);

		if (isFacts)
			return loadFacts(data);
		
		return loadDatabaseObjects(data);
	}

	public void unloadAllModules() {
		this.moduleManager.removeAllModules();
		//TODO - this is a poor attempt at clearing the internal DB
		this.database.clear();
	}

	public Pair<ReturnStatus, Module> getModule(String moduleName) {
		Module module = this.moduleManager.getModule(moduleName);
		return new Pair<>((module != null) ? ReturnStatus.SUCCESS : ReturnStatus.FAIL, module);
	}

	public void unloadModule(String moduleName) {
		this.moduleManager.removeModule(moduleName);
	}

	public SystemCommandResult compileQueryForm(String queryForm) {
		SystemCommandResult scr = new SystemCommandResult(this.parser.parseQueryForm(queryForm));
		if (scr.getStatus() == ReturnStatus.SUCCESS) {			
			scr = this.compileQueryForm((QueryForm)scr.getObject(), 
					ExecutionTarget.getExecutionTarget(this.deALSConfiguration.getProperty("deals.interpreter.executiontarget")));
		}
		
		return scr;
	}
	
	public SystemCommandResult compileQueryForm(String queryForm, GeneralizedPivotSet gps, int index, int numberOfPartitions) {
		SystemCommandResult scr = new SystemCommandResult(this.parser.parseQueryForm(queryForm));
		if (scr.getStatus() == ReturnStatus.SUCCESS) {			
			scr = this.compileQueryForm((QueryForm)scr.getObject(), 
					ExecutionTarget.getExecutionTarget(this.deALSConfiguration.getProperty("deals.interpreter.executiontarget")));
			
			if (scr.getStatus() == ReturnStatus.SUCCESS) {
				// append restricting predicate to exitrules
				OrNode root = (OrNode)((QueryForm)scr.getObject()).getProgram().getRoot();
				ExitRuleModifier.addRestrictingPredicate(root, gps, index, numberOfPartitions);
			}
		}
		
		return scr;
	}
	
	public SystemCommandResult compileQueryForm(QueryForm inputQueryForm, ExecutionTarget executionTarget) {
		ReturnStatus	returnStatus = ReturnStatus.FAIL;
		QueryForm		outputQueryForm = null;
		String 			message = "";
		
		Compiler compiler = new Compiler(this, this.moduleManager.getActiveModule());
		
		// turn off magic unless input variable present
		if (executionTarget == ExecutionTarget.OPERATORS 
				&& this.deALSConfiguration.getProperty("deals.compiler.rewriter").equals("on")) {
			boolean hasInputVariable = false;
			for (int i = 0; i < inputQueryForm.getArity(); i++)
				if (inputQueryForm.getArgument(i) instanceof CompilerInputVariable)
					hasInputVariable = true;
			
			if (!hasInputVariable)
				this.deALSConfiguration.setProperty("deals.compiler.rewriter", "off");
		}
		
		// this will create a new export if needed
		CompilationResult compilationResult = compiler.compile(inputQueryForm);
		if (compilationResult.isSuccess()) {
			ProgramGenerationResult pgr = null;
			
			switch (executionTarget) {
				case DeALS_PIPELINED:
					pgr = edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramGenerator.generateProgram(this, compilationResult);
				break;
				case OPERATORS:	
					pgr = edu.ucla.cs.wis.bigdatalog.interpreter.relational.ProgramGenerator.generateProgram(this, compilationResult);					
				break;
			}
			
			if (pgr.isSuccess()) {
				returnStatus = ReturnStatus.SUCCESS;
				outputQueryForm = compilationResult.getQueryForm();
			}
			message = pgr.getMessage();				
		} else {
			message = compilationResult.getMessage();	
		}

		return new SystemCommandResult(returnStatus, outputQueryForm, message);
	}

	// this will compile all defined Exports in the active module
	public SystemCommandResult compileAllQueryForms() {
		QueryForm exportQueryForm;
		Module module = this.moduleManager.getActiveModule();
		ExecutionTarget executionTarget = 
				ExecutionTarget.getExecutionTarget(this.deALSConfiguration.getProperty("deals.interpreter.executiontarget"));
		for (Export export : module.getExports()) {
			exportQueryForm = export.getQueryForm();
			if (exportQueryForm != null) {
				SystemCommandResult scr = this.compileQueryForm(export.getQueryForm(), executionTarget);
				if (scr.getStatus() != ReturnStatus.SUCCESS)
					return new SystemCommandResult(ReturnStatus.FAIL, scr.getMessage());
			}
		}
		return new SystemCommandResult(ReturnStatus.SUCCESS, "");
	}
		
	public SystemCommandResult compileQuery(String queryText) {
		if (!queryText.endsWith(".")) 
			queryText += ".";
		
		SystemCommandResult scr = new SystemCommandResult(this.parser.parseQuery(queryText));
		
		if (scr.getStatus() == ReturnStatus.SUCCESS) {
			Predicate query = (Predicate)scr.getObject();
			QueryForm queryForm = this.moduleManager.getActiveModule().getQueryForm(query);

			if (queryForm == null) {
				scr = new SystemCommandResult(ReturnStatus.FAIL);
				if (this.deALSConfiguration.compareProperty("deals.compiler.enforceQueryFormExports", "false")) {
					queryForm = query.getQueryForm();
					scr = new SystemCommandResult(ReturnStatus.SUCCESS);
				}
			}
			
			if (scr.getStatus() == ReturnStatus.SUCCESS) {
				queryForm.matchTerms(queryForm.getArguments(), query.getArguments());
				scr = this.compileQueryForm(queryForm, 
						ExecutionTarget.getExecutionTarget(this.deALSConfiguration.getProperty("deals.interpreter.executiontarget")));
			}
		}
		
		return scr;
	}
	
	public SystemCommandResult compileQueryToOperators(String queryText) {
		if (!queryText.endsWith(".")) 
			queryText += ".";
		
		SystemCommandResult scr = new SystemCommandResult(this.parser.parseQuery(queryText));
		
		if (scr.getStatus() == ReturnStatus.SUCCESS) {
			Predicate query = (Predicate)scr.getObject();
			//QueryForm queryForm = DeALSContext.getModuleManager().getActiveModule().getQueryForm(query);
			QueryForm queryForm = query.getQueryForm();						
			queryForm.matchTerms(queryForm.getArguments(), query.getArguments());
			scr = this.compileQueryForm(queryForm, ExecutionTarget.OPERATORS);
		}
		
		return scr;
	}

    public PCGOrNode compileQueryToPCGTree(String queryText) {
        if (!queryText.endsWith("."))
            queryText += ".";

        SystemCommandResult scr = new SystemCommandResult(this.parser.parseQuery(queryText));

        if (scr.getStatus() == ReturnStatus.SUCCESS) {
            Predicate query = (Predicate)scr.getObject();
            //QueryForm queryForm = DeALSContext.getModuleManager().getActiveModule().getQueryForm(query);
            QueryForm queryForm = query.getQueryForm();
            queryForm.matchTerms(queryForm.getArguments(), query.getArguments());
            QueryForm inputQueryForm = queryForm;
            ExecutionTarget executionTarget = ExecutionTarget.OPERATORS;
            Compiler compiler = new Compiler(this, this.moduleManager.getActiveModule());

            // turn off magic unless input variable present
            if (executionTarget == ExecutionTarget.OPERATORS
                    && this.deALSConfiguration.getProperty("deals.compiler.rewriter").equals("on")) {
                boolean hasInputVariable = false;
                for (int i = 0; i < inputQueryForm.getArity(); i++)
                    if (inputQueryForm.getArgument(i) instanceof CompilerInputVariable)
                        hasInputVariable = true;

                if (!hasInputVariable)
                    this.deALSConfiguration.setProperty("deals.compiler.rewriter", "off");
            }

            // this will create a new export if needed
            CompilationResult compilationResult = compiler.compile(inputQueryForm);
            if (compilationResult.isSuccess()) {
                return compilationResult.getCompiledProgram();
            }

        }

        return null;
    }

	public SystemCommandResult prepareQuery(QueryForm queryForm) {
		ReturnStatus returnStatus = ReturnStatus.FAIL;

		if (queryForm.isCompiled()) {
			if (this.interpreter.initializeQuery(queryForm)) {
				returnStatus = ReturnStatus.SUCCESS;
			} else {
				// if we failed to initialize the query, clean it up				
				this.resetQuery(queryForm);
				returnStatus = ReturnStatus.ERROR;
				throw new InterpreterException("Query could not be initialized");
			}
		} else {
			returnStatus = ReturnStatus.ERROR;
			throw new InterpreterException("Query form is not compiled : " + queryForm.toString());			
		}

		return new SystemCommandResult(returnStatus);
	}

	public void resetQuery(QueryForm queryForm) {
		this.interpreter.cleanUpQuery(queryForm);
	}

	public Pair<QueryResults, String> executeQuery(QueryForm queryForm, boolean returnResults) {
		String message = "Executing query '" + queryForm.toString(true) + "'";
		
		QueryResults queryResult = this.interpreter.executeQuery(queryForm, returnResults);

		return new Pair<>(queryResult, message);
	}

	public SystemCommandResult addFact(String factText)  {
		ReturnStatus returnStatus = ReturnStatus.FAIL;

		ParseResult result = this.parser.parseGroundPredicate(factText);		
		if (result.getStatus() == ReturnStatus.SUCCESS) {
			Module module = this.moduleManager.getActiveModule();
			if (module != null) {
				module.addFact((Predicate) result.getParserOutput());
				returnStatus = ReturnStatus.SUCCESS;
			} else {
				returnStatus = ReturnStatus.ERROR;
			}
		} else {
			returnStatus = ReturnStatus.ERROR;
		}

		return new SystemCommandResult(returnStatus);
	}

	public SystemCommandResult deleteFact(String factText) {
		ReturnStatus returnStatus = ReturnStatus.FAIL;

		ParseResult result = this.parser.parseGroundPredicate(factText);
		if (result.getStatus() == ReturnStatus.SUCCESS) {
			Module module = this.moduleManager.getActiveModule();
			if (module != null) {
				module.removeDatabaseTuple((Predicate) result.getParserOutput());
				returnStatus = ReturnStatus.SUCCESS;
			} else {
				returnStatus = ReturnStatus.ERROR;
			}
		} else {
			returnStatus = ReturnStatus.ERROR;
		}

		return new SystemCommandResult(returnStatus);
	}

	public SystemCommandResult addRule(String ruleText) {
		ReturnStatus returnStatus = ReturnStatus.FAIL;

		ParseResult result = this.parser.parseRule(ruleText);
		Rule rule = (Rule) result.getParserOutput();
		if (result.getStatus() == ReturnStatus.SUCCESS) {
			Module module = this.moduleManager.getActiveModule();
			if (module != null)	{
				module.addRule(rule);
				returnStatus = ReturnStatus.SUCCESS;
			} else {
				returnStatus = ReturnStatus.ERROR;
			}
		} else {
			returnStatus = ReturnStatus.ERROR;
		}

		return new SystemCommandResult(returnStatus, rule);
	}

	public SystemCommandResult deleteDerivedPredicate(String predicateName, int arity) {
		ReturnStatus returnStatus = ReturnStatus.ERROR;

		Module module = this.moduleManager.getActiveModule();
		if (module != null) {
			module.removeDerivedPredicate(predicateName, arity);
			returnStatus = ReturnStatus.SUCCESS;
		}

		return new SystemCommandResult(returnStatus);
	}

	public SystemCommandResult getActiveModuleName() {
		ReturnStatus	returnStatus = ReturnStatus.FAIL;
		String 			moduleName = null;

		moduleName = this.moduleManager.getActiveModule().getModuleName();
		if (moduleName != null)
			returnStatus = ReturnStatus.SUCCESS;

		return new SystemCommandResult(returnStatus, moduleName);
	}

	public SystemCommandResult setActiveModule(String moduleName) {
		ReturnStatus returnStatus = ReturnStatus.FAIL;

		if (moduleName == null) {
			this.moduleManager.setActiveModule();
			returnStatus = ReturnStatus.SUCCESS;
		} else {
			Module module = this.moduleManager.getModule(moduleName);
			if (module != null) {
				this.moduleManager.setActiveModule(module);
				returnStatus = ReturnStatus.SUCCESS;
			} else {
				returnStatus = ReturnStatus.ERROR;
			}
		}

		return new SystemCommandResult(returnStatus);
	}
	
	public SystemCommandResult addExport(String exportStr) {
		ReturnStatus returnStatus = ReturnStatus.FAIL;
		Export export = null;
		
		ParseResult result = this.parser.parseQueryForm(exportStr);
		QueryForm queryForm = (QueryForm) result.getParserOutput();
		if (result.getStatus() == ReturnStatus.SUCCESS) {
			Module module = this.moduleManager.getActiveModule();
			if (module != null)	{
				export = new Export(queryForm);
				module.addExport(export);
				returnStatus = ReturnStatus.SUCCESS;
			} else {
				returnStatus = ReturnStatus.ERROR;
			}
		} else {
			returnStatus = ReturnStatus.ERROR;
		}

		return new SystemCommandResult(returnStatus, export, result.getMessage());
	}
	
	public SystemCommandResult deleteExport(String exportStr) {
		ReturnStatus returnStatus = ReturnStatus.ERROR;
		String message = "";
		Module module = this.moduleManager.getActiveModule();
		if (module != null) {
			ParseResult result = this.parser.parseQueryForm(exportStr);
			message = result.getMessage();
			if (result.getStatus() == ReturnStatus.SUCCESS) {
				module.removeExport((QueryForm) result.getParserOutput());
				returnStatus = ReturnStatus.SUCCESS;
			} else {
				returnStatus = ReturnStatus.ERROR;
			}
		} else {
			returnStatus = ReturnStatus.ERROR;
		}

		return new SystemCommandResult(returnStatus, message);
	}
}
