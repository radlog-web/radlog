package edu.ucla.cs.wis.bigdatalog.api;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.DerivedPredicate;
import edu.ucla.cs.wis.bigdatalog.exception.APIException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.system.LogLevel;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;
import edu.ucla.cs.wis.bigdatalog.system.SystemCommandResult;

public class CommandWrapper {
	private static Logger logger = LoggerFactory.getLogger(CommandWrapper.class.getName());

	private DeALSContext deALSContext;
	private SystemOptions 	options;

	public CommandWrapper() {
		this(0, new DeALSContext());
	}
	
	public CommandWrapper(DeALSContext deALSContext) {
		this(0, deALSContext);
	}
	
	public CommandWrapper(int logLevel, DeALSContext deALSContext) {
		this.deALSContext = deALSContext;
		this.options = new SystemOptions(logLevel, DeALSContext.DEFAULT_LOG_LEVEL);
	}
	
	private String getCurrentPath() { 
		return this.deALSContext.getConfiguration().getProperty("deals.workingdirectory"); 
	}
	
	private String completePath(String dbName) { return this.getCurrentPath() + dbName; }	
	
	public SystemOptions getOptions() { return this.options; }
	
	public CommandResult execute(String commandText) {
		Command command;
		try {
			command = Command.parseCommand(commandText);
		} catch (APIException apie) {
			return new CommandResult(CommandType.UNKNOWN, ReturnStatus.ERROR, apie.getMessage()); 
		}
				
		return this.executeCommand(command);
	}
	
	private CommandResult executeCommand(Command command) {
		CommandResult result;
		switch (command.getCommandType())
		{
		case COMPILE:
			result = this.compileQueryForms(command);
			break;
		case DEFAULT:
			result = this.setDefault(command);
			break;
		case DELETE_RULE:
			result = this.deleteRules(command);
			break;
		case DELETE_FACT:
			result = this.deleteFacts(command);
			break;
		case DESCRIBE:
			result = this.describe(command);
			break;
		case ADD_RULE:
			result = this.addRule(command);
			break;
		case ADD_FACT:
			result = this.addFacts(command);
			break;
		case LOAD:
			result = this.load(command);
			break;
		case UNLOAD:
			result = this.unloadModule(command);
			break;
		case QUERY:
			result = this.executeQuery(command);
			break;
		//case DELETE_DB:
		//	result = this.deleteDb(command);
		//	break;
		//case SAVE_DB:
		//	result = this.saveDb(command);
		//	break;
		case SET:
			result = this.setOptions(command);
			break;
		case ADD_EXPORT:
			result = this.addExports(command);
			break;
		case DELETE_EXPORT:
			result = this.deleteExport(command);
			break;
		//case USE:
		//	result = this.setActiveDatabase(command);
		//	break;
		//case TRUNCATE_DB:
		//	result = this.truncateDatabase(command);
		//	break;
		//case CREATE_DB:
		//	result = this.createDatabase(command);
		//	break;
			//case TRUNCATE:
			//	result = this.truncateRelation(command);
			//	break;
		default:
			result = new CommandResult(CommandType.UNKNOWN, ReturnStatus.ERROR, "Unrecognized command.");
		}
		
		return result;
	}

	private CommandResult load(Command command) {
		CommandResult result = new CommandResult(CommandType.LOAD, ReturnStatus.SUCCESS);
		SystemCommandResult scr;
		// if the first argument is a file path with the correct extension, we assume we're loading a file
		//   we only load .deal, .deals or .fac files 
		// if the first argument is not a file path, we've been sent the data to parse
		String filepath = command.getCommandArguments().get(0).trim();
		
		if ((filepath.lastIndexOf(".") + 4 == filepath.length()) || (filepath.lastIndexOf(".") + 5 == filepath.length())) {
			if (filepath.endsWith(DeALSContext.MODULE_FILE_EXTENSION)
					|| filepath.endsWith(DeALSContext.DB_FILE_EXTENSION)
					|| filepath.endsWith(DeALSContext.FACTS_FILE_EXTENSION)) {
				String path = null;
				for (String argument : command.getCommandArguments()) {
					path = this.completePath(argument);
					result.message += "Loading " + argument;
		
					// exit at first failure
					scr = this.deALSContext.loadFile(path);
					if (scr.getStatus() != ReturnStatus.SUCCESS) {
						result.setReturnStatus(scr.getStatus());
						result.message += scr.getMessage();
						return result;
					}
				}
			} else {					
				String message = "File type unknown.  Please choose from:";
				message += "\n\t.fac - facts file";
				message += "\n\t.deal - module";
				message += "\n\t.deals - saved database";
				
				result.message = message;
				result.setReturnStatus(ReturnStatus.ERROR);
			}
		} else {
			if (command.commandArguments.get(0) == "facts") {
				result.message += "Loading facts into database";
				scr = this.deALSContext.load(true, command.originalCommandText);
			} else {
				result.message += "Loading objects into database";
				scr = this.deALSContext.load(false, command.originalCommandText);
			}
			result.setReturnStatus(scr.getStatus());
		}

		return result;
	}

	private CommandResult unloadModule(Command command)  {
		CommandResult result = new CommandResult(CommandType.UNLOAD, ReturnStatus.SUCCESS);
		
		if (command.getCommandArguments() == null || command.getCommandArguments().size() == 0) {
			result.message += "Unloading all modules.";

			try {
				this.deALSContext.unloadAllModules();
				result.setReturnStatus(ReturnStatus.SUCCESS);
			} catch (Exception ex) {
				result.setReturnStatus(ReturnStatus.ERROR);
				result.message += ex.getMessage();
			}
			
		} else {
			for (String argument : command.getCommandArguments()) {
				if (result.message.length() > 0)
					result.message += "\n";
				
				result.message += "Unloading module '" + argument + "'";

				// exit at first failure
				try {
					this.deALSContext.unloadModule(argument);
				} catch (Exception ex) {
					result.setReturnStatus(ReturnStatus.ERROR);
					result.message += ex.getMessage();
					return result;
				}
			}
			result.setReturnStatus(ReturnStatus.SUCCESS);
		}
		return result;
	}

	/*private CommandResult saveDb(Command command) {
		CommandResult result;
		String path = completePath(command.getCommandArguments().get(0));
		
		if (path == null)
			return new CommandResult(CommandType.SAVE_DB, ReturnStatus.ERROR, "Path is not valid.  Database can not be saved.");
		
		SystemCommandResult scr = this.deALSContext.saveDatabase(path);
		if (scr.getStatus() == ReturnStatus.SUCCESS) {
			result = new CommandResult(CommandType.SAVE_DB, ReturnStatus.SUCCESS);
			result.setMessage("Database is saved as " + path);
		} else {
			result = new CommandResult(CommandType.SAVE_DB, scr.getStatus());
			result.setMessage("Database can not be saved into " + path);
		}
		
		return result;
	}*/

	/*private CommandResult deleteDb(Command command) {
		CommandResult result;
		String databaseName = command.getCommandArguments().get(0);
		if (databaseName == null || databaseName.length() == 0)
			return new CommandResult(CommandType.DELETE_DB, ReturnStatus.ERROR, "Database name is not valid.  Database can not be deleted.");
		
		SystemCommandResult scr = this.deALSContext.deleteDatabase(databaseName);		
		if (scr.getStatus() == ReturnStatus.SUCCESS) {
			result = new CommandResult(CommandType.DELETE_DB, ReturnStatus.SUCCESS);
			result.setMessage("Database has been destroyed.");
		} else {
			result = new CommandResult(CommandType.DELETE_DB, scr.getStatus());
			result.setMessage("Database can not be destroyed.");			
		}
		
		return result;
	}*/

	private CommandResult compileQueryForms(Command command) {
		CommandResult result = new CommandResult(CommandType.COMPILE, ReturnStatus.SUCCESS);
		
		if (command.getCommandArguments().size() > 0) {
			int counter = 0;
			SystemCommandResult scr;
			for (String argument : command.getCommandArguments()) {
				if (counter > 0) 
					result.message += "\n";

				scr = this.deALSContext.compileQueryForm(argument);
				result.message += scr.getMessage();
				// exit at first failure
				if (scr.getStatus() != ReturnStatus.SUCCESS) {
					result.setReturnStatus(scr.getStatus());
					return result;
				}
				
				counter++;
			}
		} else {
			result.message += "Compiling all exported Query Form definitions.";

			SystemCommandResult scr = this.deALSContext.compileAllQueryForms();
			result.message += "\n" + scr.getMessage();
			result.setReturnStatus(scr.getStatus());
		}
		
		return result;
	}

	private CommandResult executeQuery(Command command) {		
		if (command.getCommandArguments().size() == 0)
			return new CommandResult(CommandType.QUERY, ReturnStatus.FAIL, "Query not provided.  Can not execute");
	
		String query = command.getCommandArguments().get(0);
		Pair<Boolean, String> persistResults = extractDestinationName(command.getCommandArguments());

		String persistResultRelationName = null;
		if ((persistResults != null) && persistResults.getFirst())
			persistResultRelationName = persistResults.getSecond();

		// execute query and get results
		Pair<QueryResults, String> results = this.doExecuteQuery(query, true, persistResultRelationName);
		
		CommandResult commandResult;
		if (results.getFirst() == null)
			commandResult = new CommandResult(CommandType.QUERY, ReturnStatus.FAIL, results.getSecond());
		else
			commandResult = new CommandResult(CommandType.QUERY, results.getFirst().getReturnStatus(), 
					results.getSecond(), results.getFirst());

		return commandResult;
	}
	
	private Pair<QueryResults, String> doExecuteQuery(String queryText, boolean returnResults, String persistedResultsRelationName) {
		QueryForm queryForm = null;
		QueryResults queryResult = null;
		long before, after;
		StringBuilder output = new StringBuilder();
		
		SystemCommandResult scr = this.deALSContext.compileQuery(queryText);		
		if (scr.getStatus() != ReturnStatus.SUCCESS)
			return new Pair<>(null, scr.getMessage());
				
		queryForm = (QueryForm)scr.getObject();
		
		if (persistedResultsRelationName != null)
			queryForm.setPersistedResultsRelationName(persistedResultsRelationName);
		
		before = System.currentTimeMillis();

		SystemCommandResult prepareQuerySCR = this.deALSContext.prepareQuery(queryForm);  
		if (prepareQuerySCR.getStatus() == ReturnStatus.SUCCESS) {
			after = System.currentTimeMillis();
			if (this.deALSContext.isTimerOn()) {
				if (this.options.isTimerOn(TimerOption.QUERY_SETUP))
					logger.info("Query Setup Elapsed Time: " + (after - before) + " ms");
			}				
			
			Pair<QueryResults, String> retvalPair3 = this.deALSContext.executeQuery(queryForm, returnResults);			
			queryResult = retvalPair3.getFirst();
			output.append(retvalPair3.getSecond());		
		}

		before = System.currentTimeMillis();
		this.deALSContext.resetQuery(queryForm);
		after = System.currentTimeMillis();
		
		if (this.deALSContext.isTimerOn())
			if (this.options.isTimerOn(TimerOption.QUERY_CLEANUP))
				logger.info("Query Cleanup Elapsed Time: " + (after - before) + " ms");
 
		return new Pair<>(queryResult, output.toString());
	}

	private CommandResult addRule(Command command) {
		CommandResult result = new CommandResult(CommandType.ADD_RULE, ReturnStatus.SUCCESS);
		
		result.message += "Adding rule '" + command.getOriginalCommandTextWithCommand() + "'";
		
		SystemCommandResult scr = this.deALSContext.addRule(command.getOriginalCommandTextWithCommand());
		if (scr.getStatus() != ReturnStatus.SUCCESS)
			result.setReturnStatus(scr.getStatus());
		result.message += scr.getMessage();
		
		return result;
	}
	
	private CommandResult deleteRules(Command command) {
		CommandResult result = new CommandResult(CommandType.DELETE_RULE, ReturnStatus.SUCCESS);
		SystemCommandResult scr;
		String rule;
		int arity;
		
		// must be pairs of arguments
		if (command.getCommandArguments().size() % 2 != 0)
			return new CommandResult(CommandType.DELETE_RULE, ReturnStatus.ERROR, "Arguments must be paired as [rulename] [arity]");
		
		// we expect [rulename], [arity], ..., [rulename], [arity]
		for (int i = 0; i < command.getCommandArguments().size(); i = i + 2) {
			rule = command.getCommandArguments().get(i);
			arity = Integer.parseInt(command.getCommandArguments().get(i + 1));
			if (i > 0)
				result.message += "\n";
			result.message += "Deleting rules '" + rule + "|" + arity + "'.";

			scr = this.deALSContext.deleteDerivedPredicate(rule, arity);
			if (scr.getStatus() != ReturnStatus.SUCCESS) {
				result.setReturnStatus(scr.getStatus());
				result.message += scr.getMessage();
				return result;
			}
		}
		
		return result;
	}
	
	private CommandResult addFacts(Command command) {
		CommandResult result = new CommandResult(CommandType.ADD_FACT, ReturnStatus.SUCCESS);
		SystemCommandResult scr;
		int counter = 0;
		for (String argument : command.getCommandArguments()) {
			if (counter > 0) 
				result.message += "\n";
			result.message += "Adding fact '" + argument + "'.";

			scr = this.deALSContext.addFact(argument);
			if (scr.getStatus() != ReturnStatus.SUCCESS) {
				result.setReturnStatus(scr.getStatus());
				result.message += scr.getMessage();
				break;
			}
		}
		
		return result;
	}

	private CommandResult deleteFacts(Command command) {
		CommandResult result = new CommandResult(CommandType.DELETE_FACT, ReturnStatus.SUCCESS);
		SystemCommandResult scr;
		int counter = 0;
		for (String argument : command.getCommandArguments()) {
			if (counter > 0) 
				result.message += "\n";
			result.message += "Deleting tuple '" + argument + "'.";

			scr = this.deALSContext.deleteFact(argument);
			if (scr.getStatus() != ReturnStatus.SUCCESS) {
				result.setReturnStatus(scr.getStatus());
				result.message += scr.getMessage();
				return result;
			}
		}
		
		return result;
	}

	private CommandResult setDefault(Command command) {
		CommandResult result = new CommandResult(CommandType.DEFAULT, ReturnStatus.SUCCESS);
		String	argumentStr0;
		String	argumentStr1;

		switch (command.getCommandArguments().size())
		{
		case 0:
		{
			SystemCommandResult scr = this.deALSContext.getActiveModuleName();
			if (scr.getStatus() == ReturnStatus.SUCCESS) {
				result.message += "Set " + scr.getMessage() + " to active module.";
			} else {
				result.setReturnStatus(scr.getStatus());
				result.message += scr.getMessage();
			}
		}
		break;

		case 1:
		{
			argumentStr0 = command.getCommandArguments().get(0);
			if (argumentStr0.startsWith("'")) argumentStr0 = argumentStr0.substring(1);
			if (argumentStr0.endsWith("'")) argumentStr0 = argumentStr0.substring(0, argumentStr0.length()-1);

			if (argumentStr0.equals(CommandOption.MODULE.getText())) {
				SystemCommandResult scr = this.deALSContext.setActiveModule(null); 
				if (scr.getStatus() == ReturnStatus.SUCCESS) {
					SystemCommandResult scr2 = this.deALSContext.getActiveModuleName();
					if (scr2.getStatus() == ReturnStatus.SUCCESS) {						
						result.message += "Set " + scr2.getMessage() + " to active module.";
					} else {
						result.setReturnStatus(scr2.getStatus());
						result.message += scr2.getMessage();
					}
				}
			} else {
				result.setReturnStatus(ReturnStatus.ERROR);
				result.setMessage("Illegal arguments '" + argumentStr0 + "'");
			}
		}
		break;

		case 2:
		{
			argumentStr0 = command.getCommandArguments().get(0);
			if (argumentStr0.startsWith("'")) argumentStr0 = argumentStr0.substring(1);
			if (argumentStr0.endsWith("'")) argumentStr0 = argumentStr0.substring(0, argumentStr0.length()-1);
			argumentStr1 = command.getCommandArguments().get(1);
			if (argumentStr1.startsWith("'")) argumentStr1 = argumentStr1.substring(1);
			if (argumentStr1.endsWith("'")) argumentStr1 = argumentStr1.substring(0, argumentStr1.length()-1);

			if (argumentStr0.equals(CommandOption.MODULE.getText())) {
				SystemCommandResult scr = this.deALSContext.setActiveModule(argumentStr1);
				if (scr.getStatus() == ReturnStatus.SUCCESS) {					
					SystemCommandResult scr2 = this.deALSContext.getActiveModuleName();
					if (scr2.getStatus() == ReturnStatus.SUCCESS) {						
						result.message += "Set " + scr2.getMessage() + " to active module.";
					} else {
						result.setReturnStatus(scr2.getStatus());
						result.message += scr2.getMessage();
					}
				} else {
					result.setReturnStatus(scr.getStatus());
					result.setMessage("No such module '" + command.getCommandArguments().get(1));
				}
			} else {
				result.setReturnStatus(ReturnStatus.ERROR);
				result.setMessage("Illegal arguments '" + argumentStr0 + " " + argumentStr1 + "'");
			}
		}
		break;

		default:
		{
			result.setReturnStatus(ReturnStatus.ERROR);
			result.setMessage("Too many arguments '" + command.getCommandArguments().size() + "'");
		}
		break;
		}
		
		return result;
	}

	private CommandResult setOptions(Command command) {
		CommandResult result = new CommandResult(CommandType.SET, ReturnStatus.SUCCESS);
		
		if (command.getCommandArguments().size() > 2) {			
			result.setReturnStatus(ReturnStatus.ERROR);
			result.setMessage("Too many arguments '" + command.getCommandArguments().size() + "'");
		} else {
			String argumentStr0 = command.getCommandArguments().get(0);
			String argumentStr1 = null;
			
			if (command.getCommandArguments().size() >= 2)
				argumentStr1 = command.getCommandArguments().get(1);
			
			CommandOption option = CommandOption.getCommandOption(argumentStr0);
			if (option == null) {
				result.setReturnStatus(ReturnStatus.ERROR);
				result.setMessage("Unknown argument '" + argumentStr0 + "'");				
				return result;
			}

			switch (option) 
			{
			case TRACE:
			case DEBUG:
			case INFO:
			case STATISTICS:
			case DERIVATION:
			case WARN:
			{
				if (command.getCommandArguments().size() == 2) {
					LogLevel logLevel = null;
					switch (option) {
					case TRACE:
						logLevel = LogLevel.TRACE; 
						break;
					case DEBUG: 
						logLevel = LogLevel.DEBUG;
						break;
					case INFO:
						logLevel = LogLevel.INFO;
						break;
					case STATISTICS:
						logLevel = LogLevel.STATISTICS;
						break;
					case DERIVATION:
						logLevel = LogLevel.DERIVATION;
						break;
					case WARN:
						logLevel = LogLevel.WARN;
						break;
					}
					
					argumentStr1 = argumentStr1.toLowerCase();
					
					if (argumentStr1.equals(CommandOption.ON.getText()) || argumentStr1.equals(CommandOption.OFF.getText())) {
						if (argumentStr1.equals(CommandOption.ON.getText())) {
							this.options.setLogLevel(logLevel);
							this.deALSContext.setLogLevel(this.options.getLogLevel());
						} else if (argumentStr1.equals(CommandOption.OFF.getText())) {
							this.options.unsetLogLevel(logLevel);
							this.deALSContext.setLogLevel(this.options.getLogLevel());
						} else {
							result.setReturnStatus(ReturnStatus.FAIL);
						}
					} else {
						result.setReturnStatus(ReturnStatus.FAIL);
					}
				} else {
					result.setReturnStatus(ReturnStatus.FAIL);
				}
				
				result.message += this.options.toStringLogLevelSetting();

				break;
			} 

			case TIMER:
			{
				if (command.getCommandArguments().size() == 2) {
					TimerOption to = TimerOption.getTimerOption(Integer.parseInt(argumentStr1));
					if (to == null) {
						this.options.setTimerOff();
						this.deALSContext.setTimer(false);
					} else {
						this.options.setTimer(to);
						this.deALSContext.setTimer(true);
					}
				}
				result.message += this.options.toStringTimerSetting();
			} 
			}
		}
		
		return result;
	}

	private CommandResult describe(Command command) {
		CommandResult 	result = new CommandResult(CommandType.DESCRIBE, ReturnStatus.SUCCESS);
		boolean		describeModules = false;
		String 			argumentStr;

		if (command.getCommandArguments().size() == 0) {
			result.message += this.deALSContext.getModuleManager().toStringModules();
		} else if (command.getCommandArguments().size() == 1) {
			argumentStr = command.getCommandArguments().get(0);
						
			CommandOption option = CommandOption.getCommandOption(argumentStr);			
			if (option != null) {
				
				switch (option) 
				{
				case ALL:
					result.message += this.deALSContext.getModuleManager().toStringModules();					
					break;
				case MODULE:					
					result.message += this.deALSContext.getModuleManager().toStringModuleNames();
					break;
				case SCHEMA:
					result.message += this.deALSContext.getModuleManager().getActiveModule().toStringBasePredicates();
					break;
				case RULES:
					result.message += this.deALSContext.getModuleManager().getActiveModule().toStringRules();
					break;
				case EXPORT:
					result.message += this.deALSContext.getModuleManager().getActiveModule().toStringExports();
					break;
				default:
					describeModules = true;
					break;
				}
			} else {
				describeModules = true;
			}
		} else if (command.getCommandArguments().size() == 2) {
			argumentStr = command.getCommandArguments().get(0);
			String formatType = command.getCommandArguments().get(1);
			CommandOption option = CommandOption.getCommandOption(argumentStr);	
			if (option != null) {
				
				switch (option) 
				{
				case ALL:
					if (formatType.equals("json"))
						result.message += this.deALSContext.getModuleManager().toJsonModules();
					else
						result.message += this.deALSContext.getModuleManager().toStringModules();					
					break;
				case MODULE:
					if (formatType.equals("json"))
						result.message += this.deALSContext.getModuleManager().getActiveModule().toJson();
					else
						result.message += this.deALSContext.getModuleManager().getActiveModule().toString();
					break;
				case SCHEMA:
					result.message += this.deALSContext.getModuleManager().getActiveModule().toStringBasePredicates();
					break;
				case RULES:
					result.message += this.deALSContext.getModuleManager().getActiveModule().toStringRules();
					break;
				case EXPORT:
					result.message += this.deALSContext.getModuleManager().getActiveModule().toStringExports();
					break;
				default:
					describeModules = true;
					break;
				}
			}			
		} else if ((command.getCommandArguments().size() == 3) && (command.getCommandArguments().get(1).equals(CommandOption.SLASH.getText()))) {
			int	arity;

			if ((arity = getPositiveIntegerValue(command.getCommandArguments().get(2))) != -1) {
				argumentStr = command.getCommandArguments().get(0);
				String predicateName = argumentStr;
				BasePredicate schemaRelation = this.deALSContext.getModuleManager().getActiveModule().getBasePredicate(predicateName/*, arity*/);
				if (schemaRelation != null) {
					result.message += schemaRelation.toString();
				} else {
					DerivedPredicate derivedPredicate = this.deALSContext.getModuleManager().getActiveModule().getDerivedPredicate(predicateName, arity);
					if (derivedPredicate != null) {
						result.setMessage(derivedPredicate.toString());
					} else {
						result.setReturnStatus(ReturnStatus.FAIL);
						result.setMessage("No such predicate '[" + argumentStr + "|" + arity + "]'");						
					}
				}
			} else {
				result.setReturnStatus(ReturnStatus.FAIL);
				result.setMessage("Positive integer value expected for arity");
			}
		} else if (command.getCommandArguments().size() == 3) {
			// assume this is "describe active module name"
			result.setReturnStatus(ReturnStatus.SUCCESS);
			result.setMessage(this.deALSContext.getModuleManager().getActiveModule().getModuleName());			
		} else {
			describeModules = true;
		}

		if (describeModules) {
			for (String moduleName : command.getCommandArguments()) {
				Pair<ReturnStatus, Module> retvalPair = this.deALSContext.getModule(moduleName);

				if (retvalPair.getFirst() == ReturnStatus.SUCCESS) {
					Module module = this.deALSContext.getModuleManager().getModule(moduleName);
					if (module != null)
						result.message += module.toString();
				} else {
					result.setReturnStatus(retvalPair.getFirst());
					result.setMessage("No such module '" + moduleName + "'");					
				}
			}
		}
		
		return result;
	}
	
	private CommandResult addExports(Command command) {
		CommandResult result = new CommandResult(CommandType.ADD_EXPORT, ReturnStatus.SUCCESS);
		
		int counter = 0;
		for (String argument : command.getCommandArguments()) {
			if (counter > 0) 
				result.message += "\n";	
			result.message += "Adding export '" + argument + "'";
			
			SystemCommandResult scr = this.deALSContext.addExport(argument);
			if (scr.getStatus() != ReturnStatus.SUCCESS) {
				result.setReturnStatus(scr.getStatus());
				result.message += scr.getMessage();
				return result;
			}
			counter++;
		}
		
		return result;
	}
	
	private CommandResult deleteExport(Command command) {
		CommandResult result = new CommandResult(CommandType.DELETE_EXPORT, ReturnStatus.SUCCESS);

		result.message += "Deleting export '" + command.getOriginalCommandTextWithCommand() + "'";
		SystemCommandResult scr = this.deALSContext.deleteExport(command.getOriginalCommandTextWithCommand());
		result.setReturnStatus(scr.getStatus());
		result.message += scr.getMessage();

		return result;
	}
	/*
	private CommandResult setActiveDatabase(Command command) {
		CommandResult result = new CommandResult(CommandType.USE, ReturnStatus.SUCCESS);
		String databaseName = command.getCommandArguments().get(0);
		
		SystemCommandResult scr = this.deALSContext.setActiveDatabase(databaseName);
		result.setReturnStatus(scr.getStatus());
				
		if (scr.getStatus() == ReturnStatus.SUCCESS)		
			result.message += "Set '" + databaseName + "' to active database.";
		else
			result.message += "Can not set '" + databaseName + "' to active database.";
		result.message += scr.getMessage();
		
		return result;
	}
	
	private CommandResult createDatabase(Command command) {
		CommandResult result = new CommandResult(CommandType.CREATE_DB, ReturnStatus.SUCCESS);
		String databaseName = command.getCommandArguments().get(0);
		
		SystemCommandResult scr = this.deALSContext.createDatabase(databaseName);
		result.setReturnStatus(scr.getStatus());
		
		if (scr.getStatus() == ReturnStatus.SUCCESS)
			result.message += "Created Database '" + databaseName + "'";
		else
			result.message += "Failed to create Database '" + databaseName + "'";
		result.message += scr.getMessage();
		
		return result;
	}
	
	private CommandResult truncateRelation(Command command) {
		CommandResult result = new CommandResult(CommandType.TRUNCATE, ReturnStatus.SUCCESS);
		String relationName = command.getCommandArguments().get(0);
		int arity = Integer.parseInt(command.getCommandArguments().get(1));
		
		SystemCommandResult scr = this.deALSContext.truncateRelation(relationName, arity);
		result.setReturnStatus(scr.getStatus());
		
		if (scr.getStatus() == ReturnStatus.SUCCESS)
			result.message += "Truncated relation '" + relationName + "'";
		else
			result.message += "Failed to truncate relation '" + relationName + "'";
		result.message += scr.getMessage();
		
		return result;
	}	
	
	private CommandResult truncateDatabase(Command command) {
		CommandResult result = new CommandResult(CommandType.TRUNCATE_DB, ReturnStatus.SUCCESS);
		String databaseName = command.getCommandArguments().get(0);
		
		SystemCommandResult scr = this.deALSContext.truncateDatabase(databaseName);
		result.setReturnStatus(scr.getStatus());
				
		if (scr.getStatus() == ReturnStatus.SUCCESS)
			result.message += "Truncated Database '" + databaseName + "'";
		else
			result.message += "Failed to truncate Database '" + databaseName + "'";
		result.message += scr.getMessage();
		
		return result;
	}
	*/	
	public static CommandResult getDirectoryStructure(String directoryPath) {
		CommandResult commandResult = null;
		try {
			if (directoryPath == null || directoryPath.length() == 0) {
				commandResult = new CommandResult(CommandType.UNKNOWN, ReturnStatus.ERROR);
				commandResult.setMessage("Missing directory path");
			} else {
				Path rootDir = Paths.get(directoryPath);
				
				DirectoryVisitor dv = new DirectoryVisitor();
				Files.walkFileTree(rootDir, dv);
				commandResult = new CommandResult(CommandType.UNKNOWN, ReturnStatus.SUCCESS);
				commandResult.setMessage(dv.getJson());
			}
		} catch (Exception ex) {
			commandResult = new CommandResult(CommandType.UNKNOWN, ReturnStatus.ERROR);
			commandResult.setMessage(ex.getMessage());
		}
		return commandResult;
	}
	
	public static CommandResult getFiles(String directoryPath) {
		CommandResult commandResult = null;
		try {
			if (directoryPath == null || directoryPath.length() == 0) {
				commandResult = new CommandResult(CommandType.UNKNOWN, ReturnStatus.ERROR);
				commandResult.setMessage("Missing directory path");
			} else {

				File file = new File(directoryPath);
				File[] files = file.listFiles();
				StringBuilder retval = new StringBuilder();				
				
				retval.append("{\"files\":[");
				for (int i = 0; i < files.length; i++) {
					if (i > 0)
						retval.append(",");
					retval.append("{\"fileName\":\"");
					retval.append(files[i].getName());
					retval.append("\", \"timestamp\":");
					retval.append(files[i].lastModified());
					retval.append("}");
				}
				retval.append("]}");
				
				commandResult = new CommandResult(CommandType.UNKNOWN, ReturnStatus.SUCCESS);
				commandResult.setMessage(retval.toString());
			}
		} catch (Exception ex) {
			commandResult = new CommandResult(CommandType.UNKNOWN, ReturnStatus.ERROR);
			commandResult.setMessage(ex.getMessage());
		}
		return commandResult;
	}
	
	private static Pair<Boolean, String> extractDestinationName(List<String> argumentList) {		
		String name = null;
		
		int intoPosition = 0;
		for (String argument : argumentList) {
			if (argument.toLowerCase().equals("into"))
				break;
			intoPosition++;
		}

		if ((intoPosition + 1) <= argumentList.size()) {
			// get the filename
			name = argumentList.get(intoPosition + 1);
			// remove the rest of the list
			int size = argumentList.size();
			for (int i = size - 1; i >= intoPosition; i--)
				argumentList.remove(i);
		}		
		
		if (name == null)
			return null;

		// otherwise, it is the name of the base predicate to use
		return new Pair<>(true, name.trim());
	}

	private static int getPositiveIntegerValue(String argumentStr) {
		int intValue;

		try {
			intValue = Integer.parseInt(argumentStr);
			if (intValue < 0)
				intValue = -1;
		} catch (Exception ex) {
			// catch error with -1
			intValue = -1;
		}	

		return intValue;
	}

}
