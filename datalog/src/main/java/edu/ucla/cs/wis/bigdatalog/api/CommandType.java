package edu.ucla.cs.wis.bigdatalog.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum CommandType {
	// 12/15/2013 APS - made argument appear singluar for class use
	// unknown command (instead of null)
	UNKNOWN(-1, "unknown", 0, 0, "", "unknown command", 0),

	// affect objects and settings loaded in system 
	//LOAD(0, "load", 1, -1, "[facts file with .fac file extension] \n\t\t[modulename with .deal file extension] \n\t\t[database with .deals file extension]", "loads database objects from file.", 1),
	LOAD(0, "load", 1, -1, "[facts file with .fac extension | program file with .deal extension]", "loads database objects from file.", 1),
	UNLOAD(1, "unload", 0, -1, "[no arguments for all | name of module to close]", "removes the module's schema, rules and facts.", 1),
	DEFAULT(2, "default", 0, -1, "no arguments to get default module and schema \n\t\t\"module\" for default module name \n\t\t\"schema\" for default schema name \n\t\t\"module\" + module name \n\t\t\"schema\" + schema name", "shows or changes the default module.", 0),
	DESCRIBE(4, "describe", 0, -1, "\"all\" | \"module\" | \"schema\" | \"rules\" | \"export\"", "shows objects from the active database.", 1),
	SET(8, "set", 2, 2, "[option type] [option value]", "changes the value of a system setting.", 1),
	USE(17, "use", 0, 1, "database name", "makes database current active database.", 0),
	
	COMPILE(3, "compile", 0, -1, "queryform", "compiles query form into a program to be executed.", 1),
	
	QUERY(5, "query", 1, -1 , "queryform", "query to execute against the database.", 1),
	
	// DML
	ADD_FACT(6, "add-fact", 1, -1 , "fact", "adds a fact into the database.", 1),
	DELETE_FACT(7, "delete-fact", 1, -1, "fact", "removes a fact from the database.", 1),
	TRUNCATE(18, "truncate", 2, 2, "[base relation name] [arity]", "removes all facts from a base relation.", 1),

	// DDL
	CREATE_DB(13, "create-db", 1, 1, "database name", "creates a new database", 0),
	SAVE_DB(14, "save-db", 1, 1, "db name", "save the database into a file on disk.", 0),
	TRUNCATE_DB(15, "truncate-db", 1, 1, "database name", "clears a database of all data", 0),
	DELETE_DB(16, "delete-db", 1, 1, null, "destroys all data in the database.", 0),
	
	ADD_RULE(9, "add-rule", 1, -1, "rule", "add a rule.", 1),
	DELETE_RULE(10, "delete-rule", 1, 2, "[rule] [arity]", "delete a rule from the active module.", 1),
	
	ADD_EXPORT(11, "add-export", 1, 1, "export statement", "adds an export to the active module.", 1),
	DELETE_EXPORT(12, "delete-export", 1, 1, "export statement", "delete an export from the active module.", 1);
	
	private int id;
	private String name;
	private int minimumNumberOfArgs;
	private int maximumNumberOfArgs;
	private String argumentDescription;
	private String description;
	private int active;
		
	private CommandType(int id, String name, int minimumNumberOfArgs, int maximumNumberOfArgs, String argumentDescription, String description, int active) {
		this.id = id;
		this.name = name;
		this.minimumNumberOfArgs = minimumNumberOfArgs;
		this.maximumNumberOfArgs = maximumNumberOfArgs;
		this.argumentDescription = argumentDescription;
		this.description = description;
		this.active = active;
	}
	
	private CommandType(String name, String argumentDescription, String description) {
		this.name = name;		
		this.argumentDescription = argumentDescription;
		this.description = description;
	}
	
	public int getId() { return this.id; }	
	public String getName() { return this.name; }
	public String getDescription() { return this.description; }
	public String getArgumentDescription() { return this.argumentDescription; }	
	
	public boolean isValidNumberOfArgs(int numberOfArgs) {
		if (numberOfArgs < this.minimumNumberOfArgs)
			return false;
		if ((this.maximumNumberOfArgs >= 0) && (numberOfArgs > this.maximumNumberOfArgs))
			return false;
		
		return true;
	}

	public static CommandType fromName(String name) {
		for (CommandType type : CommandType.values()) {
			if (name.toLowerCase().equals(type.name))
				return type;
		}
		return null;
	}
	
	public static CommandType fromId(int id) {
		for (CommandType type : CommandType.values()) {
			if (type.id == id)
				return type;
		}
		return null;
	}
	
	public static List<String> getSortedList() {
		List<CommandType> types = Arrays.asList(CommandType.values());
		List<String> list = new ArrayList<>();
		
		StringBuilder entry;
		for (CommandType type : types) {
			if (type.active == 0)
				continue;
			entry = new StringBuilder();
			entry.append("\t");
			entry.append(type.getName());	

			if (type.getArgumentDescription() != null) {
				//entry.append("\nARGUMENTS\t");
				if (type.getArgumentDescription().startsWith("["))
					entry.append(" " + type.getArgumentDescription());
				else
					entry.append(" [" + type.getArgumentDescription() + "]");
			}

			if (type.getDescription() != null) {
				entry.append("\n\t - ");
				entry.append(type.getDescription());
			}
			
			list.add(entry.toString());
		}

		//list.add("NAME\t\texamples\nDESCRIPTION\tlist example demo applications");
		
		//list.add("NAME\t\texit\nDESCRIPTION\tquits from the system");
		//list.add("NAME\t\t!!\nDESCRIPTION\texecutes last command");
		//list.add("NAME\t\t!\nARGUMENTS\thistoryindex\nDESCRIPTION\texecutes the past command with historyindex");
		list.add("\texit\n\t - goodbye");
		list.add("\t!!\n\t - executes last command");
		list.add("\t! [historyindex]\n\t - executes the past command with historyindex");
		
		java.util.Collections.sort(list);
		
		return list;
	}
}
