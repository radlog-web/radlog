package edu.ucla.cs.wis.bigdatalog.api;

public enum CommandOption {
	MODULE("module"),
	SCHEMA("schema"),
	COMPILE("compile"),
	HISTORY("history"),
	TRACE("trace"),
	DEBUG("debug"),
	INFO("info"),
	WARN("warn"),
	DERIVATION("derivation"),
	TIMER("timer"),
	YES("yes"),
	NO("no"),
	ON("on"),
	OFF("off"),
	ALL("all"),
	FACTS("facts"),
	RULES("rules"),
	EXPORT("export"),
	SLASH("/"),
	STATISTICS("statistics");
	
	private String text;
	
	private CommandOption(String text) {
		this.text = text;
	}
	
	public String getText() { return this.text; } 
	
	public static CommandOption getCommandOption(String text) {
		if (text == null || text.length() == 0) return null;
		
		for (CommandOption option : CommandOption.values()) {
			if (text.toLowerCase().equals(option.text))
				return option;
		}
		return null;
	}
}
