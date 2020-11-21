package edu.ucla.cs.wis.bigdatalog.system;

public enum LogLevel {
	//OFF(0),
	TRACE(1),
	DEBUG(2),
	INFO(4),
	STATISTICS(8),
	DERIVATION(16),
	WARN(32),
	ERROR(64),
	FATAL(128);	
	
	private int id;
	private LogLevel(int id) {
		this.id = id;
	}
	
	public int getLogLevelId() { return this.id; }
	
	public static LogLevel getLogLevel(int id) {
		for (LogLevel val : LogLevel.values()) {
			if (val.id == id)
				return val;
		}
		
		return null;
	}
}
