package edu.ucla.cs.wis.bigdatalog.api;

import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;

public class CommandResult {
	protected CommandType	commandType;
	protected ReturnStatus 	returnStatus;
	protected String 		message;
	protected QueryResults	queryResults;
	
	public CommandResult(CommandType commandType, ReturnStatus returnStatus) {
		this(commandType, returnStatus, "");
	}
	
	public CommandResult(CommandType commandType, ReturnStatus returnStatus, String message) {
		this(commandType, returnStatus, message, null);
	}

	public CommandResult(CommandType commandType, ReturnStatus returnStatus, String message, QueryResults queryResults) {
		this.commandType = commandType;
		this.returnStatus = returnStatus;
		this.message = message;
		this.queryResults = queryResults;				
	}

	public CommandType getCommandType() { return this.commandType; }
	
	public void setReturnStatus(ReturnStatus returnStatus) {
		this.returnStatus = returnStatus;
	}
	public ReturnStatus getReturnStatus() { return this.returnStatus; }

	public String getMessage() { return this.message; }
	
	public void setMessage(String message) { this.message = message; }

	public QueryResults getQueryResults() { return this.queryResults; }
	
	public String toString() {
		return toString(false);
	}
	
	public String toString(boolean showResults) {
		StringBuilder output = new StringBuilder();
		output.append("CommandType = ");
		if (this.commandType != null)
			output.append(this.commandType.getName());
		else
			output.append("unknown");
		output.append("\nReturnStatus = ");		
		output.append(this.returnStatus.name());
		output.append("\nMessage = ");
		output.append(this.message);
		if (this.commandType == CommandType.QUERY && this.queryResults != null) {
			output.append("\n");
			output.append(this.queryResults.toString(showResults));
		}
				
		return output.toString();
	}
	
	public String toStringShort(boolean showResults) {
		StringBuilder output = new StringBuilder();			
		
		if (this.message != null) {
			output.append(this.message);
			if (!this.message.endsWith(".") && !this.message.endsWith("\n"))
				output.append(".");
		}
		
		if (this.commandType == CommandType.QUERY && this.queryResults != null) {
			output.append("\n");
			output.append(this.queryResults.toString(showResults));
		}
		
		if (this.returnStatus != ReturnStatus.SUCCESS) {
			String name = this.returnStatus.name().substring(0, 1);
			name += this.returnStatus.name().substring(1, this.returnStatus.name().length()).toLowerCase();
			if (!output.toString().endsWith("\n"))
				output.append("\n");
			output.append(name);
			if (this.returnStatus == ReturnStatus.SUCCESS)
				output.append("!");
		}
								
		return output.toString();
	}
	
	public String toJson() {
		StringBuilder output = new StringBuilder();
		output.append("{\"CommandType\":\"");
		if (this.commandType != null) {			
			output.append(this.commandType.getName());
		} else {
			output.append("unknown");
		}
		output.append("\",\"returnStatus\":\"");
		output.append(this.returnStatus.name());
		output.append("\",\"message\":");
		if (this.message == null || this.message.length() == 0) {
			output.append("\"\"");
		} else {
			if (this.message.startsWith("{")) {
				output.append(this.message);
			} else {
				output.append("\"");
				output.append(this.message);
				output.append("\"");			
			}
		}
		
		if ((this.commandType == CommandType.QUERY) 
				&& (this.queryResults != null)) {
			output.append(",\"queryResults\":");
			output.append(this.queryResults.toJson(false));
		}
		output.append("}");
		return output.toString();
	}
}
