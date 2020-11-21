package edu.ucla.cs.wis.bigdatalog.system;

import edu.ucla.cs.wis.bigdatalog.compiler.language.ParseResult;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;

public class SystemCommandResult {

	private ReturnStatus status;
	private CompilerTypeBase object;
	private String message;
	
	public SystemCommandResult(ReturnStatus status, String message) {
		this(status);
		this.message = message;
	}
	
	public SystemCommandResult(ReturnStatus status, CompilerTypeBase object) {
		this(status);
		this.object = object;
	}
	
	public SystemCommandResult(ReturnStatus status, CompilerTypeBase object, String message) {
		this(status);
		this.object = object;
		this.message = message;
	}
	
	public SystemCommandResult(ReturnStatus status) {
		this.status = status;
	}
	
	public SystemCommandResult(ParseResult result) {
		this(result.getStatus(), result.getParserOutput(), result.getMessage());		
	}

	public ReturnStatus getStatus() {
		return status;
	}

	public void setStatus(ReturnStatus status) {
		this.status = status;
	}

	public String getMessage() {
		if (message == null)
			return "";
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public CompilerTypeBase getObject() {
		return object;
	}

	public void setObject(CompilerTypeBase object) {
		this.object = object;
	}	
	
	public String toString() {
		return this.status + " | " + this.message;
	}
}
