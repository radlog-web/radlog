package edu.ucla.cs.wis.bigdatalog.compiler.language;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;

public class ParseResult {

	private ReturnStatus status;
	private CompilerTypeBase parserOutput;
	private String message;
		
	public ParseResult(ReturnStatus status, CompilerTypeBase parserOutput, String message) {
		this.status = status;
		this.parserOutput = parserOutput;
		this.message = message;
	}

	public ReturnStatus getStatus() { return status; }

	public void setStatus(ReturnStatus status) { this.status = status; }

	public CompilerTypeBase getParserOutput() { return parserOutput; }

	public void setParserOutput(CompilerTypeBase parserOutput) { this.parserOutput = parserOutput; }
	
	public String getMessage() { return message; }

	public void setMessage(String message) { this.message = message; }

}