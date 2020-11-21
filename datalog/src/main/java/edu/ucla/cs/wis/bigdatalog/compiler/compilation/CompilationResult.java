package edu.ucla.cs.wis.bigdatalog.compiler.compilation;

import edu.ucla.cs.wis.bigdatalog.compiler.ProgramRules;
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph.PCGOrNode;

public class CompilationResult {
	protected QueryForm queryForm;
	protected ProgramRules programRules;
	protected PCGOrNode compiledProgram;
	protected String message;

	public CompilationResult(String message) {
		this.message = message;
	}

	public CompilationResult(QueryForm queryForm, ProgramRules programRules, PCGOrNode compiledProgram, String message) {
		this.queryForm = queryForm;
		this.programRules = programRules;
		this.compiledProgram = compiledProgram;
		this.message = message;		
	}
		
	public QueryForm getQueryForm() { return this.queryForm; }
	
	public ProgramRules getProgramRules() { return this.programRules; }
	
	public PCGOrNode getCompiledProgram() { return this.compiledProgram; }
	
	public void setMessage(String message) { this.message = message; }
	
	public String getMessage() { return this.message; }
	
	public boolean isSuccess() { return this.queryForm != null; }
	
	public String toString() { 
		StringBuilder output = new StringBuilder();
		output.append("QueryForm: " + this.queryForm.toString() + "\n");
		output.append("ProgramRules: " + this.programRules.toString() + "\n");
		output.append("PCG: " + this.compiledProgram.toString() + "\n");
		output.append("Message: " + this.message);
		return output.toString();
	}

}
