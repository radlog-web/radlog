package edu.ucla.cs.wis.bigdatalog.interpreter;

import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;

public class ProgramGenerationResult {
	protected QueryForm queryForm;
	protected String message;
	
	public ProgramGenerationResult(QueryForm queryForm, String message) {
		this.queryForm = queryForm;
		this.message = message;
	}
	
	public String getMessage() { return this.message; }
	
	public QueryForm getQueryForm() { return queryForm; }
	
	public boolean isSuccess() { return this.queryForm != null; }
	
}
