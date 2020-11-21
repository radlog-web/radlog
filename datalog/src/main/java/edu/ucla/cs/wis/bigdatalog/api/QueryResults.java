package edu.ucla.cs.wis.bigdatalog.api;

import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.system.ReturnStatus;

public class QueryResults {
	protected String 				queryId;
	protected QueryForm				queryForm;
	protected ReturnStatus 			returnStatus;
	protected Tuple[] 				results;
	protected int					numberOfResults;
	protected String				programExecutionInfo;
	protected long 					executionTime;
	protected long 					memoryUsed;
	protected String 				memoryMeasuredMessage;
	protected MemoryMeasurement 	memoryMeasurementFS;
	protected String 				memoryMeasuredMessageFS;
	protected List<RecursionInformation> recursionInformationByClique;
	protected boolean				verbose;
	
	public QueryResults(String queryId, 
			QueryForm queryForm, 
			ReturnStatus returnStatus, 
			boolean saveResults, 
			String programExecutionInfo, 
			List<RecursionInformation> recursionInformationByClique, 
			boolean existentialQueryResult, 
			boolean verbose, Database database) {
		this.queryId = queryId;
		this.queryForm = queryForm;
		this.returnStatus = returnStatus;
		this.programExecutionInfo = programExecutionInfo;
		this.recursionInformationByClique = recursionInformationByClique;
		this.verbose = verbose;

		if (this.queryForm.getArity() > 0) {		
			if (saveResults) {
				int counter = 0;
				this.results = new Tuple[this.queryForm.getQueryFormRelation().getTupleStore().getNumberOfTuples()];
				Cursor cursor = this.queryForm.getQueryFormCursor(database);
				Tuple tuple = cursor.getEmptyTuple();
				cursor.reset();
				Tuple queryFormTuple = new Tuple(this.queryForm.getArity());
				while (cursor.getTuple(tuple) > 0) {
					for (int i = 0; i < this.queryForm.getArity(); i++)
						queryFormTuple.columns[i] = tuple.columns[i];
					
					this.results[counter++] = queryFormTuple.copy();
				}
			}
			this.numberOfResults = this.queryForm.getQueryFormRelation().getTupleStore().getNumberOfTuples();			
		} else {
			this.results = new Tuple[1];
			// existential queries
			this.numberOfResults = 1;
			Tuple tuple = new Tuple(1);
			
			if (existentialQueryResult)
				tuple.setColumn(0, database.getTypeManager().createString("true"));
			else 
				tuple.setColumn(0, database.getTypeManager().createString("false"));

			this.results[0] = tuple;
		}
	}
		
	public String getQueryId() { return this.queryId; }
	
	public QueryForm getQueryForm() { return this.queryForm; }
	
	public ReturnStatus getReturnStatus() { return this.returnStatus; }
	
	public Tuple[] getResults() { return this.results; }
	
	public long getExecutionTime() { return this.executionTime; }
	
	public void setExecutionTime(long executionTime) { this.executionTime = executionTime; }
	
	public long getMemoryUsed() { return this.memoryUsed; }
	
	public void setMemoryUsed(long memoryUsed) { this.memoryUsed = memoryUsed; }

	public String getMemoryMeasuredMessage() { return this.memoryMeasuredMessage; }
	
	public void setMemoryMeasuredMessage(String memoryMeasuredMessage) { this.memoryMeasuredMessage = memoryMeasuredMessage; }
	
	public String getMemoryMeasuredMessageFS() { return this.memoryMeasuredMessageFS; }
	
	public void setMemoryMeasuredMessageFS(String memoryMeasuredMessageFS) { this.memoryMeasuredMessageFS = memoryMeasuredMessageFS; }
	
	public MemoryMeasurement getMemoryMeasuredFS() { return this.memoryMeasurementFS; }
	
	public void setMemoryMeasuredFS(MemoryMeasurement memoryMeasurement) { this.memoryMeasurementFS = memoryMeasurement; }
	
	public int getNumberOfResults() { return this.numberOfResults; }
				
	public String getProgramExecutionInfo() { return this.programExecutionInfo; }
	
	public List<RecursionInformation> getRecursionInformation() { return this.recursionInformationByClique; }
	
	public String getRecursionsByCliqueInfo() {
		StringBuilder output = new StringBuilder();
		
		for (int i = 0; i < this.recursionInformationByClique.size(); i++) {
			if (i > 0)
				output.append("\n");
			RecursionInformation ri = this.recursionInformationByClique.get(i);
	
			output.append(ri.name);
			output.append(" [" + ri.evaluationType + "]: "); 
			output.append(ri.iterations + " iterations ");
			output.append("\n");
			List<Integer> deltaFactsPerIteration = this.recursionInformationByClique.get(i).numberOfDeltaFactsByIteration;
			long count = 0;
			for (Integer integer : deltaFactsPerIteration)
				count += integer;
			output.append(deltaFactsPerIteration + " => " + count + " delta facts total");
			output.append("\n");
			List<Integer> generatedFactsPerIteration = this.recursionInformationByClique.get(i).numberOfGeneratedFactsByIteration;
			count = 0;
			for (Integer integer : generatedFactsPerIteration)
				count += integer;
			output.append(generatedFactsPerIteration + " => " + count + " generated facts total");
			
		}

		return output.toString(); 
	}
		
	public String toString() {
		return toString(false);
	}
	
	public String toString(boolean showResults) {
		StringBuilder output = new StringBuilder();
		if (this.verbose) {//DeALSContext.getConfiguration().compareProperty("deals.interpreter.queryresults.verbose", "true")) {
			output.append("Query Id = " + this.queryId);
			output.append("\nQuery Form = " + this.queryForm.toString());
			output.append("\nReturn Status = " + this.returnStatus.name());
			output.append("\nExecution time = " + this.executionTime + "ms");
			if (this.memoryMeasuredMessage != null)
				output.append(this.memoryMeasuredMessage);
		}

		if (showResults) {
			for (Tuple tuple : this.getResults()) {
				output.append("\n");
				output.append(this.queryForm.getPredicateName() + tuple.toString());
			}
		}
		if (this.numberOfResults == 1)
			output.append("\n1 result");
		else
			output.append("\n" + this.numberOfResults + " results");
		return output.toString();
	}
	
	public String toJson(boolean includeResults) {
		StringBuilder output = new StringBuilder();
		output.append("{\"queryId\":\"");
		output.append(this.queryId);
		output.append("\", \"executionTime\":");
		output.append(this.executionTime);
		output.append(", \"queryForm\":");
		output.append(this.queryForm.toJson());
		output.append(", \"returnStatus\":\"");
		output.append(this.returnStatus);
		output.append("\", ");
		output.append("\"numberOfResults\":");
		output.append(this.numberOfResults);
		
		if (includeResults) {
			if (this.results != null) {
				output.append(",");
				output.append("\"results\":[");
				int count = 0;
				for (Tuple tuple : this.results) {
					if (count > 0)
						output.append(",");
					output.append(tuple.toJson());
					count++;
				}
				output.append("]");
			}
		}
		output.append("}");
				
		return output.toString();
	}
}
