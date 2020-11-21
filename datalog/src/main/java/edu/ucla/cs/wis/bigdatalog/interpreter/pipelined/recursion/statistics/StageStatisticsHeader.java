package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.statistics;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

// import org.apache.logging.log4j.ThreadContext;

import com.google.gson.Gson;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;

// this class use a builder pattern
// 1) Instantiate statistics summary
// 2a) Start stage 0
// 2b) Complete stage 0
// 2c) Write tuples from stage 0 to file tuple-0
// na) Start stage n
// nb) Complete stage n
// nc) Write tuples from stage n to file tuple-n
// Final) Write statistics summary to log
public class StageStatisticsHeader {	

	protected String predicateName;
	protected int arity;
	protected int numberOfKeys;
	
	protected transient int currentProvenanceId;
	protected transient List<Triple<Integer, Tuple, List<Tuple>>> provenances;

	protected List<StageData> stageStatistics;
	protected transient int numberOfTuplesAdded;
	protected transient int numberOfTuplesUpdated;
	protected int runningTotalNumberOfTuplesAdded;
	protected int runningTotalNumberOfTuplesUpdated;
	protected transient int currentStageId;
	protected transient boolean stageInitialized;
	protected transient List<Pair<Tuple, String>> stageTuples;
			
	public StageStatisticsHeader(String predicateName, int arity, int numberOfKeys) {
		this.predicateName = predicateName;
		this.arity = arity;
		this.numberOfKeys = numberOfKeys;
		this.initialize();
	}

	private void initialize() {
		this.stageStatistics = new ArrayList<>();
		this.currentStageId = 0;
		this.stageInitialized = false;
		this.stageTuples = null;
		this.numberOfTuplesAdded = 0;
		this.numberOfTuplesUpdated = 0;
		this.runningTotalNumberOfTuplesAdded = 0;
		this.runningTotalNumberOfTuplesUpdated = 0;
		this.currentProvenanceId = 0;
	}
	
	public void reset() {
		this.initialize();
	}
	
	public void markTupleAdded(Tuple tuple) {
		this.numberOfTuplesAdded++; 
		if (tuple != null) {
			if (this.stageTuples == null)
				this.stageTuples = new LinkedList<>();
			this.stageTuples.add(new Pair<>(tuple.copy(), "add"));
		}
	}
	
	public void markTupleUpdated(Tuple tuple) {
		this.numberOfTuplesUpdated++; 
		if (tuple != null) {
			if (this.stageTuples == null)
				this.stageTuples = new LinkedList<>();
			this.stageTuples.add(new Pair<>(tuple.copy(), "update"));
		}
	}
	
	public void addProvenance(Tuple tuple, List<Tuple> provenance) {
		if (this.provenances == null)
			this.provenances = new ArrayList<>();
		this.provenances.add(new Triple<>(currentProvenanceId++, tuple, provenance));
	}

	public void complete(boolean lastStage) {
		if (lastStage) {
			// don't write empty file for last stage
			if (numberOfTuplesAdded == 0 && numberOfTuplesUpdated == 0)
				return;
		}
		this.runningTotalNumberOfTuplesAdded += numberOfTuplesAdded;
		this.runningTotalNumberOfTuplesUpdated += numberOfTuplesUpdated;
		
		// create log file and add tuples
		String fileName = this.startTupleLog(String.valueOf(this.currentStageId));
		StageData stage = new StageData(this.predicateName, this.currentStageId, 
				this.runningTotalNumberOfTuplesAdded, 
				numberOfTuplesAdded, 
				this.runningTotalNumberOfTuplesUpdated, 
				numberOfTuplesUpdated, 
				fileName, 
				this.numberOfKeys);	
		
		stage.processTuples(this.stageTuples);
		String provenanceFileName = stage.processProvenances(this.provenances);
		this.provenances = null;
		this.currentProvenanceId = 0;
		this.endTupleLog();
		
		stage.setProvenanceFileName(provenanceFileName);
		this.stageStatistics.add(stage);

		this.currentStageId++;
		this.stageTuples = null;
		this.numberOfTuplesAdded = 0;
		this.numberOfTuplesUpdated = 0;
		
		this.stageInitialized = false;
	}
	
	public String toJson() {
		StringBuilder retval = new StringBuilder();
		retval.append("{\"statistics\":");
		retval.append("{\"predicateName\":\"");
		retval.append(this.predicateName);
		retval.append("\",\"arity\":");
		retval.append(this.arity);
		retval.append(",\"numberOfKeys\":");
		retval.append(this.numberOfKeys);
		retval.append(",\"stageStatistics\":");
		Gson gson = new Gson();
		retval.append(gson.toJson(this.stageStatistics));
		retval.append("}}");
		return retval.toString();
	}
		
	private String startTupleLog(String stageId) {
		// String fileName2 = ThreadContext.get("queryLogFileName") + "-" + predicateName + "-tuples-" + stageId;
		// ThreadContext.put("tupleLogFileName", fileName2);
		// return fileName2;
		return "";
	}
	
	private void endTupleLog() {
		// ThreadContext.put("tupleLogFileName", null);
	}

}
