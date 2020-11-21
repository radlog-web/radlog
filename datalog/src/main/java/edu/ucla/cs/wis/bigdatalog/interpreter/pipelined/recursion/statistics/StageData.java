package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.statistics;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
// import org.apache.logging.log4j.ThreadContext;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.common.Triple;
import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public class StageData {
	private static Logger loggerTuple = LoggerFactory.getLogger("StageTupleLogger");
	private static Logger loggerProvenance = LoggerFactory.getLogger("StageTupleProvenanceLogger");
	
	protected String predicateName;
	protected int stageId;
	protected int tuplesAdded;
	protected int tuplesUpdated;
	protected int tuplesGenerated;
	protected int totalTuplesAdded;
	protected int totalTuplesUpdated;
	protected int totalTuplesGenerated;
	protected String fileName;
	protected String provenanceFileName;
	protected int numberOfKeys;

	public StageData(String predicateName, int stageId, int runningTotalNumberOfTuplesAdded, int numberOfTuplesAdded, 
			int runningTotalNumberOfTuplesUpdated, int numberOfTuplesUpdated, String fileName, int numberOfKeys) {
		this.predicateName = predicateName;
		this.stageId = stageId;
		this.tuplesAdded = numberOfTuplesAdded;
		this.tuplesUpdated = numberOfTuplesUpdated;
		this.tuplesGenerated = numberOfTuplesAdded + numberOfTuplesUpdated;
		this.totalTuplesAdded = runningTotalNumberOfTuplesAdded;
		this.totalTuplesUpdated = runningTotalNumberOfTuplesUpdated;
		this.totalTuplesGenerated = runningTotalNumberOfTuplesAdded + runningTotalNumberOfTuplesUpdated;
		this.fileName = fileName;
		this.numberOfKeys = numberOfKeys;
	}

	protected void setProvenanceFileName(String provenanceFileName) {
		this.provenanceFileName = provenanceFileName;
	}

	protected void processTuples(List<Pair<Tuple, String>> tuples) {
		if (tuples == null)
			tuples = new ArrayList<>();

			Pair<Tuple, String> pair;
			StringBuilder tupleStr;
			Tuple tuple;
			int superceededBy = -1;

			loggerTuple.info("[");
			for (int i = 0; i < tuples.size(); i++) {
				pair = tuples.get(i);
				tuple = pair.getFirst();
				tupleStr = new StringBuilder();
				superceededBy = isSuperceeded(i + 1, tuple, tuples);

				if (i > 0)
					tupleStr.append(", ");

				tupleStr.append("{\"tuple\":");
				if (tuple instanceof AddressedTuple)
					tupleStr.append(((AddressedTuple)tuple).toJson(false));
				else
					tupleStr.append(tuple.toJson());
				tupleStr.append(",\"status\":\"");
				tupleStr.append(pair.getSecond());
				tupleStr.append("\"");
				tupleStr.append(",\"isMax\":\"");
				tupleStr.append(String.valueOf(superceededBy));
				tupleStr.append("\"");
				tupleStr.append("}");
				loggerTuple.info(tupleStr.toString());
			}
			loggerTuple.info("]");
	}

	private int isSuperceeded(int start, Tuple tuple, List<Pair<Tuple, String>> tuples) {
		for (int i = start; i < tuples.size(); i++) {
			int j;
			for (j = 0; j < numberOfKeys; j++) {
				if (!tuple.getColumn(j).equals(tuples.get(i).getFirst().getColumn(j))) {
					break;
				}
			}
			if (j == numberOfKeys)
				return i;
		}
		return -1;
	}

	protected String processProvenances(List<Triple<Integer, Tuple, List<Tuple>>> provenances) {
		if (provenances == null)
			return null;

		// String fileName = ThreadContext.get("queryLogFileName") + "-" + this.predicateName + "-provenance-" + this.stageId;
		// ThreadContext.put("tupleProvenanceLogFileName", fileName);

		Triple<Integer, Tuple, List<Tuple>> provenance;
		loggerProvenance.info("[");
		for (int i = 0; i < provenances.size(); i++) {
			provenance = provenances.get(i); 

			if (i > 0)
				loggerProvenance.info(",");	

			loggerProvenance.info("{");
			loggerProvenance.info("\"tuple\":");
			if (provenance.getSecond() instanceof AddressedTuple)
				loggerProvenance.info(((AddressedTuple)provenance.getSecond()).toJson(false));
			else
				loggerProvenance.info(provenance.getSecond().toJson());
			loggerProvenance.info(",\"provenance\":[");

			List<Tuple> provenanceTuples = provenance.getThird();
			int count = 0;
			for (Tuple tuple : provenanceTuples) {
				if (tuple instanceof AddressedTuple) {
					if (count > 0)
						loggerProvenance.info(",{}", ((AddressedTuple)tuple).toJson(false));
					else
						loggerProvenance.info(((AddressedTuple)tuple).toJson(false));
				} else {
					if (count > 0)
						loggerProvenance.info(",{}", tuple.toJson());
					else
						loggerProvenance.info(tuple.toJson());
				}
			}

			loggerProvenance.info("]}");
		}
		loggerProvenance.info("]");
		// ThreadContext.put("tupleProvenanceLogFileName", null);

		return fileName;
	}
}

