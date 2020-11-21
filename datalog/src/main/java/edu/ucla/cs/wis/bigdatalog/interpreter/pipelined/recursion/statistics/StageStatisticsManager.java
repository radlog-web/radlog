package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.statistics;

import java.util.HashMap;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.AggregateRewriter;
import edu.ucla.cs.wis.bigdatalog.compiler.rewriting.Rewriter;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;

public class StageStatisticsManager {
	private static Logger loggerStatistics = LoggerFactory.getLogger("StageStatisticsLogger");
	
	public HashMap<String, StageStatisticsHeader> statistics;
	
	private static class StageStatisticsManagerHolder {
		public static final StageStatisticsManager INSTANCE = new StageStatisticsManager();
	}
	
	public static StageStatisticsManager getInstance() {
		return StageStatisticsManagerHolder.INSTANCE;
	}
	
	private StageStatisticsManager() {
		this.statistics = new HashMap<>();
	}
	
	public void markTupleAdded(String predicateName, Tuple tuple) {
		predicateName = extractPredicateName(predicateName);
		StageStatisticsHeader ssh = this.getStatistics(predicateName);
		ssh.markTupleAdded(tuple);
	}
	
	public void markTupleUpdated(String predicateName, Tuple tuple) {
		predicateName = extractPredicateName(predicateName);
		StageStatisticsHeader ssh = this.getStatistics(predicateName);
		ssh.markTupleUpdated(tuple);
	}
	
	public void stageComplete(String predicateName) {
		predicateName = extractPredicateName(predicateName);
		StageStatisticsHeader ssh = this.getStatistics(predicateName);		
		ssh.complete(false);
	}
	
	public void startTracking(String predicateName, int arity, int numberOfKeys) {
		predicateName = extractPredicateName(predicateName);
		StageStatisticsHeader ssh = new StageStatisticsHeader(predicateName, arity, numberOfKeys);
		// if we're already tracking, it is ok
		if (!this.statistics.containsKey(predicateName))
			this.statistics.put(predicateName,  ssh);
	}
	
	public void startTrackingFromFSAggregate(String predicateName, int arity, int numberOfKeys) {
		String cliquePredicateName = predicateName.substring(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX.length());
		cliquePredicateName = cliquePredicateName.substring(0, cliquePredicateName.lastIndexOf("_"));
		StageStatisticsHeader ssh = new StageStatisticsHeader(cliquePredicateName, arity, numberOfKeys);
		// if we're already tracking, it is ok
		if (!this.statistics.containsKey(cliquePredicateName))
			this.statistics.put(cliquePredicateName,  ssh);
	}
	
	public void endCollecting() {				
		loggerStatistics.info("[");
		
		int i = 0;
		for (StageStatisticsHeader ssh : this.statistics.values()) {
			ssh.complete(true);
			if (i > 0)
				loggerStatistics.info(", {}", ssh.toJson());
			else
				loggerStatistics.info(ssh.toJson());
			i++;
		}

		loggerStatistics.info("]");
		this.statistics.clear();
	}
	
	private StageStatisticsHeader getStatistics(String predicateName) {
		StageStatisticsHeader ssh = null;
		if (this.statistics.containsKey(predicateName))
			ssh = this.statistics.get(predicateName);

		if (ssh == null)
			throw new InterpreterException("Attempting to gather stage statistics without initialization");
		
		return ssh;
	}
	
	// should only be called by fs aggregates
	// fs aggregates are named fs_aggregate_[clique's predicate name]
	public void addProvenance(String predicateName, Tuple tuple, List<Tuple> provenance) {
		String cliquePredicateName = predicateName.substring(AggregateRewriter.FS_AGGREGATE_NODE_NAME_PREFIX.length());
		cliquePredicateName = cliquePredicateName.substring(0, cliquePredicateName.lastIndexOf("_"));
		StageStatisticsHeader ssh = this.getStatistics(cliquePredicateName);
		ssh.addProvenance(tuple, provenance);
	}
	
	public String extractPredicateName(String predicateName) {		
		return predicateName.substring(0, predicateName.lastIndexOf("_"));
	}
	
	public static boolean isValidPredicate(String predicateName) {
		return (!(predicateName.startsWith(Rewriter.MAGIC_PREDICATE_NAME_PREFIX)
				|| predicateName.startsWith(Rewriter.GENERAL_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)
				|| predicateName.startsWith(Rewriter.LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)
				|| predicateName.startsWith(Rewriter.NON_LINEAR_SUPPLEMENTARY_MAGIC_PREDICATE_NAME_PREFIX)));			
	}
	
}
