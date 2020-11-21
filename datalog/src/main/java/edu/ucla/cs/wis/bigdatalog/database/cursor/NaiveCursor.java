package edu.ucla.cs.wis.bigdatalog.database.cursor;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class NaiveCursor extends RecursiveCursor {
	private static Logger logger = LoggerFactory.getLogger(NaiveCursor.class.getName());

	protected NaiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) {
		super(relation, filteredColumns);
	}

	@Override
	public void beginNextStage(DeALSContext deALSContext, int stageId) {
		if (DEBUG) {
			deALSContext.logTrace(logger, "Entering beginNextPhase");
			deALSContext.logDebug(logger, "{}", this);
		
			if (deALSContext.isDerivationTrackingEnabled())
				deALSContext.logDerivationTracking(logger, "Naive cursor for {} entering next phase.", this.relation.getName());
		}
		
		// The first time around
		if (this.baseTupleAddress == -1) {
			this.setFirstStage();
		} else { 
			this.endTupleAddress = this.tupleStore.getLastTupleAddress();
			//this.tuple = this.tupleStore.get(this.baseTupleAddress);
		}
		
		if (DEBUG) {
			deALSContext.logDebug(logger, "{}", this);
			deALSContext.logTrace(logger, "Exiting beginNextPhase");
		}
	}
	
	@Override
	public int getIterationSize() {
		if (this.endTupleAddress == -1 && this.baseTupleAddress == -1)
			return 0;
		
		if (this.baseTupleAddress == 0)
			return (this.endTupleAddress - this.baseTupleAddress) + 1;
		
		return (this.endTupleAddress - this.baseTupleAddress);
	}	
}
