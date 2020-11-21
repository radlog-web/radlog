package edu.ucla.cs.wis.bigdatalog.database.cursor;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class SemiNaiveCursor extends NaiveCursor implements FixpointCursor {	
	private static Logger logger = LoggerFactory.getLogger(SemiNaiveCursor.class.getName());
	
	protected SemiNaiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) { 
		super(relation, filteredColumns);
	}
	
	@Override  
	public void beginNextStage(DeALSContext deALSContext, int stageId) {	
		if (DEBUG) {
			deALSContext.logTrace(logger, "Entering beginNextPhase");
			deALSContext.logDebug(logger, "{}", this);	
			if (deALSContext.isDerivationTrackingEnabled())
				deALSContext.logDerivationTracking(logger, "SemiNaive cursor for {} entering next phase.", this.relation.getName());
		}
		
		// The first time around
		if (this.baseTupleAddress == -1) {
			this.setFirstStage();
		} else {
			this.baseTupleAddress = this.endTupleAddress;
			this.endTupleAddress = this.tupleStore.getLastTupleAddress();
		}
		
		if (DEBUG) {
			deALSContext.logDebug(logger, "{}", this);
			deALSContext.logTrace(logger, "Exiting beginNextPhase");
		}
	}
}
