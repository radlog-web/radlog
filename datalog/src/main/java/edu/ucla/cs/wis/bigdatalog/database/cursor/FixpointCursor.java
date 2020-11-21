package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public interface FixpointCursor {
	public boolean isFixedPointReached();
	
	public void beginNextStage(DeALSContext deALSContext, int stageId);
	
	public void initialize();
	
	//public boolean isInCurrentStage(int address);
	
	public int getIterationSize();
}
