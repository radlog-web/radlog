package edu.ucla.cs.wis.bigdatalog.database.store.changetracking;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;

public interface ChangeTrackingStore<T> {
	public int getNumberOfModifiedKeys();
	
	public ChangeTracker getModifiedKeys(int stageId);
	
	public void initializeTracking(DeALSConfiguration deALSConfiguration);
	
	public void initializeTrackingForNextStageModifiedKeys(int stageId, DeALSConfiguration deALSConfiguration);
	
	public int getTuple(T key, Tuple tuple);
}
