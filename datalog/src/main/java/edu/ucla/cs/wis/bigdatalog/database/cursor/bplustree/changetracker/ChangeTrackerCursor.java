package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public abstract class ChangeTrackerCursor<C extends ChangeTrackingStore<?>> 
	extends Cursor 
	implements FixpointCursor {	
	protected C							store;
	protected ChangeTracker				deltaSKeys;
	protected int						keyIndex;	
	protected int						stageId;

	@SuppressWarnings("unchecked")
	public ChangeTrackerCursor(Relation<?> relation) {
		super(relation);

		if (relation.getTupleStore() instanceof TupleAggregationStore)
			this.store = (C) ((TupleAggregationStore)relation.getTupleStore()).storageStructure;
		else if (relation.getTupleStore() instanceof TupleUnorderedHeapStore)
			this.store = (C) ((TupleUnorderedHeapStore)relation.getTupleStore());
		else if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore)
			this.store = (C) ((TupleBPlusTreeUniqueStore)relation.getTupleStore()).storageStructure;
		
		this.stageId = 0;
		this.initialize();
	}
		
	public void initializeTracking(DeALSConfiguration deALSConfiguration) {
		this.store.initializeTracking(deALSConfiguration);
	}
	
	public void initializeWithKeys() {
		this.deltaSKeys = this.store.getModifiedKeys(0);
		this.initialize();
	}
	
	public void initialize() {
		this.keyIndex = 0;
	}
	
	public void reset() {
		//this.initialize();
		this.keyIndex = 0;
	}

	public C getStore() { return this.store; }
	
	abstract public int getTuple(Tuple tuple);
		
	@Override
	public boolean isFixedPointReached() { 
		return (this.store.getNumberOfModifiedKeys() == 0);
	}

	@Override
	public void beginNextStage(DeALSContext deALSContext, int stageId) { 
		// get new bplus tree
		this.stageId = stageId;
		this.deltaSKeys = this.store.getModifiedKeys(this.stageId - 1);
		this.store.initializeTrackingForNextStageModifiedKeys(this.stageId, deALSContext.getConfiguration());
		this.initialize();
	}
	
	public ChangeTracker getDeltaSKeys() { return this.deltaSKeys; }
		
	public ChangeTracker getDeltaSKeys(int stageId) { return this.store.getModifiedKeys(stageId); }
	
	@Override
	public void moveNext() { 
		this.keyIndex++; 
	}
	
	@Override
	public int getIterationSize() { 
		if (this.deltaSKeys == null)
			return 0;
		return this.deltaSKeys.getNumberOfEntries();
	}
	
	public int getSizeOfDelta() {
		if (this.store.getModifiedKeys(this.stageId-1) == null)
			return 0;
		return this.store.getModifiedKeys(this.stageId-1).getNumberOfEntries();
	}
}