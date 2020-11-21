package edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleUnorderedHeapStore 
	extends AddressedTupleStore 
	implements ChangeTrackingStore<Integer>, Serializable {

	private static final long serialVersionUID = 1L;
	
	protected int 								pageSize;
	protected DataType[] 						schema;
	protected TupleUnorderedHeap 				directory;
	protected boolean							trackModifiedTuples;
	protected Map<Integer, ChangeTracker> 		changeTrackersByStage;
	protected ChangeTracker 					changeTracker;
	protected DeALSConfiguration				deALSConfiguration;
	//protected TypeManager						typeManager;
	//protected int[] 							keyColumns;	// for changeTracking
	//protected DataType[] 						keyColumnTypes; // for changeTracking
	
	public TupleUnorderedHeapStore(){ super(); }
	
	public TupleUnorderedHeapStore(String relationName, DataType[] schema, TupleStoreConfiguration configuration, int pageSize,
			DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		super(relationName, typeManager);
		this.schema = schema;
		this.pageSize = pageSize;
		if (configuration != null) {
			this.trackModifiedTuples = configuration.trackModifiedTuples;
			//this.keyColumns = configuration.keyColumns;
		}
		this.deALSConfiguration = deALSConfiguration;
		this.initialize();
	}

	public TupleUnorderedHeap getDirectory() { return this.directory; }
	
	public int getArity() { return this.schema.length; }
	
	//public int[] getKeyColumns() { return this.keyColumns; }
	
	//public DataType[] getKeyColumnTypes() { return this.keyColumnTypes; }
	
	private void initialize() {
		this.directory = new TupleUnorderedHeap(this.pageSize, this.schema, this.typeManager);
		if (this.trackModifiedTuples) {
			// must set key column types before initializing change tracker
			/*this.keyColumnTypes = new DataType[this.keyColumns.length];
			for (int i = 0; i < this.keyColumns.length; i++)
				this.keyColumnTypes[i] = this.schema[this.keyColumns[i]];*/
			this.initializeTracking(this.deALSConfiguration);			
		}
	}
		
	@Override
	public void add(AddressedTuple tuple) {
		int address = this.directory.appendTuple(tuple);
		if (this.trackModifiedTuples)
			this.changeTracker.add(address);
	}
	
	@Override
	public void update(AddressedTuple tuple) {
		this.directory.updateTuple(tuple);
		if (this.trackModifiedTuples)
			this.changeTracker.add(tuple.address);
	}

	@Override
	public int get(int address, AddressedTuple tuple) {
		if (address < 0) 
			return 0;
		
		return this.directory.getTuple(address, tuple);
	}
	
	public TupleRowPageLeaf getPage(int address) {
		return this.directory.pages[this.directory.getPageId(address)];
	}
	
	public int getAddressInPage(int address) {
		return this.directory.getTupleId(address);
	}

	@Override
	public AddressedTuple exists(AddressedTuple tuple) {
		return this.directory.exists(tuple);
	}

	@Override
	public int getFirstTupleAddress() {
		return this.directory.getFirstTupleAddress();
	}

	@Override
	public int getLastTupleAddress() {
		return this.directory.getLastTupleAddress();
	}

	@Override
	public void remove(AddressedTuple tuple) {
		this.directory.deleteTuple(tuple);
	}
	
	@Override
	public void remove(int address) {
		if (address < 0)
			return;
		
		this.directory.deleteTuple(address);
	}

	@Override
	public void removeAll() {			
		this.directory.deleteAll();
		this.directory = null;
		this.initialize();
	}

	@Override
	public int commit() {
		return this.directory.commit();
	}
	
	@Override
	public int getNumberOfTuples() {
		return this.directory.getNumberOfTuples();
	}
	
	@Override
	public String toString() {		
		if (this.getFirstTupleAddress() == -1)
			return "Relation is empty";
		
		StringBuilder retval = new StringBuilder();
		AddressedTuple tuple = this.getEmptyTuple();
		for (int i = this.getFirstTupleAddress(); i <= this.getLastTupleAddress(); i++) {
			if (this.get(i, tuple) > 0) {
				if (retval.length() > 0)
					retval.append("\n");
				retval.append(tuple.toString(true));				
			}
		}	
		return retval.toString();
	}
	
	@Override
	public boolean sort() {
		this.directory.sort();
		return true;
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		return this.directory.getSizeOf();
	}

	@Override
	public AddressedTuple getEmptyTuple() {
		AddressedTuple tuple = new AddressedTuple(this.schema.length);
		for (int i = 0; i < this.schema.length; i++)
			tuple.columns[i] = DbTypeBase.loadFrom(this.schema[i], 0);
				
		return tuple;
	}

	@Override
	public int getNumberOfModifiedKeys() { return this.changeTracker.getNumberOfEntries(); }

	@Override
	public ChangeTracker getModifiedKeys(int stageId) {
		return this.changeTrackersByStage.get(stageId);
	}

	@Override
	public void initializeTracking(DeALSConfiguration deALSConfiguration) {
		this.trackModifiedTuples = true;
		
		if (this.changeTrackersByStage == null)
			this.changeTrackersByStage = new HashMap<>();

		this.initializeTrackingForNextStageModifiedKeys(0, deALSConfiguration);
	}

	@Override
	public void initializeTrackingForNextStageModifiedKeys(int stageId, DeALSConfiguration deALSConfiguration) {
		if (this.changeTrackersByStage.containsKey(stageId))
			return;
		
		this.changeTracker = ChangeTracker.getChangeTracker(new int[]{0}, new DataType[]{DataType.INT}, deALSConfiguration, this.typeManager);
		// TODO APS 2/25/15 - TUHS can use a key-ed changetracker, but since getTuple() is accessible by address, the keys cannot be used to retrieve the tuple
		// thus an index must be used, but ChangeTrackers are not configured to access secondary indexes, etc. much work to be done to get this to work.
		// At this time, there is no reason to upgrade TUHS to this functionality.  This means, TUHS will produce the same end results as B+Trees and Aggregators,
		// but will have different intermediateresults, and many more results too, and could look like a bug but it is not.  Using address, which is insertion order,
		// instead of the key, which is sorted in B+Trees, results in a different ordering of the edges to be joined, which is almost always less efficient.
		//this.changeTracker = ChangeTracker.getChangeTracker(this.keyColumns, this.keyColumnTypes, this.deALSConfiguration, this.typeManager);
		this.changeTrackersByStage.put(stageId, this.changeTracker);
		
		// trees 2 stages back are no longer needed
		if (this.changeTrackersByStage.containsKey(stageId - 2)) {
			ChangeTracker treeToDelete = this.changeTrackersByStage.remove(stageId - 2);
			treeToDelete.delete();
		}
	}

	public int getTuple(Integer key, Tuple tuple) {
		return this.get(key, (AddressedTuple) tuple);
	}
}
