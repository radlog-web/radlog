package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class AggregatorBPlusTreeStoreStructure<P extends BPlusTreeElement, 
		L extends BPlusTreeLeaf<L>, N extends BPlusTreeNode<P, L>> 
	extends BPlusTreeStoreStructure<P, L, N> 
	implements TupleAggregationStoreStructure, Serializable {
	private static final long serialVersionUID = 1L;

	protected AggregateInfo[] 				aggregateInfos;
	protected int[] 						valueColumns;
	protected DataType[] 					valueColumnTypes;
	protected Map<Integer, ChangeTracker> 	changeTrackersByStage;
	protected ChangeTracker 				changeTracker;
	protected TypeManager					typeManager;
	
	public AggregatorBPlusTreeStoreStructure() { super(); }
	
	public AggregatorBPlusTreeStoreStructure(int nodeSize, int bytesPerKey, int[] keyColumns, DataType[] keyColumnTypes,
			int[] valueColumns, DataType[] valueColumnTypes, AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		super(nodeSize, bytesPerKey, keyColumns, keyColumnTypes);
		this.valueColumns = valueColumns;
		this.valueColumnTypes = valueColumnTypes;
		this.aggregateInfos = aggregateInfos;
		this.changeTrackersByStage = new HashMap<>();
		this.typeManager = typeManager;
	}	
	
	public int[] getValueColumns() { return this.valueColumns; }
	
	public DataType[] getValueColumnTypes() { return this.keyColumnTypes; }
	
	@Override
	public void deleteAll() {
		super.deleteAll();
		if (this.changeTrackersByStage != null) {
			for (Map.Entry<Integer, ChangeTracker> tree : this.changeTrackersByStage.entrySet())
				tree.getValue().delete();

			this.changeTrackersByStage.clear();
		}		
	}
	
	public int getNumberOfModifiedKeys() { return this.changeTracker.getNumberOfEntries(); }
	
	public void initializeTracking(DeALSConfiguration deALSConfiguration) {	
		if (this.changeTrackersByStage == null)
			this.changeTrackersByStage = new HashMap<>();
		
		this.initializeTrackingForNextStageModifiedKeys(0, deALSConfiguration);
	}
	
	public ChangeTracker getModifiedKeys(int stageId) {
		return this.changeTrackersByStage.get(stageId);
	}
	
	public void setModifiedKeys(int stageId, ChangeTracker changeTracker) {
		this.changeTrackersByStage.put(stageId, changeTracker);
	}

	public void initializeTrackingForNextStageModifiedKeys(int stageId, DeALSConfiguration deALSConfiguration) {
		if (this.changeTrackersByStage.containsKey(stageId))
			return;
		
		this.changeTracker = ChangeTracker.getChangeTracker(this.keyColumns, this.keyColumnTypes, deALSConfiguration, this.typeManager);
		this.changeTrackersByStage.put(stageId, this.changeTracker);
		
		// trees 2 stages back are no longer needed
		/*if (this.changeTrackersByStage.containsKey(stageId - 2)) {
			ChangeTracker treeToDelete = this.changeTrackersByStage.remove(stageId - 2);
			treeToDelete.delete();
		}*/
	}
	
	abstract public void insert(Tuple tuple, AggregatorResult result);
	
	abstract public boolean delete(Tuple tuple);
	
	abstract public int getTuple(DbTypeBase[] key, Tuple tuple);
}
