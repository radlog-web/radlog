package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class BPlusTreeStoreValueStructure<P extends BPlusTreeElement, L extends BPlusTreeLeaf<L>, N extends BPlusTreeNode<P, L>> 
	extends BPlusTreeStoreStructure<P, L, N> implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int 							bytesPerValue;
	protected int[] 						valueColumns;
	protected DataType[] 					valueColumnTypes;
	protected Map<Integer, ChangeTracker> 	changeTrackersByStage;
	protected ChangeTracker 				changeTracker;
	protected boolean						trackModifiedTuples;
	protected TypeManager					typeManager;
	
	public BPlusTreeStoreValueStructure() { super(); }
	
	public BPlusTreeStoreValueStructure(int nodeSize, int bytesPerKey, int bytesPerValue, 
			int[] keyColumns, DataType[] keyColumnTypes, int valueColumns[], DataType[] valueColumnTypes, 
			TypeManager typeManager) {
		super(nodeSize, bytesPerKey, keyColumns, keyColumnTypes);
		
		this.bytesPerValue = bytesPerValue;
		this.valueColumns = valueColumns;
		this.valueColumnTypes = valueColumnTypes;
		this.typeManager = typeManager;
	}
	
	public void initialize() {
		super.initialize();
		if (this.trackModifiedTuples)
			this.changeTrackersByStage = new HashMap<>();
	}
			
	public int getBytesPerValue() { return this.bytesPerValue; }
	
	protected byte[] getBytes(DbTypeBase[] columns) {		
		byte[] bytes = new byte[this.bytesPerValue];
		int offset = 0;
		int columnIndex;
		for (int i = 0; i < this.valueColumns.length; i++) {
			columnIndex = this.valueColumns[i]; 
			if (columns[columnIndex].getDataType() == this.valueColumnTypes[i]) {
				offset = columns[columnIndex].getBytes(bytes, offset);
			} else {
				columns[columnIndex] = ((DbNumericType)columns[columnIndex]).convertTo(this.valueColumnTypes[i]);
				offset = columns[columnIndex].getBytes(bytes, offset);
			}
		}
		
		return bytes;
	}
	
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
		this.trackModifiedTuples = true;
		
		if (this.changeTrackersByStage == null)
			this.changeTrackersByStage = new HashMap<>();

		this.initializeTrackingForNextStageModifiedKeys(0, deALSConfiguration);		
	}
	
	public ChangeTracker getModifiedKeys(int stageId) {
		return this.changeTrackersByStage.get(stageId);
	}

	public void initializeTrackingForNextStageModifiedKeys(int stageId, DeALSConfiguration deALSConfiguration) {
		if (this.changeTrackersByStage.containsKey(stageId))
			return;
		
		this.changeTracker = ChangeTracker.getChangeTracker(this.keyColumns, this.keyColumnTypes, deALSConfiguration, this.typeManager);
		this.changeTrackersByStage.put(stageId, this.changeTracker);
		
		// trees 2 stages back are no longer needed
		if (this.changeTrackersByStage.containsKey(stageId - 2)) {
			ChangeTracker treeToDelete = this.changeTrackersByStage.remove(stageId - 2);
			treeToDelete.delete();
		}
	}
}
