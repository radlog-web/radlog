package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.forests;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTree;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues.AggregatorBPlusTreeLongKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AggregatorBPlusTreeForestLongKeysIntValues 
	implements BPlusTree, TupleAggregationStoreStructure, ChangeTrackingStore<Long>, RangeSearchableStorageStructure<Long> {
	protected final int size = 256;
	
	public AggregatorBPlusTreeLongKeysIntValues[] forest;
	protected final int nodeSize;
	protected final int bytesPerKey;
	protected final int[] keyColumns;
	protected final DataType[] keyColumnTypes;
	protected final int[] valueColumns;
	protected final DataType[] valueColumnTypes;
	protected AggregateInfo[] aggregateInfos;
	protected Map<Integer, ChangeTracker> modifiedKeysTrees;
	protected ChangeTracker modifiedKeysTree;
	protected int numberOfTrees;
	protected DeALSConfiguration 	deALSConfiguration;
	protected TypeManager 			typeManager;
	
	public AggregatorBPlusTreeForestLongKeysIntValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, AggregateInfo[] aggregateInfos, 
			DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		this.forest = new AggregatorBPlusTreeLongKeysIntValues[this.size];
		this.nodeSize = nodeSize;
		this.bytesPerKey = 8;
		this.keyColumns = keyColumns;
		this.keyColumnTypes = keyColumnTypes;
		this.valueColumns = valueColumns;
		this.valueColumnTypes = valueColumnTypes;
		this.aggregateInfos = aggregateInfos;
		this.modifiedKeysTrees = new HashMap<>();
		this.deALSConfiguration = deALSConfiguration;
		this.typeManager = typeManager;
	}

	public int getNodeSize() { return this.nodeSize; }
	
	@Override
	public int getBytesPerKey() { return this.bytesPerKey; }
	
	@Override
	public int[] getKeyColumns() { return this.keyColumns; }

	@Override
	public DataType[] getKeyColumnTypes() { return this.keyColumnTypes; }

	public int[] getValueColumns() { return this.valueColumns; }
	
	public DataType[] getValueColumnTypes() { return this.keyColumnTypes; }
	
	public boolean isEmpty() { return (this.numberOfTrees == 0); }
		
	@Override
	public int getNumberOfEntries() {
		int numberOfEntries = 0;
		for (int i = 0; i < this.forest.length; i++) {
			if (this.forest[i] != null)
				numberOfEntries += this.forest[i].getNumberOfEntries();
		}

		return numberOfEntries;
	}

	@Override
	public void insert(Tuple tuple, AggregatorResult result) {
		long key = this.getKey(tuple.columns);
		int index = Math.abs((int)((key >> 32) % size));
		
		AggregatorBPlusTreeLongKeysIntValues tree = this.forest[index]; 		
		if (tree == null) {	
			 tree = new AggregatorBPlusTreeLongKeysIntValues(this.nodeSize, this.keyColumns, this.keyColumnTypes, 
					this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
			 tree.setModifiedKeysTree(this.modifiedKeysTree);
			 this.forest[index] = tree;
			 this.numberOfTrees++;			
		}
		tree.insert(tuple, result);		
	}

	@Override
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		long key = this.getKey(keyColumns);
		int index = Math.abs((int)((key >> 32) % size));
		return this.forest[index].getTuple(key, tuple);
	}

	@Override
	public int getTuple(Long key, Tuple tuple) { 
		int index = Math.abs((int)((key >> 32) % size));
		return this.forest[index].getTuple(key, tuple);
	}

	@Override
	public void getTuple(Long startKey, Long endKey, RangeSearchResult result) {
	
	}
	
	@Override
	public boolean delete(Tuple tuple) {
		long key = this.getKey(tuple.columns);
		int index = Math.abs((int)((key >> 32) % size));
		return this.forest[index].delete(key);
	}

	@Override
	public void deleteAll() {
		if (this.forest != null) {
			for (int i = 0; i < this.forest.length; i++)
				if (this.forest[i] != null)
					this.forest[i].deleteAll();
		}
		this.forest = null;
		
		if (this.modifiedKeysTrees != null) {
			for (Map.Entry<Integer, ChangeTracker> tree : this.modifiedKeysTrees.entrySet())
				tree.getValue().delete();

			this.modifiedKeysTrees.clear();
		}	
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		for (int i = 0; i < this.forest.length; i++) {
			MemoryMeasurement item = new MemoryMeasurement();
			item = this.forest[i].getSizeOf();
			used += item.getUsed();
			allocated += item.getAllocated();			
		}
			
		return 	new MemoryMeasurement(used, allocated);
	}

	@Override
	public int getNumberOfModifiedKeys() {	
		return this.modifiedKeysTree.getNumberOfEntries(); 
	}	
	
	@Override
	public ChangeTracker getModifiedKeys(int stageId) {
		return this.modifiedKeysTrees.get(stageId);
	}

	@Override
	public void initializeTracking(DeALSConfiguration deALSConfiguration) {
		this.initializeTrackingForNextStageModifiedKeys(1, deALSConfiguration);
	}
	
	@Override
	public void initializeTrackingForNextStageModifiedKeys(int stageId, DeALSConfiguration deALSConfiguration) {
		if (this.modifiedKeysTrees.containsKey(stageId))
			return;
		
		this.modifiedKeysTree = ChangeTracker.getChangeTracker(this.keyColumns, this.keyColumnTypes, deALSConfiguration, this.typeManager);
		this.modifiedKeysTrees.put(stageId, this.modifiedKeysTree);
		
		// trees 2 stages back are no longer needed
		if (this.modifiedKeysTrees.containsKey(stageId - 2)) {
			ChangeTracker treeToDelete = this.modifiedKeysTrees.remove(stageId - 2);
			treeToDelete.delete();
		}
		
		for (int i = 0; i < this.forest.length; i++) {
			if (this.forest[i] != null)
				this.forest[i].setModifiedKeysTree(this.modifiedKeysTree);
		}
		
	}

	private long getKey(DbTypeBase[] columns) {
		if (this.keyColumns.length == 2) {
			int keyPart1, keyPart2;
			if (this.keyColumnTypes[0] == DataType.INT)
				keyPart1 = ((DbInteger)columns[this.keyColumns[0]]).getValue();
			else
				keyPart1 = ((DbString)columns[this.keyColumns[0]]).getKey();
			
			if (this.keyColumnTypes[1] == DataType.INT)
				keyPart2 = ((DbInteger)columns[this.keyColumns[1]]).getValue();
			else
				keyPart2 = ((DbString)columns[this.keyColumns[1]]).getKey();
			
			return (((long)keyPart1) << 32) | (keyPart2 & 0xffffffffL);
		}
		
		return ((DbLong)columns[this.keyColumns[0]]).getValue();		
	}

	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		if ((this.forest == null) || (this.numberOfTrees == 0)) {
			output.append("empty");
		} else {
			for (int i = 0; i < this.forest.length; i++) {
				if (this.forest[i] != null) {
					if (output.length() > 0)
						output.append("\n");
					output.append(this.forest[i].toStringShort());
				}
			}
		}

		return output.toString();
	}

	@Override
	public BPlusTreeLeaf<?> getFirstChild() {
		if ((this.forest != null) && (this.numberOfTrees > 0)) {
			for (int i = 0; i < this.forest.length; i++)
				if (this.forest[i] != null)
					return this.forest[i].getFirstChild();
		}
		
		return null;		
	}
	
	//so FSCiqueNode5 can add trees to the forest
	public void setForest(Collection<AggregateRelation> relations) {
		this.forest = new AggregatorBPlusTreeLongKeysIntValues[relations.size()];
		this.numberOfTrees = 0;
		for (AggregateRelation relation : relations)
			this.forest[this.numberOfTrees++] = (AggregatorBPlusTreeLongKeysIntValues) ((TupleAggregationStore)relation.getTupleStore()).storageStructure;
		
	}

}
