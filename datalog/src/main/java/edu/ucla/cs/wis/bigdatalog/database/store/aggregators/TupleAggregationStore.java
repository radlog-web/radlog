package edu.ucla.cs.wis.bigdatalog.database.store.aggregators;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.AggregatorBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues.AggregatorBPlusTreeByteKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues.AggregatorBPlusTreeByteKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues.AggregatorBPlusTreeIntKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypevalues.AggregatorBPlusTreeIntKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysfloatvalues.AggregatorBPlusTreeIntKeysFloatValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypedbkeyvalues.AggregatorBPlusTreeLongKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypevalues.AggregatorBPlusTreeLongKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysfloatvalues.AggregatorBPlusTreeLongKeysFloatValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues.AggregatorBPlusTreeLongKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleAggregationStore 
	extends BPlusTreeTupleStore 
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public		TupleAggregationStoreStructure	storageStructure;
	protected 	AggregateInfo[]					aggregateInfos;

	public TupleAggregationStore() { super(); }
	
	public TupleAggregationStore(String relationName, DataType[] schema, TupleStoreConfiguration configuration, int nodeSize, 
			TypeManager typeManager) {
		super(relationName, schema, configuration.keyColumns, nodeSize, typeManager);
		this.aggregateInfos = configuration.aggregateInfos;
		
		this.initialize();
	}
	
	protected void initialize() {
		super.initialize();
		
		DataType aggregateColumnType = this.schema[this.schema.length - 1];
		//int nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.store.aggregators.bplustree.nodesize"));
		this.initializeBPlusTree(this.nodeSize, aggregateColumnType);
		//this initializeBPlusTreeForest(nodeSize, aggregateColumnType);
		
		if (this.storageStructure == null)
			throw new DatabaseException("No Storage Structure Found for TupleAggregationStore");
	}
	
	private void initializeBPlusTree(int nodeSize, DataType aggregateColumnType) {		
		if (this.aggregateInfos[0].aggregateType == AggregateFunctionType.FSCNT 
				|| this.aggregateInfos[0].aggregateType == AggregateFunctionType.FSSUM) {
			if (this.bytesPerKey == 4) {
				this.storageStructure = new AggregatorBPlusTreeIntKeysDbTypeDbKeyValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);				
			} else if (this.bytesPerKey == 8) {
				this.storageStructure = new AggregatorBPlusTreeLongKeysDbTypeDbKeyValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
			} else {
				this.storageStructure = new AggregatorBPlusTreeByteKeysDbTypeDbKeyValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
			}
		} else if (this.aggregateInfos[0].aggregateType == AggregateFunctionType.COUNT
				|| this.aggregateInfos[0].aggregateType == AggregateFunctionType.COUNT_DISTINCT
				|| this.aggregateInfos[0].aggregateType == AggregateFunctionType.AVG) {
			// count is special in that the datatype being counted does not have to be the result datatype
			if (this.bytesPerKey == 4) {
				this.storageStructure = new AggregatorBPlusTreeIntKeysDbTypeValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
			} else if (this.bytesPerKey == 8) {
				this.storageStructure = new AggregatorBPlusTreeLongKeysDbTypeValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
			} else {
				this.storageStructure = new AggregatorBPlusTreeByteKeysDbTypeValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
			}
		} else {		
			if (this.bytesPerKey == 4) {
				if (aggregateColumnType == DataType.INT) {
					this.storageStructure = new AggregatorBPlusTreeIntKeysIntValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
				} else if (aggregateColumnType == DataType.DOUBLE) {
					this.storageStructure = new AggregatorBPlusTreeIntKeysFloatValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
				} else {
					this.storageStructure = new AggregatorBPlusTreeIntKeysDbTypeValues(nodeSize, this.keyColumns, 
							this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
				}					
			} else if (this.bytesPerKey == 8) {
				if (aggregateColumnType == DataType.INT) {
					this.storageStructure = new AggregatorBPlusTreeLongKeysIntValues(nodeSize, this.keyColumns, 
							this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
				} else if (aggregateColumnType == DataType.DOUBLE) {
					this.storageStructure = new AggregatorBPlusTreeLongKeysFloatValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
				} else {
					this.storageStructure = new AggregatorBPlusTreeLongKeysDbTypeValues(nodeSize, this.keyColumns, 
							this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
				}
			} else { 
				this.storageStructure = new AggregatorBPlusTreeByteKeysDbTypeValues(nodeSize, this.keyColumns, 
						this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos, this.typeManager);
			}
		}
	}

	/*
	private void initializeBPlusTreeForest(int nodeSize, DataType aggregateColumnType) {
		this.storageStructure = new AggregatorBPlusTreeForestLongKeysIntValues(nodeSize, this.keyColumns, 
				this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, this.aggregateInfos);
	}*/
	
	@Override
	public void add(Tuple tuple) {
		this.storageStructure.insert(tuple, new AggregatorResult());
	}
	
	public void put(Tuple tuple) {
		this.storageStructure.insert(tuple, new AggregatorResult());
	}
	
	public void put(Tuple tuple, AggregatorResult result) {
		this.storageStructure.insert(tuple, result);
	}
	
	@Override
	public void update(Tuple tuple) {
		this.storageStructure.delete(tuple);		
		this.add(tuple);
	}
	
	@Override
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		return this.storageStructure.getTuple(keyColumns, tuple);
	}
	
	@Override
	public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor) {
		RangeSearchResult result = new RangeSearchResult();
		int bytesPerKey = ((RangeSearchableStorageStructure<?>)this.storageStructure).getBytesPerKey();
		switch (bytesPerKey) { 
			case 4:
				((RangeSearchableStorageStructure<Integer>)this.storageStructure).getTuple((Integer)searchKeys.startKey, (Integer)searchKeys.endKey, result);
				break;
			case 8:
				((RangeSearchableStorageStructure<Long>)this.storageStructure).getTuple((Long)searchKeys.startKey, (Long)searchKeys.endKey, result);
				break;				
			default:
				((RangeSearchableStorageStructure<byte[]>)this.storageStructure).getTuple((byte[])searchKeys.startKey, (byte[])searchKeys.endKey, result);
		}
		
		if (result.success) {
			cursor.initialize(result.leaf, result.index, searchKeys);
			return 1;
		}
		
		return 0;
	}

	@Override
	public void remove(Tuple tuple) {
		this.storageStructure.delete(tuple);
	}

	@Override
	public void removeAll() {
		this.storageStructure.deleteAll();
	}

	@Override
	public int commit() { return 0; }

	@Override
	public int getNumberOfTuples() {
		return this.storageStructure.getNumberOfEntries();
	}

	@Override
	public String toString() {		
		if (this.getNumberOfTuples() == 0)
			return "Relation is empty";
		
		StringBuilder output = new StringBuilder();
		output.append("[" + this.storageStructure.getClass().getSimpleName() + "]");
		output.append("bytesPerKey | bytesPerValue : [" + this.bytesPerKey + " | " + this.bytesPerValue + "]");		
		output.append("key columns : " + Arrays.toString(this.keyColumns));
		output.append(this.storageStructure.toString());
		return output.toString();
	}
	
	public BPlusTreeLeaf<?> getFirstChild() {
		if (this.storageStructure instanceof AggregatorBPlusTreeStoreStructure)
			return ((AggregatorBPlusTreeStoreStructure<?,?,?>)this.storageStructure).getFirstChild();
		return null;
	}
	
	public MemoryMeasurement getSizeOf() {
		return this.storageStructure.getSizeOf();
	}
	
	public int getHeight() {
		if (this.storageStructure instanceof AggregatorBPlusTreeStoreStructure)
			return ((AggregatorBPlusTreeStoreStructure<?,?,?>)this.storageStructure).getHeight();
		return 1;
	}
	
	public int getNodeSize() {
		if (this.storageStructure instanceof AggregatorBPlusTreeStoreStructure)
			return ((AggregatorBPlusTreeStoreStructure<?,?,?>)this.storageStructure).getNodeSize();
		return 0;
	}
		
	// TODO - write a sorting function so the entries in each bucket are sorted, if using an unordered list in bucket
	public boolean sort() { return false; }
}
