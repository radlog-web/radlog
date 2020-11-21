package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.AggregatorBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes.DbTypeAggregatorHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class AggregatorBPlusTreeLongKeysDbTypeValues 
	extends AggregatorBPlusTreeStoreStructure<AggregatorBPlusTreeLongKeysDbTypeValuesPage, 
		AggregatorBPlusTreeLongKeysDbTypeValuesLeaf, AggregatorBPlusTreeLongKeysDbTypeValuesNode> 
	implements ChangeTrackingStore<Long>, RangeSearchableStorageStructure<Long>, Serializable {
	private static final long serialVersionUID = 1L;

	protected AggregatorBPlusTreeLongKeysDbTypeValuesInsertResult insertResult;
	protected AggregatorBPlusTreeLongKeysDbTypeValuesGetResult getResult;
	protected DbTypeAggregatorHelper aggregator;
	protected AggregatorBPlusTreeLongKeysDbTypeValuesGetResultRange getResultRange;
	
	public AggregatorBPlusTreeLongKeysDbTypeValues() { super(); }
	
	public AggregatorBPlusTreeLongKeysDbTypeValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		super(nodeSize, 8, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, aggregateInfos, typeManager);
		
		this.insertResult = new AggregatorBPlusTreeLongKeysDbTypeValuesInsertResult();
		this.getResult = new AggregatorBPlusTreeLongKeysDbTypeValuesGetResult();
		this.getResultRange = new AggregatorBPlusTreeLongKeysDbTypeValuesGetResultRange();
		this.aggregator = DbTypeAggregatorHelper.getAggregatorHelper(aggregateInfos[0].aggregateType, typeManager);
		this.initialize();
	}

	@Override
	protected AggregatorBPlusTreeLongKeysDbTypeValuesPage allocatePage() {
		if (this.rootNode == null)
			return new AggregatorBPlusTreeLongKeysDbTypeValuesLeaf(this.nodeSize, this.aggregator);
		
		return new AggregatorBPlusTreeLongKeysDbTypeValuesNode(this.nodeSize, this.aggregator);
	}
	
	public void insert(Tuple tuple, AggregatorResult result) {
		long key = this.getKeyL(tuple.columns); 
		DbTypeBase value = tuple.columns[this.valueColumns[0]];
		
		if ((this.valueColumnTypes[0] != value.getDataType()) 
				&& (this.valueColumnTypes[0].getOrder() > -1) 
				&& (value.getDataType().getOrder() > -1))
			tuple.columns[this.valueColumns[0]] = ((DbNumericType)value).convertTo(this.valueColumnTypes[0]);
		
		// case 1 - room to insert in root node 
		this.rootNode.insert(key, tuple.columns[this.valueColumns[0]], this.insertResult);
				
		result.status = this.insertResult.status;
		
		if (this.insertResult.status == AggregatorInsertStatus.NEW) {
			this.numberOfEntries++;
			if (this.changeTracker != null)
				this.changeTracker.add(key);
		} else if (this.insertResult.status == AggregatorInsertStatus.UPDATE) {
			if (this.changeTracker != null)
				this.changeTracker.add(key);
		}

		if (insertResult.newPage == null)
			return;

		// case 2 we have grow the tree as the root node was split 
		AggregatorBPlusTreeLongKeysDbTypeValuesNode newRoot = (AggregatorBPlusTreeLongKeysDbTypeValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = insertResult.newPage;//retval.getFirst();
		newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
					
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		this.rootNode.get(this.getKeyL(keyColumns), this.getResult);
		
		if (this.getResult.success) {
			if (this.keyColumns.length == 1) {
				tuple.columns[this.keyColumns[0]] = keyColumns[0];
			} else {
				tuple.columns[this.keyColumns[0]] = keyColumns[0];
				tuple.columns[this.keyColumns[1]] = keyColumns[1];
			}
			
			if (this.valueColumnTypes[0].usesSecondaryStorage())
				tuple.columns[this.valueColumns[0]] = DbTypeBase.loadFrom(this.valueColumnTypes[0], 
						((DbInteger)this.getResult.value).getValue(), this.typeManager);
			else
				tuple.columns[this.valueColumns[0]] = this.getResult.value;

			return 1;
		}
		return 0;
	}
	
	@Override
	public int getTuple(Long key, Tuple tuple) {
		this.rootNode.get(key, this.getResult);
		if (this.getResult.success)	
			return this.loadTuple(key, this.getResult.value, tuple);
		
		return 0;
	}
	
	@Override
	public void getTuple(Long startKey, Long endKey, RangeSearchResult result) {
		this.rootNode.get(startKey, endKey, this.getResultRange);
		
		result.success = this.getResultRange.success;
		if (this.getResultRange.success) {
			result.index = this.getResultRange.index;
			result.leaf = this.getResultRange.leaf;
			result.success = true;
			return;
		}
		result.success = false;		
	}	
	
	public int loadTuple(long key, DbTypeBase value, Tuple tuple) {
		// one key, assume DbLong
		if (this.keyColumns.length == 1) {	
			if (this.keyColumnTypes[0] == DataType.LONG)
				tuple.columns[this.keyColumns[0]] = DbLong.create(key);
			else
				tuple.columns[this.keyColumns[0]] = DbDouble.create(Double.longBitsToDouble(key));
		} else {
			if (this.keyColumnTypes[0] == DataType.INT)
				tuple.columns[this.keyColumns[0]] = DbInteger.create((int)(key >> 32));
			else
				tuple.columns[this.keyColumns[0]] = DbString.load((int)(key >> 32), this.typeManager);
			
			if (this.keyColumnTypes[1] == DataType.INT)
				tuple.columns[this.keyColumns[1]] = DbInteger.create((int)key);
			else
				tuple.columns[this.keyColumns[1]] = DbString.load((int)key, this.typeManager);
		}
		
		if (this.valueColumnTypes[0].usesSecondaryStorage())
			tuple.columns[this.valueColumns[0]] = DbTypeBase.loadFrom(this.valueColumnTypes[0], 
					((DbInteger)value).getValue(), this.typeManager);
		else
			tuple.columns[this.valueColumns[0]] = value;
						
		return 1;
	}
		
	@Override
	public boolean delete(Tuple tuple) {
		boolean status = this.rootNode.delete(this.getKeyL(tuple.columns));
		if (status) {
			this.numberOfEntries--;
			if (this.numberOfEntries == 0) {
				this.rootNode = null;
				this.rootNode = this.allocatePage();
			}
		}
		return status;
	}	
}
