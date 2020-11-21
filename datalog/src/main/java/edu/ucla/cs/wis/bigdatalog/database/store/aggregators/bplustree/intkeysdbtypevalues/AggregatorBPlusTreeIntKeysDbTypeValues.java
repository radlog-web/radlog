package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.AggregatorBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes.DbTypeAggregatorHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class AggregatorBPlusTreeIntKeysDbTypeValues 
	extends AggregatorBPlusTreeStoreStructure<AggregatorBPlusTreeIntKeysDbTypeValuesPage, 
		AggregatorBPlusTreeIntKeysDbTypeValuesLeaf, AggregatorBPlusTreeIntKeysDbTypeValuesNode> 
	implements ChangeTrackingStore<Integer>, RangeSearchableStorageStructure<Integer>, Serializable {
	private static final long serialVersionUID = 1L;

	protected AggregatorBPlusTreeIntKeysDbTypeValuesInsertResult insertResult;
	protected AggregatorBPlusTreeIntKeysDbTypeValuesGetResult getResult;
	protected DbTypeAggregatorHelper aggregator;
	protected AggregatorBPlusTreeIntKeysDbTypeValuesGetResultRange getResultRange;
	
	public AggregatorBPlusTreeIntKeysDbTypeValues() { super(); }
	
	public AggregatorBPlusTreeIntKeysDbTypeValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		super(nodeSize, 4, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, aggregateInfos, typeManager);
		
		this.insertResult = new AggregatorBPlusTreeIntKeysDbTypeValuesInsertResult();
		this.getResult = new AggregatorBPlusTreeIntKeysDbTypeValuesGetResult();
		this.getResultRange = new AggregatorBPlusTreeIntKeysDbTypeValuesGetResultRange();
		this.aggregator = DbTypeAggregatorHelper.getAggregatorHelper(aggregateInfos[0].aggregateType, typeManager);
		this.initialize();
	}

	@Override
	protected AggregatorBPlusTreeIntKeysDbTypeValuesPage allocatePage() {
		if (this.rootNode == null)
			return new AggregatorBPlusTreeIntKeysDbTypeValuesLeaf(this.nodeSize, this.aggregator);
		
		return new AggregatorBPlusTreeIntKeysDbTypeValuesNode(this.nodeSize, this.aggregator);
	}
	
	public void insert(Tuple tuple, AggregatorResult result) {
		int key = this.getKeyI(tuple.columns); 
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
		AggregatorBPlusTreeIntKeysDbTypeValuesNode newRoot = (AggregatorBPlusTreeIntKeysDbTypeValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = insertResult.newPage;
		newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
					
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		this.rootNode.get(this.getKeyI(keyColumns), this.getResult);
		if (this.getResult.success) {
			tuple.columns[this.keyColumns[0]] = keyColumns[0];	
			if (this.valueColumnTypes[0].usesSecondaryStorage())
				tuple.columns[this.valueColumns[0]] = DbTypeBase.loadFrom(this.valueColumnTypes[0], 
						((DbInteger)this.getResult.value).getValue(), this.typeManager);
			else
				tuple.columns[this.valueColumns[0]] = this.getResult.value;
			//tuple.columns[this.valueColumns[0]] = this.getResult.value;
			return 1;
		}
		return 0;
	}
	
	@Override
	public int getTuple(Integer key, Tuple tuple) {
		this.rootNode.get(key, this.getResult);
		if (this.getResult.success)
			return this.loadTuple(key, this.getResult.value, tuple);
		return 0;
	}

	@Override
	public void getTuple(Integer startKey, Integer endKey, RangeSearchResult result) {
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
	
	public int loadTuple(int key, DbTypeBase value, Tuple tuple) {
		/*if (this.keyColumnTypes[0] == DataType.INT)
			tuple.columns[this.keyColumns[0]] = DbInteger.create(key);
		else
			tuple.columns[this.keyColumns[0]] = DbString.load(key, this.typeManager);*/
		tuple.columns[this.keyColumns[0]] = DbTypeBase.loadFrom(this.keyColumnTypes[0], key, this.typeManager);
		
		if (this.valueColumnTypes[0].usesSecondaryStorage())
			tuple.columns[this.valueColumns[0]] = DbTypeBase.loadFrom(this.valueColumnTypes[0], 
					((DbInteger)value).getValue(), this.typeManager);
		else
			tuple.columns[this.valueColumns[0]] = value;
		return 1;
	}

	@Override
	public boolean delete(Tuple tuple) {		
		boolean status = this.rootNode.delete(this.getKeyI(tuple.columns));
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
