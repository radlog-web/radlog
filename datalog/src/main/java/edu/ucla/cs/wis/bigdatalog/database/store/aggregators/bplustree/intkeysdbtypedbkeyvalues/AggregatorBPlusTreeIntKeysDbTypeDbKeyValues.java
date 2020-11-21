package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.AggregatorBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class AggregatorBPlusTreeIntKeysDbTypeDbKeyValues 
	extends AggregatorBPlusTreeStoreStructure<AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesPage, 
		AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf, AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesNode> 
	implements ChangeTrackingStore<Integer>, RangeSearchableStorageStructure<Integer>, Serializable {
	private static final long serialVersionUID = 1L;

	protected AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesInsertResult insertResult;
	protected AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResult getResult;
	protected AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResultRange getResultRange;
	protected TypeManager typeManager;
	
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValues() { super(); }
	
	public AggregatorBPlusTreeIntKeysDbTypeDbKeyValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		super(nodeSize, 4, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, aggregateInfos, typeManager);
		
		this.insertResult = new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesInsertResult();
		this.getResult = new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResult();
		this.getResultRange = new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesGetResultRange();
		this.typeManager = typeManager;
		this.initialize();
	}

	@Override
	protected AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesPage allocatePage() {
		if (this.rootNode == null)
			return new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesLeaf(this.nodeSize, this.valueColumnTypes[0], 
					this.valueColumnTypes[1], this.aggregateInfos[0].aggregateType);

		return new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesNode(this.nodeSize, this.valueColumnTypes[0], 
				this.valueColumnTypes[1], this.aggregateInfos[0].aggregateType);
	}
	
	@Override
	public void insert(Tuple tuple, AggregatorResult result) {
		int key = this.getKeyI(tuple.columns); 
						
		// case 1 - room to insert in root node
		this.rootNode.insert(key, 
				tuple.columns[this.valueColumns[0]], 
				(DbNumericType) tuple.columns[this.valueColumns[1]], 
				this.insertResult,
				this.typeManager);

		result.status = this.insertResult.status;
		result.newTotalValue = this.insertResult.newTotalResult;
		
		if (this.insertResult.status == AggregatorInsertStatus.NEW) {
			this.numberOfEntries++;
			if (this.changeTracker != null)
				this.changeTracker.add(key);
		} else if (this.insertResult.status == AggregatorInsertStatus.UPDATE) {
			if (this.changeTracker != null)
				this.changeTracker.add(key);
		}
		
		if (this.insertResult.newPage == null)
			return;

		// case 2 we have grow the tree as the root node was split 
		AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesNode newRoot = (AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();

		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		this.rootNode.get(this.getKeyI(keyColumns), this.getResult);
		if (this.getResult.success) {
			if (this.keyColumns.length == 1) {
				tuple.columns[this.keyColumns[0]] = keyColumns[0];
			} else {
				tuple.columns[this.keyColumns[0]] = keyColumns[0];
				tuple.columns[this.keyColumns[1]] = keyColumns[1];
			}
			tuple.columns[this.valueColumns[0]] = this.getResult.value;
			return 1;
		}
		return 0;
	}
	
	@Override
	public int getTuple(Integer key, Tuple tuple) {
		this.rootNode.get(key, this.getResult);
		if (this.getResult.success) {
			if (this.keyColumnTypes[0] == DataType.INT)
				tuple.columns[this.keyColumns[0]] = DbInteger.create(key);
			else
				tuple.columns[this.keyColumns[0]] = DbString.load(key, this.typeManager);
			
			tuple.columns[this.valueColumns[0]] = this.getResult.value;
			
			return 1;
		}
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
	
	public int loadTuple(int key, DbTypeBase value1, DbTypeBase value2, Tuple tuple) {
		if (this.keyColumnTypes[0] == DataType.INT)
			tuple.columns[this.keyColumns[0]] = DbInteger.create(key);
		else
			tuple.columns[this.keyColumns[0]] = DbString.load(key, this.typeManager);
		
		tuple.columns[this.valueColumns[0]] = value1;
		if (tuple.columns.length > 2)
			tuple.columns[this.valueColumns[1]] = value2;
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
	
	@Override
	public void deleteAll() {
		super.deleteAll();
		if (this.changeTracker != null)
			this.changeTracker.delete();
	}
}
