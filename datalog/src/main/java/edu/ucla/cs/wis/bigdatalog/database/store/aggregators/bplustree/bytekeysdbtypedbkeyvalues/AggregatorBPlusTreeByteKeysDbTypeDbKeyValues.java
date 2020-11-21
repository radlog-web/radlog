package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorInsertStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.AggregatorBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class AggregatorBPlusTreeByteKeysDbTypeDbKeyValues 
	extends AggregatorBPlusTreeStoreStructure<AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesPage, 
		AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf, AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesNode> 
	implements ChangeTrackingStore<byte[]>, RangeSearchableStorageStructure<byte[]>, Serializable {
	private static final long serialVersionUID = 1L;

	protected AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesInsertResult insertResult;
	protected AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResult getResult;
	protected AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResultRange getResultRange;
	
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValues(){ super(); }
	
	public AggregatorBPlusTreeByteKeysDbTypeDbKeyValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		super(nodeSize, BPlusTreeStoreStructure.getBytesPerKey(keyColumnTypes, keyColumns), keyColumns, keyColumnTypes, 
				valueColumns, valueColumnTypes, aggregateInfos, typeManager);
		
		this.insertResult = new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesInsertResult();
		this.getResult = new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResult();
		this.getResultRange = new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesGetResultRange();
		this.initialize();
	}

	@Override
	public int getBytesPerKey() { return this.bytesPerKey; }
	
	@Override
	protected AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesPage allocatePage() {
		if (this.rootNode == null)
			return new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesLeaf(this.nodeSize, this.bytesPerKey, this.valueColumnTypes[0], 
					this.valueColumnTypes[1], this.aggregateInfos[0].aggregateType);

		return new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesNode(this.nodeSize, this.bytesPerKey, this.valueColumnTypes[0], 
				this.valueColumnTypes[1], this.aggregateInfos[0].aggregateType);
	}

	@Override
	public void insert(Tuple tuple, AggregatorResult result) {
		byte[] key = this.getKey(tuple.columns); 

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
		AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesNode newRoot = (AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);

		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
	}
	
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		this.rootNode.get(this.getKey(keyColumns), this.getResult);
		if (this.getResult.success) {
			for (int i = 0; i < keyColumns.length; i++)
				tuple.columns[this.keyColumns[i]] = keyColumns[i];

			tuple.columns[this.valueColumns[0]] = this.getResult.value;
			return 1;
		}
		return 0;
	}
	
	public int getTuple(byte[] key, Tuple tuple) {
		this.rootNode.get(key, this.getResult);
		if (this.getResult.success) {
			int offset = 0;
			for (int i = 0; i < this.keyColumns.length; i++) {
				tuple.columns[this.keyColumns[i]] = DbTypeBase.loadFrom(this.keyColumnTypes[i], key, offset, this.typeManager);
				offset += this.keyColumnTypes[i].getNumberOfBytes();
			}
						
			tuple.columns[this.valueColumns[0]] = this.getResult.value;
			
			return 1;
		}
		return 0;
	}
	
	public void getTuple(byte[] startKey, byte[] endKey, RangeSearchResult result) {
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
	
	public int loadTuple(byte[] key, DbTypeBase value1, DbTypeBase value2, Tuple tuple) {
		int offset = 0;
		for (int i = 0; i < this.keyColumns.length; i++) {
			tuple.columns[this.keyColumns[i]] = DbTypeBase.loadFrom(this.keyColumnTypes[i], key, offset, this.typeManager);
			offset += this.keyColumnTypes[i].getNumberOfBytes();
		}
		
		tuple.columns[this.valueColumns[0]] = value1;
		if (tuple.columns.length > (this.keyColumns.length + 1))
			tuple.columns[this.valueColumns[1]] = value2;		
		return 1;
	}

	@Override
	public boolean delete(Tuple tuple) {		
		boolean status = this.rootNode.delete(this.getKey(tuple.columns));
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
