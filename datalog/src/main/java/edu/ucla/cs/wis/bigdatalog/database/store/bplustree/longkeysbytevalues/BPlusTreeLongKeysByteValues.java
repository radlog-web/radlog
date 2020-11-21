package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTrackingStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version of the b+tree could be used with:
// 1) unordered heap stores as an index
// 2) as storage - the first column is the key and the remaining n-1 columns are the data

// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeLongKeysByteValues 
	extends BPlusTreeStoreValueStructure<BPlusTreeLongKeysByteValuesPage, BPlusTreeLongKeysByteValuesLeaf, BPlusTreeLongKeysByteValuesNode>
	implements TupleBPlusTreeStoreStructure<byte[]>, ChangeTrackingStore<Long>, 
		RangeSearchableStorageStructure<Long>, Serializable {
	private static final long serialVersionUID = 1L;
			
	protected BPlusTreeLongKeysByteValuesInsertResult insertResult;
	protected BPlusTreeLongKeysByteValuesGetResultRange getResultRange;
	
	public BPlusTreeLongKeysByteValues() { super(); }
	
	public BPlusTreeLongKeysByteValues(int nodeSize, int bytesPerValue, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, TypeManager typeManager) {
		super(nodeSize, 8, bytesPerValue, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, typeManager);
		this.insertResult = new BPlusTreeLongKeysByteValuesInsertResult(bytesPerValue);
		this.getResultRange = new BPlusTreeLongKeysByteValuesGetResultRange();
		this.initialize();
	}
	
	@Override
	protected BPlusTreeLongKeysByteValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeLongKeysByteValuesLeaf(this.nodeSize, this.bytesPerValue);
		
		return new BPlusTreeLongKeysByteValuesNode(this.nodeSize, this.bytesPerValue);
	}
	
	@Override
	public void insert(Tuple tuple) {
		long key = this.getKeyL(tuple.columns);
		byte[] data = this.getBytes(tuple.columns);
		this.insert(key, data);
	}
	
	@Override
	public void insert(DbTypeBase[] keyColumns, byte[] data) {
		long key = this.getKeyL(keyColumns);
		this.insert(key, data);
	}
	
	public byte[] insert(long key, byte[] data) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key, data, this.insertResult);
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)	
			this.numberOfEntries++;
				
		if (this.insertResult.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeLongKeysByteValuesNode newRoot = (BPlusTreeLongKeysByteValuesNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();

			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
		
		if (this.trackModifiedTuples)
			if (this.insertResult.status == BPlusTreeOperationStatus.NEW 
				|| this.insertResult.status == BPlusTreeOperationStatus.UPDATE)
				this.changeTracker.add(key);
		
		return this.insertResult.oldValue;
	}
	
	@Override
	public byte[] get(DbTypeBase[] keyColumns) {
		return this.get(this.getKeyL(keyColumns));
	}
	
	public byte[] get(long key) {
		if (this.rootNode.isEmpty())		
			return null;
		
		return this.rootNode.get(key);
	}
	
	@Override
	public boolean delete(Tuple tuple) {
		return this.delete(this.getKeyL(tuple.columns));
	}
		
	public boolean delete(long key) {
		if (this.rootNode.isEmpty())
			return false;
		
		boolean status = this.rootNode.delete(key);
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
	public int getTuple(Long key, Tuple tuple) {
		byte[] value = this.rootNode.get(key);
		if (value != null) {
			int offset = 0;						
			byte[] keyBytes = ByteArrayHelper.getLongAsBytes(key);	
			
			for (int i = 0; i < this.keyColumns.length; i++) {
				tuple.columns[this.keyColumns[i]] = DbTypeBase.loadFrom(this.keyColumnTypes[i], keyBytes, offset, this.typeManager);
				offset += this.keyColumnTypes[i].getNumberOfBytes();
			}
			
			offset = 0;
			for (int i = 0; i < this.valueColumns.length; i++) {
				tuple.columns[this.valueColumns[i]] = DbTypeBase.loadFrom(this.valueColumnTypes[i], value, offset, this.typeManager);
				offset += this.valueColumnTypes[i].getNumberOfBytes();
			}
			
			return 1;
		}
		return 0;
	}

	@Override
	public void getTuple(Long startKey, Long endKey, RangeSearchResult result) {
		//System.out.println(startKey);
		//System.out.println(endKey);
		this.rootNode.get(startKey, endKey, this.getResultRange);
		
		result.success = this.getResultRange.success;
		if (this.getResultRange.success) {
			result.index = this.getResultRange.index;
			result.leaf = this.getResultRange.leaf;
			//System.out.println("found");
			result.success = true;
			return;
		}
		//System.out.println("not found");
		result.success = false;
	}
}
