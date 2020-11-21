package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysdbtypevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.BPlusTreeStoreKeyValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//this version of the b+tree is for the DbBPlusTree type

//THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeIntKeysDbTypeValues 
	extends BPlusTreeStoreKeyValueStructure<BPlusTreeIntKeysDbTypeValuesPage, BPlusTreeIntKeysDbTypeValuesLeaf, BPlusTreeIntKeysDbTypeValuesNode> 
	implements KeyValueStoreStructure, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeIntKeysDbTypeValuesInsertResult insertResult;
	protected BPlusTreeIntKeysDbTypeValuesGetResult getResult;
	
	public BPlusTreeIntKeysDbTypeValues() { super(); }
	
	public BPlusTreeIntKeysDbTypeValues(int nodeSize, int bytesPerValue, 
			int[] keyColumns, DataType[] keyColumnTypes, int[] valueColumns, DataType[] valueColumnTypes, TypeManager typeManager) {
		super(nodeSize, 4, bytesPerValue, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, typeManager);
		
		this.insertResult = new BPlusTreeIntKeysDbTypeValuesInsertResult(valueColumnTypes[0]);
		this.getResult = new BPlusTreeIntKeysDbTypeValuesGetResult(valueColumnTypes[0]);
		this.initialize();
	}
	
	@Override
	public DataType getValueDataType() { return this.valueColumnTypes[0]; }
	
	@Override
	protected BPlusTreeIntKeysDbTypeValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeIntKeysDbTypeValuesLeaf(this.nodeSize);
		
		return new BPlusTreeIntKeysDbTypeValuesNode(this.nodeSize);
	}
	
	@Override
	public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result) {		
		// case 1 - room to insert in root node
		int encodedKey = ((EncodedType)key).getKey();
		this.rootNode.insert(encodedKey, value, this.insertResult);
		if (this.insertResult.status == KeyValueOperationStatus.NEW)
			this.numberOfEntries++;
				
		if (this.insertResult.newPage != null) {	
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeIntKeysDbTypeValuesNode newRoot = (BPlusTreeIntKeysDbTypeValuesNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
					
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
		
		result.status = this.insertResult.status;		
		if (this.insertResult.status == KeyValueOperationStatus.UPDATE)
			result.oldValue = this.insertResult.oldValue;
	}
	
	@Override
	public void get(DbTypeBase key, KeyValueStoreGetResult result) {
		if (this.rootNode == null) {
			result.success = false;
			return;
		}
		
		this.rootNode.get(((EncodedType)key).getKey(), this.getResult);
		if (!this.getResult.success) {
			result.success = false;
			return;
		}

		result.value = this.getResult.value;
		result.success = true;
	}
	
	@Override
	public boolean remove(DbTypeBase key) {
		if (this.rootNode == null)
			return false;
		
		boolean status = this.rootNode.delete(((EncodedType)key).getKey());
		if (status)
			this.numberOfEntries--;
		return status;
	}	
	
	public void clear() {
		if (this.rootNode == null)
			return;
			
		this.rootNode.deleteAll();
		this.rootNode = null;
		this.initialize();
	}
	
	public String toStringShort() {
		StringBuilder retval = new StringBuilder();
		if (this.rootNode != null)
			retval.append(this.rootNode.toStringShort());
		else
			retval.append("empty");
		return retval.toString();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {
		return rootNode.getSizeOf();
	}
	/*
	public DbLongLong sumAllValues() {
		return this.rootNode.sumAllValues();
	}*/
}
