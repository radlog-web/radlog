package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.BPlusTreeStoreKeyValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//this version of the b+tree is for the DbBPlusTree type

//THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeIntKeysByteValues 
	extends BPlusTreeStoreKeyValueStructure<BPlusTreeIntKeysByteValuesPage, BPlusTreeIntKeysByteValuesLeaf, BPlusTreeIntKeysByteValuesNode> 
	implements KeyValueStoreStructure, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeIntKeysByteValuesInsertResult insertResult;
	protected BPlusTreeIntKeysByteValuesGetResult getResult;
	
	public BPlusTreeIntKeysByteValues() { super(); }
	
	public BPlusTreeIntKeysByteValues(int nodeSize, int bytesPerValue, 
			int[] keyColumns, DataType[] keyColumnTypes, int[] valueColumns, DataType[] valueColumnTypes, TypeManager typeManager) {
		super(nodeSize, 4, bytesPerValue, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, typeManager);
		this.insertResult = new BPlusTreeIntKeysByteValuesInsertResult(bytesPerValue);
		this.getResult = new BPlusTreeIntKeysByteValuesGetResult(bytesPerValue);
		this.initialize();
	}

	@Override
	public DataType getValueDataType() { return this.valueColumnTypes[0]; }
	
	@Override
	protected BPlusTreeIntKeysByteValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeIntKeysByteValuesLeaf(this.nodeSize, this.bytesPerValue);
		
		return new BPlusTreeIntKeysByteValuesNode(this.nodeSize, this.bytesPerValue);
	}
	
	@Override
	public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result) {	
		int encodedKey = ((EncodedType)key).getKey();
		// case 1 - room to insert in root node
		this.rootNode.insert(encodedKey, value.getBytes(), this.insertResult);
		if (this.insertResult.status == KeyValueOperationStatus.NEW)
			this.numberOfEntries++;
				
		if (this.insertResult.newPage != null) {			
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeIntKeysByteValuesNode newRoot = (BPlusTreeIntKeysByteValuesNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
					
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}

		result.status = this.insertResult.status;		
		if (this.insertResult.status == KeyValueOperationStatus.UPDATE)
			result.oldValue = DbTypeBase.loadFrom(this.getValueDataType(), this.insertResult.oldValue, this.typeManager);
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

		result.value = DbTypeBase.loadFrom(this.getValueDataType(), this.getResult.value, this.typeManager);
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
	
	public DbLongLong sumAllValues() {
		return this.rootNode.sumAllValues();
	}
}
