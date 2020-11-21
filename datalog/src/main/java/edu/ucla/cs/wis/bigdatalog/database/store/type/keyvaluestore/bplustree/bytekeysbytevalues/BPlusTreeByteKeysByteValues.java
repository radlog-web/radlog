package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.bytekeysbytevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.BPlusTreeStoreKeyValueStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//this version of the b+tree is for the DbBPlusTree type

//THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeByteKeysByteValues 
	extends BPlusTreeStoreKeyValueStructure<BPlusTreeByteKeysByteValuesPage, BPlusTreeByteKeysByteValuesLeaf, BPlusTreeByteKeysByteValuesNode> 
	implements KeyValueStoreStructure, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected BPlusTreeByteKeysByteValuesInsertResult insertResult;
	protected BPlusTreeByteKeysByteValuesGetResult getResult;
	
	public BPlusTreeByteKeysByteValues() { super(); }
	
	public BPlusTreeByteKeysByteValues(int nodeSize, int bytesPerKey, int bytesPerValue, int[] keyColumns, DataType[] keyColumnTypes, 
			int[] valueColumns, DataType[] valueColumnTypes, TypeManager typeManager) {
		super(nodeSize, bytesPerKey, bytesPerValue, keyColumns, keyColumnTypes, valueColumns, valueColumnTypes, typeManager);
		
		this.insertResult = new BPlusTreeByteKeysByteValuesInsertResult(bytesPerValue);
		this.getResult = new BPlusTreeByteKeysByteValuesGetResult(bytesPerValue);
		this.initialize();
	}
	
	@Override
	public DataType getValueDataType() { return this.valueColumnTypes[0]; }
	
	@Override
	protected BPlusTreeByteKeysByteValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeByteKeysByteValuesLeaf(this.nodeSize, this.bytesPerKey, this.bytesPerValue);
		
		return new BPlusTreeByteKeysByteValuesNode(this.nodeSize, this.bytesPerKey, this.bytesPerValue);
	}
	
	@Override
	public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result) {
		// case 1 - room to insert in root node
		this.rootNode.insert(key.getBytes(), value.getBytes(), this.insertResult);
		if (this.insertResult.status == KeyValueOperationStatus.NEW);
			this.numberOfEntries++;
		
		if (this.insertResult.newPage != null) {				
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeByteKeysByteValuesNode newRoot = (BPlusTreeByteKeysByteValuesNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
			System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
			
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
		
		result.status = this.insertResult.status;
		if (result.status == KeyValueOperationStatus.UPDATE)
			result.oldValue = DbTypeBase.loadFrom(getValueDataType(), this.insertResult.oldValue, this.typeManager);//result.oldValue.load(this.insertResult.oldValue);
	}
	
	@Override
	public void get(DbTypeBase key, KeyValueStoreGetResult result) {
		if (this.rootNode == null) {
			result.success = false;
			return;
		}
		
		this.rootNode.get(key.getBytes(), this.getResult);
		if (!this.getResult.success) {
			result.success = false;
			return;
		}
		
		result.value = DbTypeBase.loadFrom(this.getValueDataType(), this.getResult.value, this.typeManager);//result.value.load(this.getResult.value);
		result.success = true;
	}
	
	@Override
	public boolean remove(DbTypeBase key) {
		if (this.rootNode == null)
			return false;
		
		byte[] keyBytes = key.getBytes();		
		boolean status = this.rootNode.delete(keyBytes); 
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
}
