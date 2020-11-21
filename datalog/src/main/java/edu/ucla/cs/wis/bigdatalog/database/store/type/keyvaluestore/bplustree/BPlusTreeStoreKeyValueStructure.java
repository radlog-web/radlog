package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class BPlusTreeStoreKeyValueStructure<P extends BPlusTreeElement, L extends BPlusTreeLeaf<L>, N extends BPlusTreeNode<P, L>> 
	extends BPlusTreeStoreStructure<P, L, N> implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int bytesPerValue;
	protected int[] valueColumns;
	protected DataType[] valueColumnTypes;
	protected TypeManager typeManager;
	
	public BPlusTreeStoreKeyValueStructure() { super(); }
	
	public BPlusTreeStoreKeyValueStructure(int nodeSize, int bytesPerKey, int bytesPerValue, int[] keyColumns, DataType[] keyColumnTypes, 
			int valueColumns[], DataType[] valueColumnTypes, TypeManager typeManager) {
		super(nodeSize, bytesPerKey, keyColumns, keyColumnTypes);
		this.bytesPerValue = bytesPerValue;
		this.valueColumns = valueColumns;
		this.valueColumnTypes = valueColumnTypes;
		this.typeManager = typeManager;
	}
		
	abstract public void get(DbTypeBase key, KeyValueStoreGetResult result);
	
	abstract public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result);
	
	abstract public boolean remove(DbTypeBase key);
}
