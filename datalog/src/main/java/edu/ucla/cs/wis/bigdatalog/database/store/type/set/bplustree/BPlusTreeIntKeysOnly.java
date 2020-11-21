package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeIntKeysOnly 
	extends edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnly 
	implements Serializable {
	private static final long serialVersionUID = 1L;

	public BPlusTreeIntKeysOnly() { super(); }
	
	public BPlusTreeIntKeysOnly(int nodeSize, DataType[] keyColumnTypes) {
		super(nodeSize, BPlusTreeStoreStructure.getKeyColumns(keyColumnTypes), keyColumnTypes);
	}
	
	public DbTypeBase insert(DbTypeBase key) {
		this.insert(((EncodedType)key).getKey(), this.insertResult);
		return key;
	}

	public DbTypeBase get(DbTypeBase key) {
		if (this.get(((EncodedType)key).getKey()) == null)
			return null;
		return key;
	}
	
	public boolean delete(DbTypeBase key) {
		return this.delete(((EncodedType)key).getKey());
	}

}
