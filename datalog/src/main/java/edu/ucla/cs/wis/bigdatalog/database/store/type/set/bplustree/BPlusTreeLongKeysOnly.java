package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeLongKeysOnly 
	extends edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly.BPlusTreeLongKeysOnly 
	implements Serializable {
	private static final long serialVersionUID = 1L;

	public BPlusTreeLongKeysOnly() { super(); }
	
	public BPlusTreeLongKeysOnly(int nodeSize, DataType[] keyColumnTypes) {
		super(nodeSize, BPlusTreeStoreStructure.getKeyColumns(keyColumnTypes), keyColumnTypes);
	}
	
	public DbTypeBase insert(DbTypeBase key) {
		this.insert(this.getKeyL(new DbTypeBase[]{key}));
		return key;
	}
	
	public DbTypeBase[] insert(DbTypeBase[] key) {
		this.insert(this.getKeyL(key));
		return key;
	}
	
	public DbTypeBase get(DbTypeBase key) {
		if (this.get(this.getKeyL(new DbTypeBase[]{key})) == null)
			return null;
		return key;
	}
	
	public DbTypeBase[] get(DbTypeBase[] key) {
		if (this.get(this.getKeyL(key)) == null)
			return null;
		return key;
	}

	public boolean delete(DbTypeBase key) {
		return this.delete(this.getKeyL(new DbTypeBase[]{key}));
	}
	
	public boolean delete(DbTypeBase[] key) {
		return this.delete(this.getKeyL(key));
	}
	
}
