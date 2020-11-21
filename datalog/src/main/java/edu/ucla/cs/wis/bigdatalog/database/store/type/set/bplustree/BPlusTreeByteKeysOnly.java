package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class BPlusTreeByteKeysOnly 
	extends edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnly 
	implements Serializable {
	private static final long serialVersionUID = 1L;

	public BPlusTreeByteKeysOnly() { super(); }
	
	public BPlusTreeByteKeysOnly(int nodeSize, int bytesPerKey, DataType[] keyColumnTypes) {
		super(nodeSize, bytesPerKey, keyColumnTypes);
	}
	
	public DbTypeBase[] insert(DbTypeBase[] key) {
		this.insert(this.get(getKey(key)));
		return key;
	}
	
	public DbTypeBase[] get(DbTypeBase[] key) {
		if (this.get(getKey(key)) == null)
			return null;
		return key;
	}

	public boolean delete(DbTypeBase[] key) {
		return this.delete(getKey(key));
	}
}
