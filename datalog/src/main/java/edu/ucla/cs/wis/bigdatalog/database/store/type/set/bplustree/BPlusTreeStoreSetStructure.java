package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class BPlusTreeStoreSetStructure<P extends BPlusTreeElement, L extends BPlusTreeLeaf<L>, N extends BPlusTreeNode<P, L>> 
	extends BPlusTreeStoreStructure<P, L, N> 
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public BPlusTreeStoreSetStructure() { super(); }
	
	public BPlusTreeStoreSetStructure(int nodeSize, int bytesPerKey, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, bytesPerKey, keyColumns, keyColumnTypes);
	}
	
	//abstract public DbTypeBase get(DbTypeBase key);
	
	abstract public boolean delete(DbTypeBase key);

	//abstract public DbTypeBase insert(DbTypeBase key);
}
