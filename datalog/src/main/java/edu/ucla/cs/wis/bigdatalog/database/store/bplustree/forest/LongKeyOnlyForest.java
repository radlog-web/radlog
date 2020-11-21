package edu.ucla.cs.wis.bigdatalog.database.store.bplustree.forest;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysonly.BPlusTreeLongKeysOnly;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class LongKeyOnlyForest implements Serializable {
	private static final long serialVersionUID = 1L;

	protected BPlusTreeLongKeysOnly[] forest;
	
	public LongKeyOnlyForest(){}
	
	public LongKeyOnlyForest(int nodeSize, int keyColumns[], DataType[] keyColumnTypes/*, 
			DeALSConfiguration deALSConfiguration, TypeManager typeManager*/) {
		int size = 256;
		this.forest = new BPlusTreeLongKeysOnly[size];
		for (int i = 0; i < size; i++)
			this.forest[i] = new BPlusTreeLongKeysOnly(nodeSize, keyColumns, keyColumnTypes/*, deALSConfiguration, typeManager*/);	
	}
	
	public void insert(long key) {
		byte lowBytes = (byte)(key & 0xFF);
		this.forest[lowBytes].insert(key);
	}
	
	public Long get(long key) {
		byte lowBytes = (byte)(key & 0xFF);
		return this.forest[lowBytes].get(key);
	}

}
