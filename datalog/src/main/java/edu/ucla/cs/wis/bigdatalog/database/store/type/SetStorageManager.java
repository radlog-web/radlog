package edu.ucla.cs.wis.bigdatalog.database.store.type;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeIntKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeLongKeysOnly;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class SetStorageManager implements Serializable {
	private static final long serialVersionUID = 1L;
	private final int INITIAL_SIZE = 256;
	
	public class DbSetLoader {
		public BPlusTreeStoreStructure<?, ?, ?> set;
	}
	
	private BPlusTreeStoreStructure<?, ?, ?>[] sets;
	private int highWaterMark;
	private int nodeSize;
	
	public SetStorageManager(int nodeSize) {
		this.initialize();
		this.nodeSize = nodeSize;
	}
	
	private void initialize() {
		this.sets = new BPlusTreeStoreStructure<?, ?, ?>[INITIAL_SIZE];
		this.highWaterMark = 0;
	}
	
	public Pair<Integer, BPlusTreeStoreStructure<?, ?, ?>> createSet(DataType[] keyTypes) {
		// if full, we have to grow the table
		if (this.highWaterMark == this.sets.length) {
			// just double the size of the array each time for now
			BPlusTreeStoreStructure<?, ?, ?>[] temp = new BPlusTreeStoreStructure<?, ?, ?>[this.sets.length * 2];
			for (int i = 0; i < this.sets.length; i++)
				temp[i] = this.sets[i];

			this.sets = temp;
		}
		
		int index = this.highWaterMark++;
		BPlusTreeStoreStructure<?, ?, ?> set = SetStorageManager.doCreateSet(keyTypes, this.nodeSize/*, this.deALSConfiguration*/);
		this.sets[index] = set;
		return new Pair<Integer, BPlusTreeStoreStructure<?, ?, ?>>(index + 1, set); // we don't use 0, since that is the default value for integer
	}
	
	private static BPlusTreeStoreStructure<?, ?, ?> doCreateSet(DataType[] keyTypes, int nodeSize/*, 
			DeALSConfiguration deALSConfiguration*/) {
		int bytesPerKey = 0;
		
		for (DataType dataType : keyTypes)
			bytesPerKey += dataType.getNumberOfBytes();
		
		if (bytesPerKey == 4)
			return new BPlusTreeIntKeysOnly(nodeSize, keyTypes);
		
		if (bytesPerKey == 8)
			return new BPlusTreeLongKeysOnly(nodeSize, keyTypes);

		return new BPlusTreeByteKeysOnly(nodeSize, bytesPerKey, keyTypes/*, deALSConfiguration*/);
	}

	public DbSetLoader getSet(int id) {
		if (id > this.highWaterMark || id < 1)
			return null;
		
		DbSetLoader loader = new DbSetLoader();
		loader.set = this.sets[id - 1]; // array is 0 based, but our ids are 1 based
		return loader;
	}
	
	public void clear() {
		this.initialize();
	}
	
	public String toString() {
		return "# of sets: " + this.highWaterMark;
	}
}