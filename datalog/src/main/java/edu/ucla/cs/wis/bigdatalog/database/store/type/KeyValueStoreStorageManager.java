package edu.ucla.cs.wis.bigdatalog.database.store.type;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreType;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.bytekeysbytevalues.BPlusTreeByteKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.bplustree.intkeysbytevalues.BPlusTreeIntKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable.GeneralHashTable;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable.IntegerKeyHashTable;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.keyvaluelist.GeneralKeyValueList;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.keyvaluelist.IntegerKeyValueList;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class KeyValueStoreStorageManager implements Serializable {
	private static final long serialVersionUID = 1L;

	private final int INITIAL_SIZE = 256;
	
	private KeyValueStoreType keyValueStoreType;
	private double splitPolicy;
	private int directorySize;
	private int segmentSize;
	private int numberOfInitialBuckets;
	private int nodeSize;
	
	public class DbKeyValueStoreLoader {
		public KeyValueStoreStructure keyValueStore;
	}
	
	private TypeManager typeManager;
	private KeyValueStoreStructure[] keyValueStructures;
	private int highWaterMark;
	
	public KeyValueStoreStorageManager() {}
	
	public KeyValueStoreStorageManager(DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		this.initialize();
		this.typeManager = typeManager;
		this.keyValueStoreType = KeyValueStoreType.getKeyValueStoreType(deALSConfiguration.getProperty("deals.database.type.keyvalue.type"));
		this.splitPolicy = Double.parseDouble(deALSConfiguration.getProperty("deals.database.indexes.hash.splitpolicy"));
		this.directorySize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.indexes.hash.directorysize"));
		this.segmentSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.indexes.hash.segmentsize"));
		this.numberOfInitialBuckets = Integer.parseInt(deALSConfiguration.getProperty("deals.database.indexes.hash.numberofinitialbuckets"));
		this.nodeSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.indexes.bplustree.nodesize"));
	}
	
	private void initialize() {
		this.keyValueStructures = new KeyValueStoreStructure[INITIAL_SIZE];
		this.highWaterMark = 0;
	}
	
	public Pair<Integer, KeyValueStoreStructure> createKeyValue(DataType keyType, DataType valueType) {
		return createKeyValue(new DataType[]{keyType}, new DataType[]{valueType});
	}
	
	public Pair<Integer, KeyValueStoreStructure> createKeyValue(DataType[] keyTypes, DataType[] valueTypes) {
		// if full, we have to grow the table
		if (this.highWaterMark == this.keyValueStructures.length) {
			// just double the size of the array each time for now
			KeyValueStoreStructure[] temp = new KeyValueStoreStructure[this.keyValueStructures.length * 2];
			for (int i = 0; i < this.keyValueStructures.length; i++)
				temp[i] = this.keyValueStructures[i];

			this.keyValueStructures = temp;
		}
		
		int index = this.highWaterMark++;
		KeyValueStoreStructure kvss = this.doCreateKeyValue(keyTypes, valueTypes);
		this.keyValueStructures[index] = kvss; 
		return new Pair<>(index + 1, kvss); // we don't use 0, since that is the default value for integer
	}
	
	private KeyValueStoreStructure doCreateKeyValue(DataType[] keyTypes, DataType[] valueTypes) {
		KeyValueStoreStructure kvs = null;
		switch (this.keyValueStoreType) {
			case BPLUSTREE:
				kvs = createBPlusTree(keyTypes, valueTypes, this.nodeSize, this.typeManager);
				break;
			case HASHTABLE:
				kvs = createHashTable(keyTypes, valueTypes, this.splitPolicy, this.directorySize, 
						this.segmentSize, this.numberOfInitialBuckets, this.typeManager);
				break;
			case KEYVALUELIST:
				kvs = createKeyValueList(keyTypes, valueTypes, this.typeManager);
				break;
		}
		return kvs;
	}
		
	private static KeyValueStoreStructure createBPlusTree(DataType[] keyTypes, DataType[] valueTypes, int nodeSize, TypeManager typeManager) {
		//int nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.type.bplustree.nodesize"));
		int bytesPerKey = 0;
		int bytesPerValue = 0;
		
		for (DataType dataType : keyTypes)
			bytesPerKey += dataType.getNumberOfBytes();
		
		if (valueTypes != null && valueTypes.length > 0)
			for (DataType dataType : valueTypes)
				bytesPerValue += dataType.getNumberOfBytes();

		if (bytesPerKey == 4) {
			//return new BPlusTreeIntKeysDbTypeValues(conserveMemory, nodeSize, bytesPerValue, 
			//		new int[]{0}, keyTypes, new int[]{0}, valueTypes);
			return new BPlusTreeIntKeysByteValues(nodeSize, bytesPerValue, new int[]{0}, keyTypes, new int[]{0}, valueTypes, typeManager);
		}
		
		return new BPlusTreeByteKeysByteValues(nodeSize, bytesPerKey, bytesPerValue, new int[]{0}, keyTypes, new int[]{0}, valueTypes, typeManager);
	}
	
	private static KeyValueStoreStructure createHashTable(DataType[] keyTypes, DataType[] valueTypes, double splitPolicy, 
			int directorySize, int segmentSize, int numberOfInitialBuckets, TypeManager typeManager) {
		/*double splitPolicy = Double.parseDouble(DeALSContext.getConfiguration().getProperty("deals.database.type.hashtable.splitpolicy"));
		int directorySize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.type.hashtable.directorysize"));
		int segmentSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.type.hashtable.segmentsize"));
		int numberOfInitialBuckets = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.type.hashtable.numberofinitialbuckets"));
		*/
		if (keyTypes.length == 1) {
			if (keyTypes[0] == DataType.INT  || keyTypes[0] == DataType.STRING || keyTypes[0] == DataType.COMPLEX)
				return new IntegerKeyHashTable(valueTypes, splitPolicy, directorySize, segmentSize, numberOfInitialBuckets, typeManager);
		}
		
		return new GeneralHashTable(keyTypes, valueTypes, splitPolicy, directorySize, segmentSize, numberOfInitialBuckets, typeManager);
	}
	
	private static KeyValueStoreStructure createKeyValueList(DataType[] keyTypes, DataType[] valueTypes, TypeManager typeManager) {
		if (keyTypes[0] == DataType.INT || keyTypes[0] == DataType.STRING 
				|| keyTypes[0] == DataType.COMPLEX)
			return new IntegerKeyValueList(valueTypes[0], typeManager);
		
		return new GeneralKeyValueList(keyTypes[0], valueTypes[0], typeManager);
	}
	
	public DbKeyValueStoreLoader getKVS(int id) {
		if (id > this.highWaterMark || id < 1)
			return null;
		
		DbKeyValueStoreLoader loader = new DbKeyValueStoreLoader();
		loader.keyValueStore = this.keyValueStructures[id - 1]; // array is 0 based, but our ids are 1 based
		return loader;
	}
	
	public void clear() {
		this.initialize();
	}
	
	public String toString() {
		return "# of keyvalue structures: " + this.highWaterMark;
	}
}