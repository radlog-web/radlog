package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore;

public enum KeyValueStoreType {
	BPLUSTREE("bplustree"), 
	HASHTABLE("hashtable"), 
	KEYVALUELIST("keyvaluelist");
	
	private String name;
	private KeyValueStoreType(String name) {
		this.name = name;
	}
	
	public String getName() { return this.name; }
	
	public static KeyValueStoreType getKeyValueStoreType(String name) {
		for (KeyValueStoreType keyValueType : KeyValueStoreType.values())
			if (keyValueType.name.equals(name))
				return keyValueType;
		return null;
	}
	
}
