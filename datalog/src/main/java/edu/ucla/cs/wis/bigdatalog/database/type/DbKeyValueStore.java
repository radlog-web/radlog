package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.type.KeyValueStoreStorageManager;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbKeyValueStore 
	extends DbTypeBase {
	private static final long serialVersionUID = 1L;
	
	private int id;
	private KeyValueStoreStructure keyValueStore;
	private KeyValueStorePutResult putResult;
	private KeyValueStoreGetResult getResult;
		
	private DbKeyValueStore(int id) {
		this.id = id;
	}
	
	private DbKeyValueStore(int id, KeyValueStoreStructure store) {
		this.id = id;
		this.keyValueStore = store;
		this.getResult = new KeyValueStoreGetResult(store.getValueDataType());
		this.putResult = new KeyValueStorePutResult(store.getValueDataType());		
	}
	
	public static DbKeyValueStore load(int id, TypeManager typeManager) {
		DbKeyValueStore kvs = new DbKeyValueStore(id);
		kvs.load(typeManager);
		return kvs;
	}
	
	public static DbKeyValueStore load(int id, KeyValueStoreStructure store) {
		return new DbKeyValueStore(id, store);
	}
	
	public int getId() { return this.id; }

	public int getNumberOfEntries() { return this.keyValueStore.getNumberOfEntries(); }
	
	public DbTypeBase put(DbTypeBase key, DbNumericType value) {		
		if (value.getDataType() != this.keyValueStore.getValueDataType())
			value = value.convertTo(this.keyValueStore.getValueDataType());
		
		this.keyValueStore.put(key, value, this.putResult);
		if (this.putResult.status == KeyValueOperationStatus.UPDATE)
			return this.putResult.oldValue; 
					
		return null;
	}
	
	public DbTypeBase get(DbTypeBase key) {
		this.keyValueStore.get(key, this.getResult);
		if (this.getResult.success)
			return this.getResult.value;
		
		return null;
	}
			
	public boolean remove(DbTypeBase key) { return this.keyValueStore.remove(key); }
	
	public void clear() { this.keyValueStore.clear(); }
	
	@Override
	public DataType getDataType() { return DataType.KEYVALUESTORE; }

	@Override
	public boolean isConstant() { return false; }

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
	
		if (this == other)
			return true;
		
		if (!(other instanceof DbKeyValueStore))
			return false;
		
		DbKeyValueStore otherKVS = (DbKeyValueStore)other;
		return this.id == otherKVS.id;
	}

	@Override
	public boolean greaterThan(DbTypeBase other) { return false; }
	
	@Override
	public boolean lessThan(DbTypeBase other) { return false; }

	@Override
	public int hashCode() { return 0; }

	@Override
	public long hashCodeL() { return 0; }

	@Override
	public long hashCodeL(int position) { return 0; }

	@Override
	public String toString() {
		//this.loadKeyValueStore();
		return "Id: " + this.id + " | # of entries: " + this.keyValueStore.getNumberOfEntries() + " " + this.keyValueStore.toStringShort();
	}

	@Override
	public DbTypeBase copy() { return new DbKeyValueStore(this.id, this.keyValueStore); }

	public void load(TypeManager typeManager) {
		if (this.id > 0) {
			KeyValueStoreStorageManager.DbKeyValueStoreLoader loader = typeManager.getKeyValueStore(this.id);
			if (loader != null) {
				this.keyValueStore = loader.keyValueStore;
				this.getResult = new KeyValueStoreGetResult(this.keyValueStore.getValueDataType());
				this.putResult = new KeyValueStorePutResult(this.keyValueStore.getValueDataType());
			}
		}
	}
	
	@Override
	public int getBytes(byte[] bytes, int offset) {
		return ByteArrayHelper.getIntAsBytes(this.id, bytes, offset);
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getIntAsBytes(this.id);
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		//this.loadKeyValueStore();
		int used = 4; // for the integer key
		int allocated = 4; // for the integer key
		MemoryMeasurement mm;
		
		if (this.keyValueStore != null) {
			mm = this.keyValueStore.getSizeOf();
			used += mm.getUsed();
			allocated += mm.getAllocated();
		}		
		return new MemoryMeasurement(used, allocated);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { return this.equals(dbTypeObject); }
		
	@Override
	public boolean isValid() { 
		return (this.id != 0);
	}
}
