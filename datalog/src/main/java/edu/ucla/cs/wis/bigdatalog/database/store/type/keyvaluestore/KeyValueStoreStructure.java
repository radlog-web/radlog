package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public interface KeyValueStoreStructure {

	public DataType getValueDataType();
	
	public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result);
	
	public void get(DbTypeBase key, KeyValueStoreGetResult result);
	
	public boolean remove(DbTypeBase key);
	
	public void clear();

	public int getNumberOfEntries();
	
	public String toStringShort();
	
	public MemoryMeasurement getSizeOf();
}
