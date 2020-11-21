package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.keyvaluelist;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.Data;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class KeyValueList
	implements KeyValueStoreStructure, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	protected DataType keyType;
	protected DataType valueType;
	protected TypeManager typeManager;
	protected int bytesPerEntry;		
	protected Data data;
	protected int numberOfEntries;
	
	public KeyValueList() { super(); }
	
	public KeyValueList(DataType keyType, DataType valueType, TypeManager typeManager) {
		this.keyType = keyType;
		this.valueType = valueType;
		this.typeManager = typeManager;
		this.bytesPerEntry = this.getKeySize() + this.getValueSize();
		this.initialize();		
	}	
	
	protected void initialize(){
		this.data = new Data(0);
		this.numberOfEntries = 0;	
	}
	
	public int getKeySize() { return this.keyType.getNumberOfBytes(); }
	
	public int getValueSize() { return this.valueType.getNumberOfBytes(); }

	@Override
	public DataType getValueDataType() { return this.valueType; }
	
	public int getNumberOfEntries() { return this.numberOfEntries; }
	
	public void clear() { 
		this.initialize(); 
	}
	
	@Override
	public String toStringShort() {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < this.numberOfEntries; i++) {
			if (i > 0)
				output.append(", ");
			output.append("[Key:");
			output.append(this.data.read(this.getKeySize()));
			output.append("|Value:");
			output.append(this.data.read(this.getValueSize()));
			output.append("]");
		}
		return output.toString();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {		
		return new MemoryMeasurement(this.data.getSize(), this.data.getSize());
	}
	
	abstract public void get(DbTypeBase key, KeyValueStoreGetResult result);
	
	abstract public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result);
	
	abstract public boolean remove(DbTypeBase key);

}
