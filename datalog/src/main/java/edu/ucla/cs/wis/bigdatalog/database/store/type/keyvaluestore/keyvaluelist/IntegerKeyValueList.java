package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.keyvaluelist;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class IntegerKeyValueList 
	extends KeyValueList implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public IntegerKeyValueList() { super(); }
	
	public IntegerKeyValueList(DataType valueType, TypeManager typeManager) {
		super(DataType.INT, valueType, typeManager);
		this.initialize();
	}
	
	public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result) {
		this.data.reset();
		int entryKey, i;
		int intKey = ((EncodedType)key).getKey();
		
		for (i = 0; i < this.numberOfEntries; i++) {
			entryKey = this.data.readInt();
			if (intKey == entryKey) {
				result.oldValue = DbTypeBase.loadFrom(this.valueType, this.data, this.typeManager);//.load(this.data);
				this.data.setOffset(this.data.getOffset() - this.getValueSize());
				this.data.write(value.getBytes());
				result.status = KeyValueOperationStatus.UPDATE;
				return;
			}
			if (intKey > entryKey)
				break;
			this.data.setOffset(this.data.getOffset() + this.getValueSize());
		}
		
		// extend array
		this.data.grow(this.bytesPerEntry);
		if (i != this.numberOfEntries) {
			this.data.shiftRight(this.bytesPerEntry * i, this.bytesPerEntry);
			this.data.setOffset(this.bytesPerEntry * i);
		}
		this.data.writeInt(intKey);
		this.data.write(value.getBytes());

		this.numberOfEntries++;
		result.status = KeyValueOperationStatus.NEW;
	}
		
	public void get(DbTypeBase key, KeyValueStoreGetResult result) {
		this.data.reset();
		int entryKey;
		int intKey = ((EncodedType)key).getKey();
		for (int i = 0; i < this.numberOfEntries; i++) {
			entryKey = this.data.readInt();
			if (intKey == entryKey) {
				result.success = true;
				result.value = DbTypeBase.loadFrom(this.valueType, this.data, this.typeManager);//.load(this.data);
				return;
			}
			
			if (intKey > entryKey)
				break;
			this.data.setOffset(this.data.getOffset() + this.getValueSize());
		}		
		result.success = false;
	}
	
	public boolean remove(DbTypeBase key) {
		this.data.reset();
		boolean status = false;		
		int entryKey;
		int intKey = ((EncodedType)key).getKey();
		for (int deleteAt = 0; deleteAt < this.numberOfEntries; deleteAt++) {
			entryKey = this.data.readInt();
			if (intKey == entryKey) {
				// move all keys and values after this position, left one key or value worth
				this.data.shiftLeft(this.data.getOffset() + this.getValueSize(), this.bytesPerEntry);
				this.numberOfEntries--;
				status = true;
				break;
			}
			this.data.setOffset(this.data.getOffset() + this.getValueSize());
		}
		return status;	
	}
}