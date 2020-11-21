package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.keyvaluelist;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class GeneralKeyValueList 
	extends KeyValueList implements Serializable {
	private static final long serialVersionUID = 1L;

	public GeneralKeyValueList() { super(); }
	
	public GeneralKeyValueList(DataType keyType, DataType valueType, TypeManager typeManager) {
		super(keyType, valueType, typeManager);		
	}
	
	@Override
	public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result) {
		this.data.reset();
		byte[] keyBytes = key.getBytes();
		int i, compare = 0;
		byte[] entryKeyBytes; 
		for (i = 0; i < this.numberOfEntries; i++) {
			entryKeyBytes = this.data.read(this.getKeySize());
			compare = ByteArrayHelper.compare(keyBytes, entryKeyBytes, this.getKeySize());
			if (compare == 0) {
				result.oldValue = DbTypeBase.loadFrom(this.valueType, this.data, this.typeManager);//.load(this.data);
				this.data.setOffset(this.data.getOffset() - this.getValueSize());
				this.data.write(value.getBytes());
				result.status = KeyValueOperationStatus.UPDATE;
				return;
			}
			if (compare < 0)
				break;
			this.data.setOffset(this.data.getOffset() + this.getValueSize());
		}
		
		// extend array
		this.data.grow(this.bytesPerEntry);
		if (i != this.numberOfEntries) {
			this.data.shiftRight(this.bytesPerEntry * i, this.bytesPerEntry);
			this.data.setOffset(this.bytesPerEntry * i);
		}
		this.data.write(keyBytes);
		this.data.write(value.getBytes());

		this.numberOfEntries++;
		result.status = KeyValueOperationStatus.NEW;
	}
		
	public void get(DbTypeBase key, KeyValueStoreGetResult result) {
		this.data.reset();
		byte[] keyBytes = key.getBytes();
		int compare = 0;
		byte[] entryKeyBytes;
		for (int i = 0; i < this.numberOfEntries; i++) {
			entryKeyBytes = this.data.read(this.getKeySize());
			compare = ByteArrayHelper.compare(keyBytes, entryKeyBytes, this.getKeySize());
			if (compare == 0) {
				result.value = DbTypeBase.loadFrom(this.valueType, this.data, this.typeManager);
				result.success = true;
				return;
			}
			if (compare < 0)
				break;
			this.data.setOffset(this.data.getOffset() + this.getValueSize());
		}
		result.success = false;		
	}
	
	public boolean remove(DbTypeBase key) {
		boolean status = false;
		byte[] keyBytes = key.getBytes();
		byte[] entryKeyBytes;
		this.data.reset();
		for (int deleteAt = 0; deleteAt < this.numberOfEntries; deleteAt++) {
			entryKeyBytes = this.data.read(this.getKeySize());
			if (ByteArrayHelper.compare(keyBytes, entryKeyBytes, this.getKeySize()) == 0) {
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