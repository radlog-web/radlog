package edu.ucla.cs.wis.bigdatalog.database.type;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.ComplexStorageManager;
import edu.ucla.cs.wis.bigdatalog.database.store.type.KeyValueStoreStorageManager;
import edu.ucla.cs.wis.bigdatalog.database.store.type.SetStorageManager;
import edu.ucla.cs.wis.bigdatalog.database.store.type.ListStorageManager;
import edu.ucla.cs.wis.bigdatalog.database.store.type.StringStorageManager;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TypeManager implements Serializable {
	private static final long serialVersionUID = 1L;
	private StringStorageManager stringManager;
	private ComplexStorageManager complexManager;
	private ListStorageManager listManager;
	private SetStorageManager setManager;
	private KeyValueStoreStorageManager keyValueStoreManager;
	private DataType countDataType;
	
	public TypeManager(){}
	
	public TypeManager(DeALSConfiguration deALSConfiguration) {
		this.stringManager = new StringStorageManager();
		this.complexManager = new ComplexStorageManager(this.stringManager, this);
		this.listManager = new ListStorageManager(this);
		this.setManager = new SetStorageManager(Integer.parseInt(deALSConfiguration.getProperty("deals.database.type.set.nodesize")));
		this.keyValueStoreManager = new KeyValueStoreStorageManager(deALSConfiguration, this);
		this.countDataType = deALSConfiguration.getCountDataType();
	}
		
	public KeyValueStoreStorageManager getKeyValueStoreManager() { return this.keyValueStoreManager; } 
	
	public DbString createString(String value) {
		// string manager will return id, which represents the string
		int key = this.stringManager.write(value);
		return DbString.load(key, value);
	}

	public String getString(Integer key) {
		return this.stringManager.read(key);
	}

	public DbComplex createComplex(String name, DbTypeBase[] args) {
		int key = this.complexManager.write(name, args);
		return DbComplex.load(key, this.createString(name), args);
	}
	
	public ComplexStorageManager.DbComplexLoader getComplex(Integer key) {
		return this.complexManager.read(key);
	}

	public DbList createList(DbTypeBase head, DbList tail) {
		if (head == null)
			return DbList.create();
		
		if (tail == null || tail.isEmpty()) {
			if (head instanceof DbList)
				return (DbList)head;
			tail = DbList.create();
		}
						
		long key = this.listManager.write(head, tail);

		return DbList.load(key, head, tail);
	}

	public DbList createList() {
		return createList(null);
	}
	
	public DbList createList(DbTypeBase[] dbTypeObjects) {
		if (dbTypeObjects == null || dbTypeObjects.length == 0)
			return DbList.create();
			
		DbList dbList = DbList.create();
		for (int i = dbTypeObjects.length - 1; i >= 0; i--)
			dbList = this.createList(dbTypeObjects[i], dbList);

		return dbList;
	}
	
	public ListStorageManager.DbListLoader getList(long key) {
		return this.listManager.read(key);
	}

	public DbSet createSet(DataType keyType) {
		return this.createSet(new DataType[]{keyType});
	}
	
	public DbSet createSet(DataType[] keyTypes) {
		Pair<Integer, BPlusTreeStoreStructure<?, ?, ?>> setInfo = this.setManager.createSet(keyTypes);
		return DbSet.load(setInfo.getFirst(), setInfo.getSecond());
	}
	
	public DbKeyValueStore createKeyValueStore(DataType keyType, DataType valueType) {
		Pair<Integer, KeyValueStoreStructure> kvInfo = this.keyValueStoreManager.createKeyValue(keyType, valueType);
		return DbKeyValueStore.load(kvInfo.getFirst(), kvInfo.getSecond());
	}
		
	public SetStorageManager.DbSetLoader getSet(int id) {
		return this.setManager.getSet(id);
	}
	
	public KeyValueStoreStorageManager.DbKeyValueStoreLoader getKeyValueStore(int id) {
		return this.keyValueStoreManager.getKVS(id);
	}
		
	public boolean clear() {
		this.stringManager.clear();
		this.complexManager.clear();
		this.listManager.clear();
		//this.arrayManager.clear();
		this.keyValueStoreManager.clear();
		this.setManager.clear();
		return true;
	}
	
	/***** START - These methods are here to keep a consistent interface for DbTypeBase.toDbType() *****/
	public DbInteger createInt(int value) {
		return DbInteger.create(value);
	}
	
	public DbShort createShort(short value) {
		return DbShort.create(value);
	}
	
	public DbByte createByte(byte value) {
		return DbByte.create(value);
	}
	
	public DbLong createLong(long value) {
		return DbLong.create(value);
	}
		
	public DbLongLong createLongLong(byte[] value) {
		return DbLongLong.create(value);
	}
	
	public DbLongLong createLongLong(long value) {
		return DbLongLong.create(value);
	}
	
	public DbLongLongLongLong createLongLongLongLong(byte[] value) {
		return DbLongLongLongLong.create(value);
	}	
	
	public DbLongLongLongLong createLongLongLongLong(long value) {
		return DbLongLongLongLong.create(value);
	}

	public DbFloat createFloat(float value) {
		return DbFloat.create(value);
	}
	
	public DbDouble createDouble(double value) {
		return DbDouble.create(value);
	}
	/***** END *****/
	
	public DbNumericType castToCountDataType(long value) {
		switch (this.countDataType) {
			case INT:
				return DbInteger.create((int)value);
			case LONG:
				return DbLong.create(value);
			case LONGLONG:
				return DbLongLong.create(value);	
			case LONGLONGLONGLONG:
				return DbLongLongLongLong.create(value);
		}	
		return null;
	}
}
