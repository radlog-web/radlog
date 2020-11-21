package edu.ucla.cs.wis.bigdatalog.database.store.type;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.Data;
import edu.ucla.cs.wis.bigdatalog.database.store.keyvalue.KeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.BigEncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbByte;
import edu.ucla.cs.wis.bigdatalog.database.type.DbComplex;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbKeyValueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLongLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.DbShort;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//this class stores and retrieves list objects from underlying byte storage
//format in storage:
//  1 byte - identifying the data type of the head
//  4 bytes or 8 bytes (floats) - identifying the value (by key except float and int) of the head
//  4 byte - identifying the tail
//    - 0 for no tail
//    - key for the list representing the tail 
public class ListStorageManager implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final int NIL_LIST_KEY = -1;
	
	public class DbListLoader {
		public DbTypeBase head;
		public DbList tail;
	}

	private KeyValueStore store;
	private TypeManager typeManager;
	
	public ListStorageManager(){}
	
	public ListStorageManager(TypeManager typeManager) {
		this.store = new KeyValueStore();
		this.typeManager = typeManager;
	}

	public long write(DbTypeBase head, DbList tail) {
		long key = ListStorageManager.getKey(head, tail);
		
		// check if already stored
		// if not, write to store and return key
		if (this.store.get(key) == null) {			
			Data data = new Data(4096);
			// first write head
			data.write(head.getDataType().getIdAsByte());
			switch (head.getDataType()) {
				case INT:
					data.writeInt(((DbInteger)head).getValue());
					break;
				case FLOAT:
					data.write(((DbFloat)head).getBytes());
					break;
				case DOUBLE:
					data.write(((DbDouble)head).getBytes());
					break;
				case LIST:
				case DATETIME:
				case LONG:
					data.writeLong(((BigEncodedType)head).getKeyL());
					break;
				case SET:
				case KEYVALUESTORE:
					data.writeInt(((DbInteger)head).getValue());
					break;
				case SHORT:
					data.writeShort(((DbShort)head).getValue());
					break;
				case BYTE:
					data.write(((DbByte)head).getValue());
					break;
				default:
					data.writeInt(((EncodedType)head).getKey());
					break;
			}

			if (tail == null || tail.isEmpty()) {
				// signifies tail is null
				data.writeLong(0);
			} else {
				// otherwise, tail is list
				data.writeLong(tail.getKeyL());
			}
			
			this.store.put(key, data.read(0, data.getOffset()));
		}
				
		return key;
	}
	
	private static long getKey(DbTypeBase head, DbList tail) {
		// APS 1/17/2014 - bug with string and integer heads being encoded the same
		// encode the type of the head as well
		// 1 for the type + 8 for the data		
		int numberOfBytes = 9;
		// for tail
		if (tail != null && !tail.isEmpty())
			numberOfBytes += 8;
		
		byte[] bytes = new byte[numberOfBytes];
		bytes[0] = head.getDataType().getIdAsByte();
		
		int offset = ByteArrayHelper.getLongAsBytes(head.hashCodeL(), bytes, 1);
		
		if (tail != null && !tail.isEmpty())
			ByteArrayHelper.getLongAsBytes(tail.hashCodeL(), bytes, offset);
		
		return Math.abs(MurmurHash.hash(bytes));
	}

	public DbListLoader read(long key) {
		byte[] value = this.store.get(key);
		if (value == null)
			return null;
		
		// use a data object, since its easier to manipulate
		Data data = new Data(value.length);
		data.write(value);
		data.reset();
		
		DbListLoader loader = new DbListLoader();
		DataType dataType = DataType.getDataType(data.read());
		switch (dataType) {
			case INT:
				loader.head = DbInteger.create(data.readInt());
				break;
			case STRING:
				loader.head = DbString.load(data.readInt(), this.typeManager);
				break;
			case FLOAT:
				loader.head = DbFloat.create(data.readFloat());
				break;
			case DOUBLE:
				loader.head = DbDouble.create(data.readDouble());
				break;
			case LONG:
				loader.head = DbLong.create(data.readLong());
				break;
			case COMPLEX:
				loader.head = DbComplex.load(data.readInt(), this.typeManager);
				break;
			case LIST:
				loader.head = DbList.load(data.readLong(), this.typeManager);
				break;
			//case ARRAY:
			//	loader.head = DbArray.create(data.readInt());
			//	break;
			case LONGLONG:
				loader.head = DbLongLong.create(data.read(DbLongLong.LENGTH));
				break;
			case LONGLONGLONGLONG:
				loader.head = DbLongLongLongLong.create(data.read(DbLongLongLongLong.LENGTH));
				break;
			case SET:
				loader.head = DbSet.load(data.readInt(), this.typeManager);
				break;
			case KEYVALUESTORE:
				loader.head = DbKeyValueStore.load(data.readInt(), this.typeManager);
				break;
			case DATETIME:
				loader.head = DbDateTime.create(data.readLong());
				break;
			case SHORT:
				loader.head = DbShort.create(data.readShort());
				break;
			case BYTE:
				loader.head = DbByte.create(data.read());
				break;
		}
		
		long tailKey = data.readLong();
		if (tailKey != 0)
			loader.tail = DbList.load(tailKey, this.typeManager);
				
		return loader;		
	}

	public void clear() {	
		this.store.clear();
	}	
}
