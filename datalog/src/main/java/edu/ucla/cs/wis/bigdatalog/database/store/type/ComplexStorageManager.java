package edu.ucla.cs.wis.bigdatalog.database.store.type;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.Data;
import edu.ucla.cs.wis.bigdatalog.database.store.keyvalue.KeyValueStore;
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
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this class stores and retrieves complex objects from underlying byte storage
// format in storage:
//   name - 4 bytes
//   number of arguments - 4 bytes
//   for each argument - 
//     1 byte - identifying the data type
//     4 bytes or 8 bytes (floats) - identifying the value (by key except float and int)
public class ComplexStorageManager implements Serializable {
	private static final long serialVersionUID = 1L;

	public class DbComplexLoader {
		public DbString name;
		public DbTypeBase[] arguments;
	}
	
	private StringStorageManager stringManager;
	private KeyValueStore store;
	private TypeManager typeManager;
	
	public ComplexStorageManager() {}
	
	public ComplexStorageManager(StringStorageManager stringManager, TypeManager typeManager) {
		this.stringManager = stringManager;
		this.store = new KeyValueStore();
		this.typeManager = typeManager;
	}

	public int write(String name, DbTypeBase[] args) {	
		int key = ComplexStorageManager.getKey(name, args);
		
		// check if already stored
		// if not, write to store and return key
		if (this.store.get(key) == null) {
			int nameId = this.stringManager.write(name);

			Data data = new Data(4096);
			data.writeInt(nameId);
			data.writeInt(args.length);
			for (DbTypeBase arg : args) {
				data.write(arg.getDataType().getIdAsByte());
				if (arg instanceof DbInteger)
					data.writeInt(((DbInteger)arg).getValue());
				else if (arg instanceof DbString)
					data.writeInt(((DbString)arg).getKey());
				else if (arg instanceof DbDouble)
					data.write(((DbDouble)arg).getBytes());
				else if (arg instanceof DbFloat)
					data.write(((DbFloat)arg).getBytes());
				else if (arg instanceof DbComplex)
					data.writeInt(((DbComplex)arg).getKey());
				else if (arg instanceof DbList)
					data.writeLong(((DbList)arg).getKeyL());
				else if (arg instanceof DbLong)
					data.writeLong(((DbLong)arg).getKeyL());
				else if (arg instanceof DbLongLong)
					data.write(((DbLongLong)arg).getBytes());
				else if (arg instanceof DbLongLongLongLong)
					data.write(((DbLongLongLongLong)arg).getBytes());
				else if (arg instanceof DbDateTime)
					data.writeLong(((DbDateTime)arg).getKeyL());
				else if (arg instanceof DbShort)
					data.writeShort(((DbShort)arg).getValue());
				else if (arg instanceof DbByte)
					data.write(((DbByte)arg).getValue());
			}

			this.store.put(key, data.read(0, data.getOffset()));
		}

		return key;
	}
	
	private static int getKey(String name, DbTypeBase[] args) {
		int numberOfBytes = 1; // for datatype
		List<byte[]> byteArrays = new LinkedList<>();
		
		byte[] bytes = name.getBytes();
		byteArrays.add(bytes);
		numberOfBytes += bytes.length;
		
		for (int i = 0; i < args.length; i++) {
			bytes = args[i].getBytes();
			numberOfBytes += bytes.length;
			byteArrays.add(bytes);
		}
		
		bytes = new byte[numberOfBytes];
		bytes[0] = DataType.COMPLEX.getIdAsByte();
		int counter = 1;
		
		for (byte[] byteArray : byteArrays)
			for (int i = 0; i < byteArray.length; i++)
				bytes[counter++] = byteArray[i];
		
		return Math.abs((int)MurmurHash.hash(bytes));
	}

	public DbComplexLoader read(int key) {
		byte[] value = this.store.get(key);
		if (value == null)
			return null;
		
		// use a data object, since its easier to manipulate
		Data data = new Data(value.length);
		data.write(value);
		data.reset();
		
		DbComplexLoader loader = new DbComplexLoader();
		
		// first 4 bytes for name
		loader.name = DbString.load(data.readInt(), this.typeManager);
		// next 4 bytes for number of arguments
		int numberOfArguments = data.readInt();
		
		loader.arguments = new DbTypeBase[numberOfArguments];
		for (int i = 0; i < numberOfArguments; i++) {
			// next 4 bytes are for what type of argument

			DataType dataType = DataType.getDataType(data.read());
			switch (dataType) {
			case INT:
				loader.arguments[i] = DbInteger.create(data.readInt());
				break;
			case STRING:
				loader.arguments[i] = DbString.load(data.readInt(), this.typeManager);
				break;
			case FLOAT:
				loader.arguments[i] = DbFloat.create(data.readFloat());
				break;
			case DOUBLE:
				loader.arguments[i] = DbDouble.create(data.readDouble());
				break;
			case LONG:
				loader.arguments[i] = DbLong.create(data.readLong());
				break;
			case COMPLEX:
				loader.arguments[i] = DbComplex.load(data.readInt(), this.typeManager);
				break;
			case LIST:
				loader.arguments[i] = DbList.load(data.readLong(), this.typeManager);
				break;
			//case ARRAY:
			//	loader.arguments[i] = DbArray.create(data.readInt());
			//	break;
			case LONGLONG:
				loader.arguments[i] = DbLongLong.create(data.read(DbLongLong.LENGTH));
				break;
			case LONGLONGLONGLONG:
				loader.arguments[i] = DbLongLongLongLong.create(data.read(DbLongLongLongLong.LENGTH));
				break;
			case SET:
				loader.arguments[i] = DbSet.load(data.readInt(), this.typeManager);
				break;
			case KEYVALUESTORE:
				loader.arguments[i] = DbKeyValueStore.load(data.readInt(), this.typeManager);
				break;
			case DATETIME:
				loader.arguments[i] = DbDateTime.create(data.readLong());
				break;
			case SHORT:
				loader.arguments[i] = DbShort.create(data.readShort());
				break;
			case BYTE:
				loader.arguments[i] = DbByte.create(data.read());
				break;
			}
		}		

		return loader;
	}

	public void clear() {		
		this.store.clear();
	}
}
