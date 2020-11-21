package edu.ucla.cs.wis.bigdatalog.database.type;

import java.io.Serializable;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.Data;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class DbTypeBase implements Argument, MemorySize, Serializable {
	private static final long serialVersionUID = 1L;

	//public DbTypeBase() {}
	public boolean isNumber() { return ((this instanceof DbInteger) 
			|| (this instanceof DbFloat) 
			|| (this instanceof DbLong) 
			|| (this instanceof DbLongLong) 
			|| (this instanceof DbLongLongLongLong)
			|| (this instanceof DbDouble)
			|| (this instanceof DbShort)
			|| (this instanceof DbByte)); } 

	public boolean isGround() { return true; }

	public boolean isBound() { return true; }
	
	public int getIntValue() {
		if (this instanceof DbInteger)
			return ((DbInteger)this).getValue();
		if (this instanceof DbLong)
			return (int)((DbLong)this).getValue();
		if (this instanceof DbLongLong)
			return ((DbLongLong)this).toDbInteger().getValue();
		if (this instanceof DbLongLongLongLong)
			return ((DbLongLongLongLong)this).toDbInteger().getValue();
		if (this instanceof DbShort)
			return ((DbShort)this).getValue();
		
		return ((DbByte)this).getValue();
	}

	public boolean notEquals(DbTypeBase other) {
		return (!this.equals(other));
	}

	public boolean lessThan(DbTypeBase other) {
		return (!(this.greaterThan(other) || this.equals(other)));
	}

	public boolean lessThanOrEqualsTo(DbTypeBase other) {
		return (!this.greaterThan(other));
	}

	public boolean greaterThanOrEqualsTo(DbTypeBase other) {
		return (this.greaterThan(other) || this.equals(other));
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) { return this; }

	public static DbTypeBase loadFrom(DataType dataType, byte[] bytes, TypeManager typeManager) {
		return loadFrom(dataType, bytes, 0, typeManager);
	}
	
	public static DbTypeBase loadFrom(DataType dataType, byte[] bytes, int offset, TypeManager typeManager) {
		switch (dataType) {
			case INT:
				return DbInteger.create(ByteArrayHelper.getBytesAsInt(bytes, offset));
			case STRING:
				return DbString.load(ByteArrayHelper.getBytesAsInt(bytes, offset), typeManager);
			case LONG:
				return DbLong.create(ByteArrayHelper.getBytesAsLong(bytes, offset));
			case DOUBLE:
				return DbDouble.create(ByteArrayHelper.getBytesAsDouble(bytes, offset));
			case FLOAT:
				return DbFloat.create(ByteArrayHelper.getBytesAsFloat(bytes, offset));
			case COMPLEX:
				return DbComplex.load(ByteArrayHelper.getBytesAsInt(bytes, offset), typeManager);
			case LIST:
				return DbList.load(ByteArrayHelper.getBytesAsLong(bytes, offset), typeManager);
			case LONGLONG:
				return DbLongLong.create(ByteArrayHelper.getBytesAsBytes(bytes, offset));
			case LONGLONGLONGLONG:
				return DbLongLongLongLong.create(ByteArrayHelper.getBytesAsBytes(bytes, offset));
			case SET:
				return DbSet.load(ByteArrayHelper.getBytesAsInt(bytes, offset), typeManager);
			case KEYVALUESTORE:
				return DbKeyValueStore.load(ByteArrayHelper.getBytesAsInt(bytes, offset), typeManager);
			case DATETIME:
				return DbDateTime.create(ByteArrayHelper.getBytesAsLong(bytes, offset));
			case SHORT:
				return DbShort.create(ByteArrayHelper.getBytesAsShort(bytes, offset));
			case BYTE:
				return DbByte.create(ByteArrayHelper.getByte(bytes, offset));
			case AVERAGE:
				return DbAverage.create((DbNumericType) DbTypeBase.loadFrom(DataType.DOUBLE, bytes, offset, typeManager), 
						ByteArrayHelper.getBytesAsLong(bytes, offset+8));	
		}
		return null;
	}
	
	public static DbTypeBase loadFrom(DataType dataType, Data data, TypeManager typeManager) {
		switch (dataType) {
			case INT:
				return DbInteger.create(data.readInt());
			case STRING:
				return DbString.load(data.readInt(), typeManager);
			case LONG:
				return DbLong.create(data.readLong());
			case DOUBLE:
				return DbDouble.create(data.readDouble());
			case FLOAT:
				return DbFloat.create(data.readFloat());
			case COMPLEX:
				return DbComplex.load(data.readInt(), typeManager);
			case LIST:
				return DbList.load(data.readLong(), typeManager);
			case LONGLONG:
				return DbLongLong.create(data.read(DbLongLong.LENGTH));
			case LONGLONGLONGLONG:
				return DbLongLongLongLong.create(data.read(DbLongLongLongLong.LENGTH));
			case SET:
				return DbSet.load(data.readInt(), typeManager);
			case KEYVALUESTORE:
				return DbKeyValueStore.load(data.readInt(), typeManager);
			case DATETIME:
				return DbDateTime.create(data.readLong());
			case SHORT:
				return DbShort.create(data.readShort());
			case BYTE:
				return DbByte.create(data.read());
			case AVERAGE:
				return DbAverage.create((DbNumericType) DbTypeBase.loadFrom(DataType.DOUBLE, data, typeManager), data.readLong());			
		}
		return null;
	}
	
	// load if possible - this is meant to convert key format from data storage into DbTypes
	public static DbTypeBase loadFrom(DataType dataType, long val, TypeManager typeManager) {
		switch (dataType) {
			case INT:
				return DbInteger.create((int) val);
			case STRING:
				return DbString.load((int) val, typeManager);
			case COMPLEX:
				return DbComplex.load((int) val, typeManager);
			case LONG:
				return DbLong.create(val);
			case DOUBLE:
				return DbDouble.load(val);
			case LONGLONG:
				return DbLongLong.create(val);
			case LONGLONGLONGLONG:
				return DbLongLongLongLong.create(val);
			case FLOAT:
				return DbFloat.load((int)val);
			case LIST:
				return DbList.load(val, typeManager);  // this is bad, but we can use this to pass 0
			case SET:
				return DbSet.load((int) val, typeManager);
			case KEYVALUESTORE:
				return DbKeyValueStore.load((int) val, typeManager);
			case DATETIME:
				return DbDateTime.create(val);
			case SHORT:
				return DbShort.create((short) val);
			case BYTE:
				return DbByte.create((byte) val);
		}
		return null;
	}
	
	public static DbTypeBase loadFrom(DataType dataType, long val) {
		switch (dataType) {
			case INT:
				return DbInteger.create((int) val);
			case LONG:
				return DbLong.create(val);
			case LONGLONG:
				return DbLongLong.create(val);
			case LONGLONGLONGLONG:
				return DbLongLongLongLong.create(val);
			case FLOAT:
				return DbFloat.create(val); // this is bad, but we can use this to pass 0
			case DOUBLE:
				return DbDouble.create(val);
			case DATETIME:
				return DbDateTime.create(val);
			case SHORT:
				return DbShort.create((short) val);
			case BYTE:
				return DbByte.create((byte) val);
		}
		return null;
	}

	public Argument reduce() { return this; }
	
	
	public boolean isValid() { return true; }
	
	@Override
	public boolean matchByFree(Argument boundArgument) {
		return boundArgument.match(this);
	}
	
	@Override
	public boolean matchByBound(Argument freeArgument) {
		return this.equals(freeArgument);
	}

	public String toFact() {
		StringBuilder fact = new StringBuilder();
		fact.append("argument(");
		fact.append(this.hashCode());
		fact.append(",'constant','");
		fact.append(this.toString());
		fact.append("','");
		fact.append(this.getDataType());
		fact.append("').");
		return fact.toString();
	}
	
	public DbTypeBase copy(ArgumentList argumentList) {
		return copy();
	}

	public int compare(DbTypeBase other) {
		if (this.greaterThan(other)) return 1;
		
		if (this.lessThan(other)) return -1;
		
		return 0;
	}
	
	abstract public boolean greaterThan(DbTypeBase other);

	abstract public int hashCode();

	abstract public long hashCodeL();

	abstract public long hashCodeL(int position);

	abstract public String toString();

	abstract public DbTypeBase copy();
	
	abstract public int getBytes(byte bytes[], int offset);

	abstract public byte[] getBytes();
}
