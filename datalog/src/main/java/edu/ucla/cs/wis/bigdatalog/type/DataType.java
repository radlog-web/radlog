package edu.ucla.cs.wis.bigdatalog.type;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerByte;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerDouble;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerFloat;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerInt;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerLong;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerLongLong;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerLongLongLongLong;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerShort;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.DbByte;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLongLongLongLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbShort;

public enum DataType {
	INT(1, "integer", 4, 3, false),
	FLOAT(2, "float", 4, 7, false),
	STRING(3, "string", 4, -1, true),
	COMPLEX(4, "complex", 4, -1, true),
	LIST(5, "list", 8, -1, true),
	//HASHTABLE(6, "hashtable", 4, -1),
	LONG(7, "long", 8, 4, false),
	//ARRAY(8, "array", 4, -1),
	//BPLUSTREE(9, "bplustree", 4, -1),
	LONGLONG(10, "longlong", 16, 5, false),
	//KEYVALUELIST(11, "keyvaluelist", 4, -1),
	LONGLONGLONGLONG(12, "longlonglonglong", 32, 6, false),
	ANY(13, "any", 8, -1, false),	// this should only be used in predicates with lists
	SET(14, "set", 4, -1, true),
	KEYVALUESTORE(15, "keyvaluestore", 4, -1, true),
	DATETIME(16, "datetime", 8, -1, false),
	UNKNOWN(17, "unknown", -1, 0, false),
	DOUBLE(18, "double", 8, 8, false),
	SHORT(19, "short", 8, 2, false),
	BYTE(20, "byte", 8, 1, false),
	AVERAGE(21, "average", 16, -1, false);

	private int id;
	private String name;
	private int numberOfBytes;
	private int order;
	private boolean usesSecondaryStorage;
	
	private DataType(int id, String name, int bytes, int order, boolean usesSecondaryStorage) {
		this.id = id;
		this.name = name;
		this.numberOfBytes = bytes;
		this.order = order;
		this.usesSecondaryStorage = usesSecondaryStorage;
	}
	
	public int getId() { return this.id; }
	
	public byte getIdAsByte() {return (byte)this.id;}
	
	public String getName() { return this.name; }

	public int getNumberOfBytes() { return this.numberOfBytes; }
	
	public int getOrder() { return this.order; }
	
	public boolean usesSecondaryStorage() { return this.usesSecondaryStorage; }
	
	public String toString() { return this.name; }
	
	public static boolean isNumeric(DataType dataType) {
		return isInteger(dataType) || isDecimal(dataType);
	}
	
	public static boolean isDecimal(DataType dataType) {
		switch (dataType) {
			case FLOAT:
			case DOUBLE:			
				return true;
		}
		return false;
	}
	
	public static boolean isInteger(DataType dataType) {
		switch (dataType) {
			case INT:			
			case LONG:
			case LONGLONG:
			case LONGLONGLONGLONG:
			case BYTE:
			case SHORT:
				return true;
		}
		return false;
	}
	
	public static boolean isPromotable(DataType from, DataType to) {
		if (from == to)
			return true;
		
		return (from.order <= to.order);
	}
	
	public static boolean comparable(DataType from, DataType to) {
		if (from == to)
			return true;
		
		if (DataType.isNumeric(from) && DataType.isNumeric(to))
			return true;
				
		return false;
	}
	
	public static boolean isVariableLength(DataType dataType) {
		switch (dataType) {
		case STRING:
		case COMPLEX:
		case LIST:
		case SET:
		case KEYVALUESTORE:
			return true;
		}
		return false;
	}
		
	public static DataType getDataType(byte id) {
		switch (id) {
		case 1:
			return DataType.INT;
		case 2:
			return DataType.FLOAT;
		case 3:
			return DataType.STRING;
		case 4:
			return DataType.COMPLEX;
		case 5:
			return DataType.LIST;
		case 7:
			return DataType.LONG;
		case 10:
			return DataType.LONGLONG;
		case 12:
			return DataType.LONGLONGLONGLONG;
		case 13:
			return DataType.ANY;
		case 14:
			return DataType.SET;
		case 15:
			return DataType.KEYVALUESTORE;
		case 16:
			return DataType.DATETIME;
		case 18:
			return DataType.DOUBLE;
		case 19:
			return DataType.SHORT;
		case 20:
			return DataType.BYTE;
		case 21:
			return DataType.AVERAGE;
		}
		
		return null;
	}
	
	public static DataType getDataType(String name) {
		for (DataType dataType : DataType.values())
			if (dataType.getName().equals(name.toLowerCase()))
				return dataType;
		
		return null;
	}
	
	public static CompilerTypeBase cast(CompilerTypeBase value, DataType dataType) {
		if (value.isConstant()) {
			switch (dataType) {
				case INT:
					switch (value.getDataType()) {
						case BYTE:
							return new CompilerInt(((CompilerByte)value).getValue());
						case SHORT:
							return new CompilerInt(((CompilerShort)value).getValue());
						case LONG:
							return new CompilerInt((int)((CompilerLong)value).getValue());
						case LONGLONG:
							return new CompilerInt((int)((CompilerLongLong)value).getValue());
						case LONGLONGLONGLONG:
							return new CompilerInt((int)((CompilerLongLongLongLong)value).getValue());
						case FLOAT:
							return new CompilerInt((int)((CompilerFloat)value).getValue());
						case DOUBLE:
							return new CompilerInt((int)((CompilerDouble)value).getValue());
					}
					break;
				case LONG:
					switch (value.getDataType()) {
						case BYTE:
							return new CompilerLong(((CompilerByte)value).getValue());
						case SHORT:
							return new CompilerLong(((CompilerShort)value).getValue());
						case INT:
							return new CompilerLong(((CompilerInt)value).getValue());
						case LONGLONG:
							return new CompilerLong(((CompilerLongLong)value).getValue());
						case LONGLONGLONGLONG:
							return new CompilerLong(((CompilerLongLongLongLong)value).getValue());
						case FLOAT:
							return new CompilerLong((long)((CompilerFloat)value).getValue());
						case DOUBLE:
							return new CompilerLong((long)((CompilerDouble)value).getValue());
					}
					break;
				case LONGLONG:
					switch (value.getDataType()) {
						case BYTE:
							return new CompilerLongLong(((CompilerByte)value).getValue());
						case SHORT:
							return new CompilerLongLong(((CompilerShort)value).getValue());
						case INT:
							return new CompilerLongLong(((CompilerInt)value).getValue());	
						case LONG:
							return new CompilerLongLong(((CompilerLong)value).getValue());
						case LONGLONGLONGLONG:
							return new CompilerLongLong(((CompilerLongLongLongLong)value).getValue());
						case FLOAT:
							return new CompilerLongLong((long)((CompilerFloat)value).getValue());
						case DOUBLE:
							return new CompilerLongLong((long)((CompilerDouble)value).getValue());
					}
					break;
				case LONGLONGLONGLONG:
					switch (value.getDataType()) {
						case BYTE:
							return new CompilerLongLongLongLong(((CompilerByte)value).getValue());
						case SHORT:
							return new CompilerLongLongLongLong(((CompilerShort)value).getValue());
						case INT:
							return new CompilerLongLongLongLong(((CompilerInt)value).getValue());	
						case LONG:
							return new CompilerLongLongLongLong(((CompilerLong)value).getValue());
						case LONGLONG:
							return new CompilerLongLongLongLong(((CompilerLongLong)value).getValue());
						case FLOAT:
							return new CompilerLongLongLongLong((long)((CompilerFloat)value).getValue());
						case DOUBLE:
							return new CompilerLongLongLongLong((long)((CompilerDouble)value).getValue());
					}
					break;					
				case FLOAT:
					switch (value.getDataType()) {
						case BYTE:
							return new CompilerFloat(((CompilerByte)value).getValue());
						case SHORT:
							return new CompilerFloat(((CompilerShort)value).getValue());
						case INT:
							return new CompilerFloat(((CompilerInt)value).getValue());	
						case LONG:
							return new CompilerFloat(((CompilerLong)value).getValue());
						case LONGLONG:
							return new CompilerFloat(((CompilerLongLong)value).getValue());
						case LONGLONGLONGLONG:
							return new CompilerFloat(((CompilerLongLongLongLong)value).getValue());
						case DOUBLE:
							return new CompilerFloat((float) ((CompilerDouble)value).getValue());
					}
					break;
				case DOUBLE:
					switch (value.getDataType()) {
						case BYTE:
							return new CompilerDouble(((CompilerByte)value).getValue());
						case SHORT:
							return new CompilerDouble(((CompilerShort)value).getValue());
						case INT:
							return new CompilerDouble(((CompilerInt)value).getValue());	
						case LONG:
							return new CompilerDouble(((CompilerLong)value).getValue());
						case LONGLONG:
							return new CompilerDouble(((CompilerLongLong)value).getValue());
						case LONGLONGLONGLONG:
							return new CompilerDouble(((CompilerLongLongLongLong)value).getValue());
						case FLOAT:
							return new CompilerDouble(((CompilerFloat)value).getValue());
					}
					break;
				case SHORT:
					switch (value.getDataType()) {
						case BYTE:
							return new CompilerShort(((CompilerByte)value).getValue());
						case INT:
							return new CompilerShort((short)((CompilerInt)value).getValue());
						case LONG:
							return new CompilerShort((short)((CompilerLong)value).getValue());
						case LONGLONG:
							return new CompilerShort((short)((CompilerLongLong)value).getValue());
						case LONGLONGLONGLONG:
							return new CompilerShort((short)((CompilerLongLongLongLong)value).getValue());
						case FLOAT:
							return new CompilerShort((short)((CompilerFloat)value).getValue());
						case DOUBLE:
							return new CompilerShort((short)((CompilerDouble)value).getValue());
					}
					break;
				case BYTE:
					switch (value.getDataType()) {
						case SHORT:
							return new CompilerByte(((CompilerByte)value).getValue());
						case INT:
							return new CompilerByte((byte)((CompilerInt)value).getValue());
						case LONG:
							return new CompilerByte((byte)((CompilerLong)value).getValue());
						case LONGLONG:
							return new CompilerByte((byte)((CompilerLongLong)value).getValue());
						case LONGLONGLONGLONG:
							return new CompilerByte((byte)((CompilerLongLongLongLong)value).getValue());
						case FLOAT:
							return new CompilerByte((byte)((CompilerFloat)value).getValue());
						case DOUBLE:
							return new CompilerByte((byte)((CompilerDouble)value).getValue());
					}
					break;
			}
		}
		return value;
	}
	
	public static DbNumericType cast(DbNumericType value, DataType dataType) {
		switch (dataType) {
			case INT:
				switch (value.getDataType()) {
					case BYTE:
						return DbInteger.create(((DbByte)value).getValue());
					case SHORT:
						return DbInteger.create(((DbShort)value).getValue());
					case LONG:
						return DbInteger.create((int)((DbLong)value).getValue());
					case LONGLONG:
						return ((DbLongLong)value).toDbInteger();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)value).toDbInteger();
					case FLOAT:
						return DbInteger.create((int)((DbFloat)value).getValue());
					case DOUBLE:
						return DbInteger.create((int)((DbDouble)value).getValue());
				}
				break;
			case LONG:
				switch (value.getDataType()) {
					case BYTE:
						return DbLong.create(((DbByte)value).getValue());
					case SHORT:
						return DbLong.create(((DbShort)value).getValue());
					case INT:
						return DbLong.create(((DbInteger)value).getValue());
					case LONGLONG:
						return (((DbLongLong)value).toDbLong());
					case LONGLONGLONGLONG:
						return (((DbLongLongLongLong)value).toDbLong());
					case FLOAT:
						return DbLong.create((long)((DbFloat)value).getValue());
					case DOUBLE:
						return DbLong.create((long)((DbDouble)value).getValue());
				}
				break;
			case DOUBLE:
				switch (value.getDataType()) {
					case BYTE:
						return DbDouble.create(((DbByte)value).getValue());
					case SHORT:
						return DbDouble.create(((DbShort)value).getValue());
					case INT:
						return DbDouble.create(((DbInteger)value).getValue());	
					case LONG:
						return DbDouble.create(((DbLong)value).getValue());
					case LONGLONG:
						return ((DbLongLong)value).toDbDouble();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)value).toDbDouble();
					case FLOAT:
						return DbDouble.create(((DbFloat)value).getValue());
				}
				break;
			case FLOAT:
				switch (value.getDataType()) {
					case BYTE:
						return DbFloat.create(((DbByte)value).getValue());
					case SHORT:
						return DbFloat.create(((DbShort)value).getValue());
					case INT:
						return DbFloat.create(((DbInteger)value).getValue());	
					case LONG:
						return DbFloat.create(((DbLong)value).getValue());
					case LONGLONG:
						return ((DbLongLong)value).toDbFloat();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)value).toDbFloat();
					case DOUBLE:
						return DbFloat.create((float) ((DbDouble)value).getValue());
				}
				break;	
			case LONGLONG:
				switch (value.getDataType()) {
					case BYTE:
						return DbLongLong.create(((DbByte)value).getValue());
					case SHORT:
						return DbLongLong.create(((DbShort)value).getValue());
					case INT:
						return DbLongLong.create(((DbInteger)value).getValue());	
					case LONG:
						return DbLongLong.create(((DbLong)value).getValue());
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)value).toDbLongLong();
					case FLOAT:
						return DbLongLong.create((long)((DbFloat)value).getValue());
					case DOUBLE:
						return DbLongLong.create((long)((DbDouble)value).getValue());
				}
				break;
			case LONGLONGLONGLONG:
				switch (value.getDataType()) {
					case BYTE:
						return DbLongLongLongLong.create(((DbByte)value).getValue());
					case SHORT:
						return DbLongLongLongLong.create(((DbShort)value).getValue());
					case INT:
						return DbLongLongLongLong.create(((DbInteger)value).getValue());	
					case LONG:
						return DbLongLongLongLong.create(((DbLong)value).getValue());
					case LONGLONG:
						return DbLongLongLongLong.create(((DbLongLong)value).getBytes());
					case FLOAT:
						return DbLongLongLongLong.create((long)((DbFloat)value).getValue());
					case DOUBLE:
						return DbLongLongLongLong.create((long)((DbDouble)value).getValue());
				}
				break;					
			case BYTE:
				switch (value.getDataType()) {
					case SHORT:
						return DbByte.create((byte) ((DbShort)value).getValue());
					case INT:
						return DbByte.create((byte) ((DbInteger)value).getValue());
					case LONG:
						return DbByte.create((byte) ((DbLong)value).getValue());
					case LONGLONG:
						return ((DbLongLong)value).toDbByte();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)value).toDbByte();
					case FLOAT:
						return DbByte.create((byte)((DbFloat)value).getValue());
					case DOUBLE:
						return DbByte.create((byte)((DbDouble)value).getValue());
				}
				break;					
			case SHORT:
				switch (value.getDataType()) {
					case BYTE:
						return DbShort.create(((DbByte)value).getValue());
					case INT:
						return DbShort.create((short) ((DbInteger)value).getValue());
					case LONG:
						return DbShort.create((short) ((DbLong)value).getValue());
					case LONGLONG:
						return ((DbLongLong)value).toDbShort();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)value).toDbShort();
					case FLOAT:
						return DbShort.create((byte)((DbFloat)value).getValue());
					case DOUBLE:
						return DbShort.create((byte)((DbDouble)value).getValue());
				}
				break;
		}
		
		return value;
	}
	
	public static Pair<DbNumericType, DbNumericType> castToHigherType(DbNumericType value0, DbNumericType value1) {
		DataType arg0DataType = value0.getDataType();
		DataType arg1DataType = value1.getDataType();
		
		if (arg0DataType.getOrder() > arg1DataType.getOrder())
			value1 = DataType.cast(value1, arg0DataType);
		else
			value0 = DataType.cast(value0, arg1DataType);
		
		return new Pair<>(value0, value1);
	}
	
	public static DataType getNextHigherOrderType(DataType dataType) {
		if (dataType == LONGLONGLONGLONG || dataType == DOUBLE)
			return dataType;
		
		int order = dataType.getOrder();
		for (DataType dt : DataType.values())
			if (dt.getOrder() == (order + 1))
				return dt;
		
		return dataType;
	}
}
