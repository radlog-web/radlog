package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbShort extends DbNumericType implements EncodedType {
	private static final long serialVersionUID = 1L;

	public static final short cacheSize = 2048;	
	public static DbShort[] cache;
	
	static {
		cache = new DbShort[cacheSize];
		for (short i = 0; i < cacheSize; i++)
			cache[i] = new DbShort(i);
	}

	private short value;

	private DbShort(short val) {
		this.value = val;
	}

	public static DbShort create(short value) {
		if (value > -1 && value < cacheSize)
			return cache[value];
		
		return new DbShort(value);
	}
	
	public short getValue() { return this.value; }
	
	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;
		
		if (other instanceof DbShort)
			return (this.value == ((DbShort)other).value);	 

		if (other instanceof DbInteger)
			return (this.value == ((DbInteger)other).getValue());
		
		if (other instanceof DbLong)
			return (this.value == ((DbLong)other).getValue());

		if (other instanceof DbDouble)
			return ((DbDouble)other).equals(this);
		
		if (other instanceof DbFloat)
			return ((DbFloat)other).equals(this);
		
		if (other instanceof DbShort)
			return (this.value == ((DbShort)other).getValue());
		
		if (other instanceof DbByte)
			return (this.value == ((DbByte)other).getValue());
		
		if (other instanceof DbLongLong)
			return ((DbLongLong)other).equals(this);
		
		if (other instanceof DbLongLongLongLong)
			return ((DbLongLongLongLong)other).equals(this);

		return false;		 
	}
	
	@Override
	public boolean greaterThan(DbTypeBase other) {
		if (other == null)
			return false;

		if (other instanceof DbShort)
			return (this.value > ((DbShort)other).value);
		
		if (other instanceof DbInteger)
			return (this.value > ((DbInteger)other).getValue());
		
		if (other instanceof DbLong)
			return (this.value > ((DbLong)other).getValue());

		if (other instanceof DbDouble)
			return ((DbDouble)other).lessThan(this);
		
		if (other instanceof DbFloat)
			return ((DbFloat)other).lessThan(this);
		
		if (other instanceof DbShort)
			return (this.value > ((DbShort)other).getValue());
		
		if (other instanceof DbByte)
			return (this.value > ((DbByte)other).getValue());
		
		if (other instanceof DbLongLong)
			return ((DbLongLong)other).lessThan(this);
		
		if (other instanceof DbLongLongLongLong)
			return ((DbLongLongLongLong)other).lessThan(this);
		
		return false;
	}
	
	@Override
	public boolean lessThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (other instanceof DbShort)
			return (this.value < ((DbShort)other).value);
		
		if (other instanceof DbInteger)
			return (this.value < ((DbInteger)other).getValue());
		
		if (other instanceof DbLong)
			return (this.value < ((DbLong)other).getValue());
		
		if (other instanceof DbDouble)
			return ((DbDouble)other).greaterThan(this);
		
		if (other instanceof DbFloat)
			return ((DbFloat)other).greaterThan(this);
		
		if (other instanceof DbShort)
			return (this.value < ((DbShort)other).getValue());
		
		if (other instanceof DbByte)
			return (this.value < ((DbByte)other).getValue());
		
		if (other instanceof DbLongLong)
			return ((DbLongLong)other).greaterThan(this);
		
		if (other instanceof DbLongLongLongLong)
			return ((DbLongLongLongLong)other).greaterThan(this);
		
		return false;
	}

	@Override
	public int hashCode()  {
		return (int)MurmurHash.hash(ByteArrayHelper.getIntAsBytes(this.value));
	}
	
	@Override
	public long hashCodeL() {
		return MurmurHash.hash(ByteArrayHelper.getIntAsBytes(this.value));
	}

	@Override
	public long hashCodeL(int position) {
		return MurmurHash.hash(ByteArrayHelper.getIntAsBytes(this.value), position);
	}
	
	@Override
	public String toString() {
		return Integer.toString(this.value);
	}
	
	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.SHORT; }

	@Override
	public DbShort copy() { return DbShort.create(this.value); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {	
		return ByteArrayHelper.getShortAsBytes(this.value, bytes, offset);
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getShortAsBytes(this.value);
	}
	
	@Override
	public int getKey() { return this.value; }

	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(2,2);
	}
	
	@Override
	public DbNumericType add(DbNumericType addend2) {
		return DbShort.create((short) (this.value + ((DbShort)addend2).value));
	}
	
	@Override
	public DbNumericType subtract(DbNumericType subtrahend) {
		return DbShort.create((short) (this.value - ((DbShort)subtrahend).value));
	}

	@Override
	public DbNumericType multiply(DbNumericType multiplier) {
		return DbShort.create((short) (this.value * ((DbShort)multiplier).value));
	}

	@Override
	public DbNumericType divide(DbNumericType divisor) {
		if (((DbShort)divisor).value == 0)
			throw new InterpreterException("Divided by zero");		
		return DbDouble.create(((double)this.value) / (double)((DbShort)divisor).value);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbShort)dbTypeObject).value); 
	}
	
	@Override
	public DbDouble logarithm() {
		return DbDouble.create(java.lang.Math.log10(this.value));
	}

    @Override
    public DbDouble exponential() {
        return DbDouble.create(java.lang.Math.exp(this.value));
    }

    @Override
    public DbInteger step() {
        return DbInteger.create(DbNumericType.step_impl(this.value));
    }
}
