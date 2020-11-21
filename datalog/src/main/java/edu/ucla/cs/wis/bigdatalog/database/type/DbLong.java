package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbLong extends DbNumericType implements BigEncodedType {
	private static final long serialVersionUID = 1L;

	public static final int cacheSize = 256;	
	public static DbLong[] cache;
		
	static {
		cache = new DbLong[cacheSize];
		for (int i = 0; i < cacheSize; i++)
			cache[i] = new DbLong(i);
	}
	
	private long value;
	
	private DbLong(long value) {
		this.value = value;
	}
	
	public static DbLong create(long value) {
		if (value > -1 && value < cacheSize)
			return cache[(int) value];
		
		return new DbLong(value);
	}
	
	public long getValue() {return this.value;}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;

		if (this == other)
			return true;

		if (other instanceof DbLong)
			return (this.value == ((DbLong)other).value);
		
		if (other instanceof DbInteger)
			return (this.value == ((DbInteger)other).getValue());

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

		if (other instanceof DbLong)
			return (this.value > ((DbLong)other).value);
		
		if (other instanceof DbInteger)
			return (this.value > ((DbInteger)other).getValue());		
		
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

		if (other instanceof DbLong)
			return (this.value < ((DbLong)other).value);
		
		if (other instanceof DbInteger)
			return (this.value < ((DbInteger)other).getValue());		
		
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
		return (int)MurmurHash.hash(ByteArrayHelper.getLongAsBytes(this.value));
	}
	
	@Override
	public long hashCodeL() {
		return MurmurHash.hash(ByteArrayHelper.getLongAsBytes(this.value));
	}

	@Override
	public long hashCodeL(int position) {
		return MurmurHash.hash(ByteArrayHelper.getLongAsBytes(this.value), position);
	}

	@Override
	public String toString() {
		return Long.toString(this.value);
	}
	
	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.LONG; }

	@Override
	public DbLong copy() { return DbLong.create(this.value); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {	
		return ByteArrayHelper.getLongAsBytes(this.value, bytes, offset);
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getLongAsBytes(this.value);
	}
	
	@Override
	public int getKey() { return -1; }
	
	@Override
	public long getKeyL() { return this.value; }

	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(8, 8);
	}

	@Override
	public DbNumericType add(DbNumericType addend2) {
		return DbLong.create(this.value + ((DbLong)addend2).value);
	}
	
	@Override
	public DbNumericType subtract(DbNumericType subtrahend) {
		return DbLong.create(this.value - ((DbLong)subtrahend).value);
	}

	@Override
	public DbNumericType multiply(DbNumericType multiplier) {
		return DbLong.create(this.value * ((DbLong)multiplier).value);
	}

	@Override
	public DbNumericType divide(DbNumericType divisor) {
		if (((DbLong)divisor).getValue() == 0)
			throw new InterpreterException("Divided by zero");
    	return DbDouble.create(((double)this.value) / (double)((DbLong)divisor).value);
    }
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbLong)dbTypeObject).value); 
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
