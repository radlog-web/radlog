package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbByte extends DbNumericType implements EncodedType {
	private static final long serialVersionUID = 1L;

	public static DbByte[] cache;
	
	// position 0 is byte(-128) position 128 is 0 and position 255 is 127
	static {
		cache = new DbByte[256];
		for (int i = 0; i < 256; i++)
			cache[i] = new DbByte((byte) (i - 128));
	}

	private byte value;

	private DbByte(byte val) {
		this.value = val;
	}

	public static DbByte create(byte value) {
		return cache[value + 128];
	}
	
	public byte getValue() { return this.value; }
	
	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;
		
		if (other instanceof DbByte)
			return (this.value == ((DbByte)other).value);
			
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
		
		if (other instanceof DbByte)
			return (this.value > ((DbByte)other).value);		
		
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
		
		if (other instanceof DbByte)
			return (this.value < ((DbByte)other).value);
		
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
		
		if (other instanceof DbLongLong)
			return ((DbLongLong)other).greaterThan(this);
		
		if (other instanceof DbLongLongLongLong)
			return ((DbLongLongLongLong)other).greaterThan(this);
		
		return false;
	}

	@Override
	public int hashCode()  {
		return (int)MurmurHash.hash(new byte[]{this.value});
	}
	
	@Override
	public long hashCodeL() {
		return MurmurHash.hash(new byte[]{this.value});
	}

	@Override
	public long hashCodeL(int position) {
		return MurmurHash.hash(new byte[]{this.value}, position);
	}
	
	@Override
	public String toString() {
		return Byte.toString(this.value);
	}
	
	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.BYTE; }

	@Override
	public DbByte copy() { return DbByte.create(this.value); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {	
		bytes[offset++] = this.value;
		return offset;
	}
	
	@Override
	public byte[] getBytes() {
		return new byte[]{this.value};
	}
	
	@Override
	public int getKey() { return this.value; }

	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(1, 1);
	}
	
	@Override
	public DbNumericType add(DbNumericType addend2) {
		return DbByte.create((byte) (this.value + ((DbByte)addend2).value));
	}
	
	@Override
	public DbNumericType subtract(DbNumericType subtrahend) {
		return DbByte.create((byte) (this.value - ((DbByte)subtrahend).value));
	}

	@Override
	public DbNumericType multiply(DbNumericType multiplier) {
		return DbByte.create((byte) (this.value * ((DbByte)multiplier).value));
	}

	@Override
	public DbNumericType divide(DbNumericType divisor) {
		if (((DbByte)divisor).value == 0)
			throw new InterpreterException("Divided by zero");		
		return DbDouble.create(((double)this.value) / (double)((DbByte)divisor).value);
	}
		
	@Override
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbByte)dbTypeObject).value); 
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
