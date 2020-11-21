package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbInteger extends DbNumericType implements EncodedType {
	private static final long serialVersionUID = 1L;

	public static final int cacheSize = 2048;	
	public static DbInteger[] cache;
	
	// 2048 of the lowest positive integers
	static {
		cache = new DbInteger[cacheSize];
		for (int i = 0; i < cacheSize; i++)
			cache[i] = new DbInteger(i);
	}

	private int value;

	private DbInteger(int val) {
		this.value = val;
	}

	public static DbInteger create(int value) {
		if (value > -1 && value < cacheSize)
			return cache[value];
		
		return new DbInteger(value);
	}
	
	public int getValue() { return this.value; }
	
	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;

		if (other instanceof DbInteger)
			return (this.value == ((DbInteger)other).value);	 

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

		if (other instanceof DbInteger)
			return (this.value > ((DbInteger)other).value);
		
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
		
		if (other instanceof DbInteger)
			return (this.value < ((DbInteger)other).value);
		
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
		//return this.value;
		// APS 11/24/2013 - we had collisions with list creating with hashcodes
		return (int)MurmurHash.hash(ByteArrayHelper.getIntAsBytes(this.value));
		//return Integer.valueOf(this.value).hashCode();
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
	public DataType getDataType() { return DataType.INT; }

	@Override
	public DbInteger copy() { return DbInteger.create(this.value); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {	
		return ByteArrayHelper.getIntAsBytes(this.value, bytes, offset);
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getIntAsBytes(this.value);
	}
	
	@Override
	public int getKey() { return this.value; }

	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(4, 4);
	}
	
	@Override
	public DbNumericType add(DbNumericType addend2) {
		return DbInteger.create(this.value + ((DbInteger)addend2).value);
	}
	
	@Override
	public DbNumericType subtract(DbNumericType subtrahend) {
		return DbInteger.create(this.value - ((DbInteger)subtrahend).value);
	}

	@Override
	public DbNumericType multiply(DbNumericType multiplier) {
		return DbInteger.create(this.value * ((DbInteger)multiplier).value);
	}

	@Override
	public DbNumericType divide(DbNumericType divisor) {
		if (((DbInteger)divisor).value == 0)
			throw new InterpreterException("Divided by zero");		
		return DbDouble.create(((double)this.value) / (double)((DbInteger)divisor).value);
	}
	
	public DbInteger modulo(DbTypeBase right) {			
		// both must be integers to mod
		// left is dividend
		// right is divisor
		if (!(right instanceof DbInteger))
			throw new InterpreterException("Non-Integer passed to mod.");
		
		DbInteger otherRight = (DbInteger)right;
		
		if (otherRight.value == 0)
			throw new InterpreterException("0 passed to mod as divisor.");

		int hashInt = (this.value + "" + otherRight.value).hashCode();

		return DbInteger.create(hashInt);
	}

	public DbInteger opcom(DbNumericType right) {
		// both must be integers to mod
		// left is dividend
		// right is divisor
//		if (!(right instanceof DbInteger))
//			throw new InterpreterException("Non-Integer passed to opc.");
//
		int opc_left = ((DbInteger)DataType.cast(this, DataType.INT)).getValue();
		int opc_right = ((DbInteger)DataType.cast(right, DataType.INT)).getValue();

		return DbInteger.create(opc_left | opc_right);
//		(this.value << 16) | otherRight.value & 0xFFFF
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbInteger)dbTypeObject).value); 
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
