package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbFloat extends DbNumericType implements BigEncodedType {
	private static final long serialVersionUID = 1L;
	// APS 8/14/2013
	// floating point comparisons might not be exactly equal, especially after arithmetic operations
	// epsilon value arbitrarily picked
	private static final float epsilon = 0.000001f;
	
	public static final DbFloat ZERO = new DbFloat(0.0f);
	public static final DbFloat ONE = new DbFloat(1.0f);
	
	private float value;

	private DbFloat(float val) {
		this.value = val;
	}
	
	public static DbFloat create(float val) {
		if (val == 0.0) return ZERO;
		if (val == 1.0) return ONE;
		return new DbFloat(val);
	}
	
	private DbFloat(int val) {
		this.value = val;
	}
	
	public static DbFloat create(int val) {
		if (val == 0) return ZERO;
		if (val == 1) return ONE;
		return new DbFloat(val);
	}
	
	public static DbFloat load(int floatBits) {
		return DbFloat.create(Float.intBitsToFloat(floatBits));
	}
	
	public float getValue() { return this.value; }

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;

		if (other instanceof DbFloat)
			return (Math.abs(this.value - ((DbFloat)other).value)  < epsilon);

		if (other instanceof DbInteger)
			return (Math.abs(this.value - ((DbInteger)other).getValue())  < epsilon);

		if (other instanceof DbLong)
			return (Math.abs(this.value - ((DbLong)other).getValue())  < epsilon);
		
		if (other instanceof DbDouble)
			return (Math.abs(this.value - ((DbDouble)other).getValue())  < epsilon);

		if (other instanceof DbShort)
			return (Math.abs(this.value - ((DbShort)other).getValue())  < epsilon);
		
		if (other instanceof DbByte)
			return (Math.abs(this.value - ((DbByte)other).getValue())  < epsilon);
		
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

		if (other instanceof DbFloat)			
			return ((this.value - epsilon) > ((DbFloat)other).value);

		if (other instanceof DbInteger)
			return ((this.value - epsilon) > ((DbInteger)other).getValue());

		if (other instanceof DbLong)
			return ((this.value - epsilon) > ((DbLong)other).getValue());
		
		if (other instanceof DbDouble)
			return ((this.value - epsilon) > ((DbDouble)other).getValue());
		
		if (other instanceof DbShort)
			return ((this.value - epsilon) > ((DbShort)other).getValue());
		
		if (other instanceof DbByte)
			return ((this.value - epsilon) > ((DbByte)other).getValue());

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

		if (other instanceof DbFloat)			
			return ((this.value + epsilon) < ((DbFloat)other).value);

		if (other instanceof DbInteger)
			return ((this.value + epsilon) < ((DbInteger)other).getValue());

		if (other instanceof DbLong)
			return ((this.value - epsilon) < ((DbLong)other).getValue());

		if (other instanceof DbDouble)
			return ((this.value + epsilon) < ((DbDouble)other).getValue());
		
		if (other instanceof DbShort)
			return ((this.value + epsilon) < ((DbShort)other).getValue());
		
		if (other instanceof DbByte)
			return ((this.value + epsilon) < ((DbByte)other).getValue());
		
		if (other instanceof DbLongLong)
			return ((DbLongLong)other).greaterThan(this);
		
		if (other instanceof DbLongLongLongLong)
			return ((DbLongLongLongLong)other).greaterThan(this);
		
		//return ((this.value + epsilon) < other.value);
		return false;
	}

	public int hashCode()  {
		return Float.valueOf(this.value).hashCode();
	}

	public long hashCodeL() {
		// from javadocs for double
		//long bits = Double.doubleToLongBits(this.value);
		//return (bits ^ (bits >>> 32));
		return Float.floatToIntBits(this.value);
	}

	public long hashCodeL(int position) {
		return this.hashCodeL();
	}

	public String toString() {
		return Float.toString(this.value);
	}

	@Override
	public byte[] getBytes() {	
		return ByteArrayHelper.getFloatAsBytes(this.value);
	}

	@Override
	public int getBytes(byte[] bytes, int offset) {
		return ByteArrayHelper.getFloatAsBytes(this.value, bytes, offset);
	}

	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.FLOAT; }

	@Override
	public DbFloat copy() { return DbFloat.create(this.value); }

	@Override
	public int getKey() { return Float.floatToIntBits(this.value); }
	
	@Override
	public long getKeyL() { return Double.doubleToLongBits(this.value); }
	
	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(4, 4);
	}

	@Override
	public DbNumericType add(DbNumericType addend2) {
		return DbFloat.create(this.value + ((DbFloat)addend2).value);
	}

	@Override
	public DbNumericType subtract(DbNumericType subtrahend) {
		return DbFloat.create(this.value - ((DbFloat)subtrahend).value);
	}

	@Override
	public DbNumericType multiply(DbNumericType multiplier) {
		return DbFloat.create(this.value * ((DbFloat)multiplier).value);
	}

	@Override
	public DbNumericType divide(DbNumericType divisor) {
		float otherValue = ((DbFloat)divisor).value;
		if (otherValue == 0.0)
			throw new InterpreterException("Divided by zero");
    	return DbFloat.create(this.value / otherValue);
	}
	
	@Override	
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbFloat)dbTypeObject).value); 
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
