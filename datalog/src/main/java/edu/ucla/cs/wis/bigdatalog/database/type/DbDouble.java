package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbDouble extends DbNumericType implements BigEncodedType {
	private static final long serialVersionUID = 1L;
	// APS 8/14/2013
	// floating point comparisons might not be exactly equal, especially after arithmetic operations
	// epsilon value arbitrarily picked
	public static final double epsilon = 0.000001;
	
	public static final DbDouble ZERO = new DbDouble(0.0);
	public static final DbDouble ONE = new DbDouble(1.0);
	
	private double value;

	private DbDouble(double val) {
		this.value = val;
	}
	
	public static DbDouble create(double val) {
		if (val == 0.0) return ZERO;
		if (val == 1.0) return ONE;
		return new DbDouble(val);
	}
	
	private DbDouble(int val) {
		this.value = val;
	}
	
	public static DbDouble create(int val) {
		if (val == 0) return ZERO;
		if (val == 1) return ONE;
		return new DbDouble(val);
	}
	
	public static DbDouble load(long val) {
		return DbDouble.create(Double.doubleToLongBits(val));
	}
	
	public double getValue() { return this.value; }

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;

		if (other instanceof DbDouble)
			return (Math.abs(this.value - ((DbDouble)other).value)  < epsilon);

		if (other instanceof DbInteger)
			return (Math.abs(this.value - ((DbInteger)other).getValue())  < epsilon);

		if (other instanceof DbLong)
			return (Math.abs(this.value - ((DbLong)other).getValue())  < epsilon);

		if (other instanceof DbFloat)
			return (Math.abs(this.value - ((DbFloat)other).getValue())  < epsilon);
		
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

		if (other instanceof DbDouble)			
			return ((this.value - epsilon) > ((DbDouble)other).value);

		if (other instanceof DbInteger)
			return ((this.value - epsilon) > ((DbInteger)other).getValue());

		if (other instanceof DbLong)
			return ((this.value - epsilon) > ((DbLong)other).getValue());

		if (other instanceof DbFloat)
			return ((this.value - epsilon) > ((DbFloat)other).getValue());

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

		if (other instanceof DbDouble)			
			return ((this.value + epsilon) < ((DbDouble)other).value);

		if (other instanceof DbInteger)
			return ((this.value + epsilon) < ((DbInteger)other).getValue());

		if (other instanceof DbLong)
			return ((this.value - epsilon) < ((DbLong)other).getValue());

		if (other instanceof DbFloat)			
			return ((this.value + epsilon) < ((DbFloat)other).getValue());
		
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
		return Double.valueOf(this.value).hashCode();
	}

	public long hashCodeL() {
		// from javadocs for double
		long bits = Double.doubleToLongBits(value);
		return (bits ^ (bits >>> 32));
	}

	public long hashCodeL(int position) {
		return this.hashCodeL();
	}

	public String toString() {
		return Double.toString(this.value);
	}

	@Override
	public byte[] getBytes() {	
		return ByteArrayHelper.getDoubleAsBytes(this.value);
	}

	@Override
	public int getBytes(byte[] bytes, int offset) {	
		return ByteArrayHelper.getDoubleAsBytes(this.value, bytes, offset);
	}

	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.DOUBLE; }

	@Override
	public DbDouble copy() { return DbDouble.create(this.value); }

	@Override
	public int getKey() { return -1; }
	
	@Override
	public long getKeyL() { return Double.doubleToLongBits(this.value); }
	
	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(8,8);
	}

	@Override
	public DbNumericType add(DbNumericType addend2) {
		return DbDouble.create(this.value + ((DbDouble)addend2).value);
	}

	@Override
	public DbNumericType subtract(DbNumericType subtrahend) {
		return DbDouble.create(this.value - ((DbDouble)subtrahend).value);
	}

	@Override
	public DbNumericType multiply(DbNumericType multiplier) {
		return DbDouble.create(this.value * ((DbDouble)multiplier).value);
	}

	@Override
	public DbNumericType divide(DbNumericType divisor) {
		double otherValue = ((DbDouble)divisor).value;
		if (otherValue == 0.0)
			throw new InterpreterException("Divided by zero");
    	return DbDouble.create(this.value / otherValue);
	}
	
	@Override	
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbDouble)dbTypeObject).value); 
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
