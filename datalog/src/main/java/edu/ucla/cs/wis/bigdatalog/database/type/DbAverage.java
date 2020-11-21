package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbAverage extends DbNumericType implements EncodedType {
	private static final long serialVersionUID = 1L;

	private double runningSum;
	private long count;	

	private DbAverage(double runningSum, long count) {
		this.runningSum = runningSum;
		this.count = count;
	}

	public static DbAverage create(DbNumericType value) {
		return new DbAverage(((DbDouble)DataType.cast(value, DataType.DOUBLE)).getValue(), 1);
	}
	
	public static DbAverage create(DbNumericType value, long count) {
		return new DbAverage(((DbDouble)DataType.cast(value, DataType.DOUBLE)).getValue(), count);
	}
		
	public DbNumericType computeAverage() {
		return DbDouble.create(this.runningSum / this.count);
	}
	
	public DbAverage accrue(DbNumericType value) {
		this.runningSum += ((DbDouble)DataType.cast(value, DataType.DOUBLE)).getValue();
		this.count++;
		return this;
	}

	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;

		if (!(other instanceof DbAverage))
			return false;
		
		if (this == other)
			return true;
		
		if (other instanceof DbAverage) {
			DbAverage otherAverage = (DbAverage)other;
			return (this.count == otherAverage.count) 
					&& DbDouble.create(this.runningSum).equals(DbDouble.create(otherAverage.runningSum));					
		}

		return false;
	}
	
	@Override
	public boolean greaterThan(DbTypeBase other) {
		if (other == null)
			return false;

		if (!(other instanceof DbAverage))
			return false;
		
		if (this == other)
			return true;
		
		if (other instanceof DbAverage)
			return this.computeAverage().greaterThan(((DbAverage)other).computeAverage());					
				
		return false;
	}
	
	@Override
	public boolean lessThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof DbAverage))
			return false;
		
		if (this == other)
			return true;
		
		if (other instanceof DbAverage) 
			return this.computeAverage().lessThan(((DbAverage)other).computeAverage());
		
		return false;
	}

	@Override
	public int hashCode()  {
		return (int)this.hashCodeL();
	}
	
	@Override
	public long hashCodeL() {
		return MurmurHash.hash(this.getBytes());
	}

	@Override
	public long hashCodeL(int position) {
		return MurmurHash.hash(this.getBytes(), position);
	}
	
	@Override
	public String toString() {
		return "running sum: " + this.runningSum + " / count: " + this.count;
	}
	
	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.AVERAGE; }

	@Override
	public DbAverage copy() { return new DbAverage(this.runningSum, this.count); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {		
		offset = ByteArrayHelper.getDoubleAsBytes(this.runningSum, bytes, offset);
		offset = ByteArrayHelper.getLongAsBytes(this.count, bytes, offset);
		return offset;
	}
	
	@Override
	public byte[] getBytes() {
		byte[] bytes = new byte[8 + 8];
		ByteArrayHelper.getDoubleAsBytes(this.runningSum, bytes, 0);		
		ByteArrayHelper.getLongAsBytes(this.count, bytes, 8);
		return bytes;
	}
	
	@Override
	public int getKey() { return -1; }

	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(8 + 8, 8 + 8);
	}
	
	@Override
	public DbNumericType add(DbNumericType addend2) {
		return this.computeAverage().add((addend2));
	}
	
	@Override
	public DbNumericType subtract(DbNumericType subtrahend) {
		return this.computeAverage().subtract((subtrahend));
	}

	@Override
	public DbNumericType multiply(DbNumericType multiplier) {
		return this.computeAverage().multiply(multiplier);
	}

	@Override
	public DbNumericType divide(DbNumericType divisor) {
		return this.computeAverage().divide(divisor);
	}
		
	@Override
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this == dbTypeObject); 
	}
	
	@Override
	public DbDouble logarithm() {
		return this.computeAverage().logarithm();
	}

    @Override
    public DbDouble exponential() {
        return this.computeAverage().exponential();
    }

    @Override
    public DbInteger step() {
        return this.computeAverage().step();
    }
}
