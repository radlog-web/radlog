package edu.ucla.cs.wis.bigdatalog.database.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

/* APS 4/9/2014
 * For counting really big numbers upwards from 0.  Not for negative numbers*/
public class DbLongLong extends DbNumericType implements ReallyBigEncodedType {
	private static final long serialVersionUID = 1L;
	private static final long LONG_MASK = 0xffffffffL;
	public static final int LENGTH = 16;
	
	private int zero;
	private int one;
	private int two;
	private int three;
	
	private DbLongLong(byte[] value) {
		if (value.length < LENGTH) {
			byte padding = 0;
			if (value[0] < 0)
				padding = -1;
			
			byte[] temp = new byte[LENGTH];
			int len = LENGTH;
			if (value.length < len)
				len = value.length;
			
			// load lsb to msb
			for (int i = 0; i < len; i++)
				temp[LENGTH - (i + 1)] = value[value.length - (i + 1)];
			
			// now pad if space remaining
			if (len < LENGTH)
				for (int i = 0; i < LENGTH - len; i++)
					temp[i] = padding;
			value = temp;
		}
		
		this.zero = ByteArrayHelper.getBytesAsInt(value, 0);
		this.one =  ByteArrayHelper.getBytesAsInt(value, 4);
		this.two =  ByteArrayHelper.getBytesAsInt(value, 8);
		this.three =  ByteArrayHelper.getBytesAsInt(value, 12);
	}
	
	public static DbLongLong create(byte[] value) {
		return new DbLongLong(value);
	}
	
	private DbLongLong(int[] fourInts) {
		this.zero = (fourInts[0]);
		this.one = (fourInts[1]);
		this.two = (fourInts[2]);
		this.three = (fourInts[3]);
	}
	
	private DbLongLong(int value) {
		this.three  = (int) (value & LONG_MASK);
		if (value < 0) {
			this.two = (int) LONG_MASK;
			this.one = (int) LONG_MASK;
			this.zero = (int) LONG_MASK;
		}
	}
	
	public static DbLongLong create(int value) {
		return new DbLongLong(value);
	}
	
	private DbLongLong(long value) {
		this.three  = (int) (value & LONG_MASK);
		this.two = (int) ((value >>> 32) & LONG_MASK);
		if (value < 0) {
			this.one = (int) LONG_MASK;
			this.zero = (int) LONG_MASK;
		}
	}
	
	public static DbLongLong create(long value) {
		return new DbLongLong(value);
	}
	
	/*@Override
	public int getIntValue() {
		return (int) (this.three & LONG_MASK);
	}*/
	
	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;

		if (this == other)
			return true;

		if (other instanceof DbLongLong) {
			DbLongLong otherLongLong = (DbLongLong)other;
			return ((this.zero == otherLongLong.zero) 
					&& (this.one == otherLongLong.one) 
					&& (this.two == otherLongLong.two) 
					&& (this.three == otherLongLong.three));
		} else if (other instanceof DbLong) {
			return this.equals(DbLongLong.create(((DbLong)other).getValue()));
		} else if (other instanceof DbInteger) {
			return this.equals(DbLongLong.create(((DbInteger)other).getValue()));
		} else if (other instanceof DbDouble) {
			return this.equals(DbLongLong.create((long)((DbDouble)other).getValue()));			
		} else if (other instanceof DbFloat) {
			return this.equals(DbLongLong.create((int)((DbFloat)other).getValue()));
		} else if (other instanceof DbShort) {
			return this.equals(DbLongLong.create(((DbShort)other).getValue()));
		} else if (other instanceof DbByte) {
			return this.equals(DbLongLong.create(((DbByte)other).getValue()));
		}
		
		return false;		
	}
		
	@Override
	public boolean greaterThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (other instanceof DbLongLong) {
			DbLongLong otherLongLong = (DbLongLong)other;
			
			if (this.isPositive() && !otherLongLong.isPositive())
				return true;
			
			if (!this.isPositive() && otherLongLong.isPositive())
				return false;
			
			// if both negative, we want this to be smaller in binary
			if ((!this.isPositive()) && (!otherLongLong.isPositive()))
				return !doGreaterThan(otherLongLong, this);
			
			return doGreaterThan(this, otherLongLong);	
		} else if (other instanceof DbLong) {
			return (this.greaterThan(DbLongLong.create(((DbLong)other).getValue())));
		} else if (other instanceof DbInteger) {
			return (this.greaterThan(DbLongLong.create(((DbInteger)other).getValue())));
		} else if (other instanceof DbDouble) {
			return (this.greaterThan(DbLongLong.create((long)((DbDouble)other).getValue())));
		} else if (other instanceof DbFloat) {
			return (this.greaterThan(DbLongLong.create((int)((DbFloat)other).getValue())));
		} else if (other instanceof DbShort) {
			return (this.greaterThan(DbLongLong.create(((DbShort)other).getValue())));
		} else if (other instanceof DbByte) {
			return (this.greaterThan(DbLongLong.create(((DbByte)other).getValue())));
		}
		
		return false;
	}
	
	private boolean doGreaterThan(DbLongLong a, DbLongLong b) {
		if (a.zero != b.zero)
			return ((a.zero & LONG_MASK) > (b.zero & LONG_MASK));

		if (a.one != b.one)
			return ((a.one & LONG_MASK) > (b.one & LONG_MASK));
		
		if (a.two != b.two)
			return ((a.two & LONG_MASK) > (b.two & LONG_MASK));

		if (a.three != b.three)
			return ((a.three & LONG_MASK) > (b.three & LONG_MASK));
			
		return false;
	}
	
	@Override
	public boolean lessThan(DbTypeBase other) {
		if (other == null)
			return false;

		if (other instanceof DbLongLong) {
			DbLongLong otherLongLong = (DbLongLong)other;
			
			if (this.isPositive() && !otherLongLong.isPositive())
				return false;
			
			if (!this.isPositive() && otherLongLong.isPositive())
				return true;
			
			// if both negative, we want this to be smaller in binary
			if ((!this.isPositive()) && (!otherLongLong.isPositive()))
				return !doLessThan(otherLongLong, this);
			
			return doLessThan(this, otherLongLong);		
						
		} else if (other instanceof DbLong) {
			DbLong otherLong = (DbLong)other;
			return (this.lessThan(DbLongLong.create(otherLong.getValue())));
		} else if (other instanceof DbInteger) {
			DbInteger otherInteger = (DbInteger)other;
			return (this.lessThan(DbLongLong.create(otherInteger.getValue())));
		} else if (other instanceof DbFloat) {
			DbFloat otherFloat = (DbFloat)other;
			return (this.lessThan(DbLongLong.create((long)otherFloat.getValue())));
		}
		return false;
	}
	
	private boolean doLessThan(DbLongLong a, DbLongLong b) {
		if (a.zero != b.zero)
			return ((a.zero & LONG_MASK) < (b.zero & LONG_MASK));

		if (a.one != b.one)
			return ((a.one & LONG_MASK) < (b.one & LONG_MASK));
		
		if (a.two != b.two)
			return ((a.two & LONG_MASK) < (b.two & LONG_MASK));

		if (a.three != b.three)
			return ((a.three & LONG_MASK) < (b.three & LONG_MASK));
			
		return false;
	}
	
	@Override
	public int hashCode()  {	
		//return (int)MurmurHash.hash(this.value);
		return (int)MurmurHash.hash(this.getBytes());
	}
	
	@Override
	public long hashCodeL() {
		//return MurmurHash.hash(this.value);
		return MurmurHash.hash(this.getBytes());
	}

	@Override
	public long hashCodeL(int position) {
		//return MurmurHash.hash(this.value, position);
		return MurmurHash.hash(this.getBytes(), position);
	}

	@Override
	public String toString() {
		return new BigInteger(this.getBytes()).toString();
	}
	
	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.LONGLONG; }
	
	@Override
	public DbLongLong copy() { return DbLongLong.create(this.getBytes()); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {	
		offset = ByteArrayHelper.getIntAsBytes(this.zero, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.one, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.two, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.three, bytes, offset);
		return offset;
	}
	
	@Override
	public byte[] getBytes() {
		byte[] bytes = new byte[16];
		ByteArrayHelper.getIntAsBytes(this.zero, bytes, 0);
		ByteArrayHelper.getIntAsBytes(this.one, bytes, 4);
		ByteArrayHelper.getIntAsBytes(this.two, bytes, 8);
		ByteArrayHelper.getIntAsBytes(this.three, bytes, 12);
		return bytes;
	}
	
	@Override
	public int getKey() { return -1; }
	
	@Override
	public long getKeyL() { return -1; }

	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(LENGTH, LENGTH);
	}

	@Override
	public byte[] getKeyRB() {
		return this.getBytes();
		//return this.value; 
	}

	private boolean isPositive() {
		return (this.zero >= 0);
		//return this.greaterThanOrEqualsTo(DbInteger.create(0));
	}
	
	@Override
	public DbNumericType add(DbNumericType other) {  
		DbLongLong otherLongLong = (DbLongLong)other;
    	int[] result = new int[4];
        long sum = (this.three & LONG_MASK) + (otherLongLong.three & LONG_MASK);
        result[3] = (int)sum;
        sum = (this.two & LONG_MASK) + (otherLongLong.two & LONG_MASK) + (sum >>> 32);
        result[2] = (int)sum;
        sum = (this.one & LONG_MASK) + (otherLongLong.one & LONG_MASK) + (sum >>> 32);
        result[1] = (int)sum;
        sum = (this.zero & LONG_MASK) + (otherLongLong.zero & LONG_MASK) + (sum >>> 32);
        result[0] = (int)sum;

        return new DbLongLong(result);
	}	
	
	public DbLongLong add(long val) {
		int[] result = new int[4];
		int index = 3; // necessary for carry and copy operations
		// if not negative and highword empty
		int highWord = (int)(val >>> 32);
        if (highWord == 0) {
            result = new int[4];
            long sum = (this.three & LONG_MASK) + val;
            result[3] = (int)sum;
            index = 2;
            
            boolean carry = (sum >>> 32 != 0);
            if (carry) {
                carry = ((result[2] = this.two + 1) == 0);
                index = 1;
                if (carry) {
                	carry = ((result[1] = this.one + 1) == 0);
                	index = 0;
                	if (carry) {
                		carry = ((result[0] = this.zero + 1) == 0);
                		index = -1;
                	}                	
                }
            }
        } else {
        	result = new int[4];
            long sum = (three & LONG_MASK) + (val & LONG_MASK);
            result[3] = (int)sum;
            index = 2;
            sum = (two & LONG_MASK) + (highWord & LONG_MASK) + (sum >>> 32);
            result[2] = (int)sum;
            index = 1;
            boolean carry = (sum >>> 32 != 0);
            if (carry) {
            	carry = ((result[1] = this.one + 1) == 0);
            	index = 0;
            	if (carry) {
            		carry = ((result[0] = this.zero + 1) == 0);
            		index = -1;
            	}
            }
        }
        
        if (index > -1) {
        	switch (index) {
        		case 3:
        			result[3] = this.three;
        		case 2:
        			result[2] = this.two;
        		case 1:
        			result[1] = this.one;
        		case 0:
        			result[0] = this.zero;        		
        	}
        }
	
        return new DbLongLong(result);
	}
	
	@Override
	public DbNumericType subtract(DbNumericType other) {
		DbLongLong otherLL = (DbLongLong) other;
		BigInteger biThis = new BigInteger(this.getBytes());
		BigInteger biOther = new BigInteger(otherLL.getBytes());

		return DbLongLong.create(biThis.subtract(biOther).toByteArray());
	}
	
	@Override
	public DbNumericType multiply(DbNumericType other) {
		DbLongLong otherLL = (DbLongLong) other;
		BigInteger biThis = new BigInteger(this.getBytes());
		BigInteger biOther = new BigInteger(otherLL.getBytes());

		return DbLongLong.create(biThis.multiply(biOther).toByteArray());
	}
	
	@Override
	public DbNumericType divide(DbNumericType divisor) {
		DbLongLong otherLL = (DbLongLong) divisor;

		BigDecimal bdThis = new BigDecimal(new BigInteger(this.getBytes()));
		BigDecimal bdOther = new BigDecimal(new BigInteger(otherLL.getBytes()));
	
		BigDecimal decimalResult = null;
		BigInteger integerResult = null;
		try {
			decimalResult = bdThis.divide(bdOther, 10, RoundingMode.HALF_UP);
		} catch (ArithmeticException ae) {
			try {
				integerResult = new BigInteger(this.getBytes()).divide(new BigInteger(otherLL.getBytes()));
			} catch (ArithmeticException ae2) {}
		}		
		
		// not an integer, so return a float - hope it fits...
		if (integerResult == null)
			return DbDouble.create(decimalResult.doubleValue());

		return DbLongLong.create(integerResult.toByteArray());
	}
	
	public DbByte toDbByte() {
		return DbByte.create((byte) (this.three & LONG_MASK));
	}
	
	public DbShort toDbShort() {
		return DbShort.create((short) (this.three & LONG_MASK));
	}
	
	public DbInteger toDbInteger() {
		return DbInteger.create((int) (this.three & LONG_MASK));
	}
	
	public DbLong toDbLong() {
		return DbLong.create(((this.two & LONG_MASK) << 32 | this.three & LONG_MASK));
	}	
	
	public DbFloat toDbFloat() {
		return DbFloat.create(new BigDecimal(new BigInteger(this.getBytes())).floatValue());
	}
	
	public DbDouble toDbDouble() {
		return DbDouble.create(new BigDecimal(new BigInteger(this.getBytes())).doubleValue());
	}

	public static DbLongLong getMaxLongLong() {
		return new DbLongLong(new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE});
	}
	
	public static DbLongLong getMinLongLong() {
		return new DbLongLong(new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE});
	}
	
	public static byte[] getBytes(DbTypeBase value) {
		if (value instanceof DbLongLong)
			return value.getBytes();

		if (value instanceof DbLongLongLongLong)
			return value.getBytes();

		if (value instanceof DbInteger)
			return DbLongLong.create(((DbInteger)value).getValue()).getBytes();
		
		return DbLongLong.create(((DbLong)value).getValue()).getBytes();
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { return this.equals(dbTypeObject); }

	@Override
	public DbDouble logarithm() {
		//int base = 10;
		//BigInteger val = new BigInteger(this.getBytes());		
		//return DbFloat.create(val.bitLength() / (Math.log(base) / Math.log(2)));
		// APS 10/10/2014 - this will truncate - a temporary fix, that will likely be permanent
		return DbDouble.create(java.lang.Math.log10(new BigInteger(this.getBytes()).doubleValue()));
	}

    @Override
    public DbDouble exponential() {
        return DbDouble.create(java.lang.Math.exp(new BigInteger(this.getBytes()).doubleValue()));
    }

    @Override
    public DbInteger step() {
        return DbInteger.create(DbNumericType.step_impl(new BigInteger(this.getBytes()).doubleValue()));
    }
}
