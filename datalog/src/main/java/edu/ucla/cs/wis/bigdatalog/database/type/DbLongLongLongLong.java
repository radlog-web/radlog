package edu.ucla.cs.wis.bigdatalog.database.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbLongLongLongLong extends DbNumericType implements ReallyBigEncodedType {
	private static final long serialVersionUID = 1L;
	private static final long LONG_MASK = 0xffffffffL;
	public static final int LENGTH = 32;
	
	private int zero;
	private int one;
	private int two;
	private int three;
	private int four;
	private int five;
	private int six;
	private int seven;
	
	private DbLongLongLongLong(byte[] value) {
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
		this.four =  ByteArrayHelper.getBytesAsInt(value, 16);
		this.five =  ByteArrayHelper.getBytesAsInt(value, 20);
		this.six =  ByteArrayHelper.getBytesAsInt(value, 24);
		this.seven =  ByteArrayHelper.getBytesAsInt(value, 28);
	}
	
	public static DbLongLongLongLong create(byte[] value) {
		return new DbLongLongLongLong(value);
	}
	
	private DbLongLongLongLong(int[] eightInts) {
		this.zero = (eightInts[0]);
		this.one = (eightInts[1]);
		this.two = (eightInts[2]);
		this.three = (eightInts[3]);
		this.four = (eightInts[4]);
		this.five = (eightInts[5]);
		this.six = (eightInts[6]);
		this.seven = (eightInts[7]);
	}
	
	private DbLongLongLongLong(int value) {
		this.seven = (int) (value & LONG_MASK);
		if (value < 0) {
			this.six = (int) LONG_MASK;
			this.five = (int) LONG_MASK;
			this.four = (int) LONG_MASK;
			this.three = (int) LONG_MASK;
			this.two = (int) LONG_MASK;
			this.one = (int) LONG_MASK;
			this.zero = (int) LONG_MASK;
		}
	}
	
	public static DbLongLongLongLong create(int value) {
		return new DbLongLongLongLong(value);
	}
	
	private DbLongLongLongLong(long value) {
		this.seven = (int) (value & LONG_MASK);
		this.six = (int) ((value >>> 32) & LONG_MASK);
		if (value < 0) {
			this.five = (int) LONG_MASK;
			this.four = (int) LONG_MASK;
			this.three = (int) LONG_MASK;
			this.two = (int) LONG_MASK;
			this.one = (int) LONG_MASK;
			this.zero = (int) LONG_MASK;
		}
	}
	
	public static DbLongLongLongLong create(long value) {
		return new DbLongLongLongLong(value);
	}
		
	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;

		if (this == other)
			return true;

		if (other instanceof DbLongLongLongLong) {
			DbLongLongLongLong otherLongLongLongLong = (DbLongLongLongLong)other;
			return ((this.zero == otherLongLongLongLong.zero) 
					&& (this.one == otherLongLongLongLong.one) 
					&& (this.two == otherLongLongLongLong.two) 
					&& (this.three == otherLongLongLongLong.three)
					&& (this.four == otherLongLongLongLong.four)
					&& (this.five == otherLongLongLongLong.five)
					&& (this.six == otherLongLongLongLong.six)
					&& (this.seven == otherLongLongLongLong.seven));
		} else if (other instanceof DbLongLong) {
			return (this.equals(new DbLongLongLongLong(((DbLongLong)other).getBytes())));
		} else if (other instanceof DbLong) {
			return (this.equals(new DbLongLongLongLong(((DbLong)other).getValue())));
		} else if (other instanceof DbInteger) {
			return (this.equals(new DbLongLongLongLong(((DbInteger)other).getValue())));
		} else if (other instanceof DbDouble) {
			return (this.equals(new DbLongLongLongLong((long)((DbDouble)other).getValue())));
		} else if (other instanceof DbFloat) {
			return (this.equals(new DbLongLongLongLong((int)((DbFloat)other).getValue())));
		} else if (other instanceof DbShort) {
			return (this.equals(new DbLongLongLongLong(((DbShort)other).getValue())));
		} else if (other instanceof DbByte) {
			return (this.equals(new DbLongLongLongLong(((DbByte)other).getValue())));
		}
		return false;		
	}
		
	@Override
	public boolean greaterThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (other instanceof DbLongLongLongLong) {
			DbLongLongLongLong otherLongLongLongLong = (DbLongLongLongLong)other;
			
			if (this.isPositive() && !otherLongLongLongLong.isPositive())
				return true;
			
			if (!this.isPositive() && otherLongLongLongLong.isPositive())
				return false;
			
			// if both negative, we want this to be smaller in binary
			if ((!this.isPositive()) && (!otherLongLongLongLong.isPositive()))
				return !doGreaterThan(otherLongLongLongLong, this);
			
			return doGreaterThan(this, otherLongLongLongLong);
		} else if (other instanceof DbLongLong) {
			return (this.greaterThan(new DbLongLongLongLong(((DbLongLong)other).getBytes())));
		} else if (other instanceof DbLong) {
			return (this.greaterThan(new DbLongLongLongLong(((DbLong)other).getValue())));
		} else if (other instanceof DbInteger) {
			return (this.greaterThan(new DbLongLongLongLong(((DbInteger)other).getValue())));
		} else if (other instanceof DbFloat) {
			return (this.greaterThan(new DbLongLongLongLong((int)((DbFloat)other).getValue())));
		} else if (other instanceof DbDouble) {
			return (this.greaterThan(new DbLongLongLongLong((long)((DbDouble)other).getValue())));
		} else if (other instanceof DbShort) {
			return (this.greaterThan(new DbLongLongLongLong(((DbShort)other).getValue())));
		} else if (other instanceof DbByte) {
			return (this.greaterThan(new DbLongLongLongLong(((DbByte)other).getValue())));
		}
		return false;
	}
	
	private boolean doGreaterThan(DbLongLongLongLong a, DbLongLongLongLong b) {
		if (a.zero != b.zero)
			return ((a.zero & LONG_MASK) > (b.zero & LONG_MASK));

		if (a.one != b.one)
			return ((a.one & LONG_MASK) > (b.one & LONG_MASK));
		
		if (a.two != b.two)
			return ((a.two & LONG_MASK) > (b.two & LONG_MASK));

		if (a.three != b.three)
			return ((a.three & LONG_MASK) > (b.three & LONG_MASK));
		
		if (a.four != b.four)
			return ((a.four & LONG_MASK) > (b.four & LONG_MASK));
		
		if (a.five != b.five)
			return ((a.five & LONG_MASK) > (b.five & LONG_MASK));
		
		if (a.six != b.six)
			return ((a.six & LONG_MASK) > (b.six & LONG_MASK));
		
		if (a.seven != b.seven)
			return ((a.seven & LONG_MASK) > (b.seven & LONG_MASK));
			
		return false;
	}
	
	@Override
	public boolean lessThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (other instanceof DbLongLongLongLong) {
			DbLongLongLongLong otherLongLongLongLong = (DbLongLongLongLong)other;
			
			if (this.isPositive() && !otherLongLongLongLong.isPositive())
				return false;
			
			if (!this.isPositive() && otherLongLongLongLong.isPositive())
				return true;
			
			// if both negative, we want this to be smaller in binary
			if ((!this.isPositive()) && (!otherLongLongLongLong.isPositive()))
				return !doLessThan(otherLongLongLongLong, this);
			
			return doLessThan(this, otherLongLongLongLong);
		} else if (other instanceof DbLongLong) {
			DbLongLong otherLongLong = (DbLongLong)other;
			return (this.lessThan(new DbLongLongLongLong(otherLongLong.getBytes())));
		} else if (other instanceof DbLong) {
			DbLong otherLong = (DbLong)other;
			return (this.lessThan(new DbLongLongLongLong(otherLong.getValue())));
		} else if (other instanceof DbInteger) {
			DbInteger otherInteger = (DbInteger)other;
			return (this.lessThan(new DbLongLongLongLong(otherInteger.getValue())));
		} else if (other instanceof DbFloat) {
			DbFloat otherFloat = (DbFloat)other;
			return (this.lessThan(new DbLongLongLongLong((long)otherFloat.getValue())));
		}
		return false;
	}
	
	private boolean doLessThan(DbLongLongLongLong a, DbLongLongLongLong b) {
		if (a.zero != b.zero)
			return ((a.zero & LONG_MASK) < (b.zero & LONG_MASK));

		if (a.one != b.one)
			return ((a.one & LONG_MASK) < (b.one & LONG_MASK));
		
		if (a.two != b.two)
			return ((a.two & LONG_MASK) < (b.two & LONG_MASK));

		if (a.three != b.three)
			return ((a.three & LONG_MASK) < (b.three & LONG_MASK));
		
		if (a.four != b.four)
			return ((a.four & LONG_MASK) < (b.four & LONG_MASK));
		
		if (a.five != b.five)
			return ((a.five & LONG_MASK) < (b.five & LONG_MASK));
		
		if (a.six != b.six)
			return ((a.six & LONG_MASK) < (b.six & LONG_MASK));
		
		if (a.seven != b.seven)
			return ((a.seven & LONG_MASK) < (b.seven & LONG_MASK));
			
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
	public DataType getDataType() { return DataType.LONGLONGLONGLONG; }
	
	@Override
	//public DbLongLongLongLong copy() { return new DbLongLongLongLong(this.value); }
	public DbLongLongLongLong copy() { return new DbLongLongLongLong(this.getBytes()); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {	
		offset = ByteArrayHelper.getIntAsBytes(this.zero, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.one, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.two, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.three, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.four, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.five, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.six, bytes, offset);
		offset = ByteArrayHelper.getIntAsBytes(this.seven, bytes, offset);
		return offset;
	}
	
	@Override
	public byte[] getBytes() {
		byte[] bytes = new byte[LENGTH];
		ByteArrayHelper.getIntAsBytes(this.zero, bytes, 0);
		ByteArrayHelper.getIntAsBytes(this.one, bytes, 4);
		ByteArrayHelper.getIntAsBytes(this.two, bytes, 8);
		ByteArrayHelper.getIntAsBytes(this.three, bytes, 12);
		ByteArrayHelper.getIntAsBytes(this.four, bytes, 16);
		ByteArrayHelper.getIntAsBytes(this.five, bytes, 20);
		ByteArrayHelper.getIntAsBytes(this.six, bytes, 24);
		ByteArrayHelper.getIntAsBytes(this.seven, bytes, 28);	
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
	}

	private boolean isPositive() {
		return (this.zero >= 0);
	}
	
	@Override
	public DbNumericType add(DbNumericType other) {
		DbLongLongLongLong otherLongLongLongLong = (DbLongLongLongLong)other;
		int[] result = new int[8];
        long sum = (this.seven & LONG_MASK) + (otherLongLongLongLong.seven & LONG_MASK);
        result[7] = (int)sum;
        sum = (this.six & LONG_MASK) + (otherLongLongLongLong.six & LONG_MASK) + (sum >>> 32);
        result[6] = (int)sum;
        sum = (this.five & LONG_MASK) + (otherLongLongLongLong.five & LONG_MASK) + (sum >>> 32);
        result[5] = (int)sum;
        sum = (this.four & LONG_MASK) + (otherLongLongLongLong.four & LONG_MASK) + (sum >>> 32);
        result[4] = (int)sum;
        sum = (this.three & LONG_MASK) + (otherLongLongLongLong.three & LONG_MASK) + (sum >>> 32);
        result[3] = (int)sum;
        sum = (this.two & LONG_MASK) + (otherLongLongLongLong.two & LONG_MASK) + (sum >>> 32);
        result[2] = (int)sum;
        sum = (this.one & LONG_MASK) + (otherLongLongLongLong.one & LONG_MASK) + (sum >>> 32);
        result[1] = (int)sum;
        sum = (this.zero & LONG_MASK) + (otherLongLongLongLong.zero & LONG_MASK) + (sum >>> 32);
        result[0] = (int)sum;

        return new DbLongLongLongLong(result);
	}	
	
	public DbLongLongLongLong add(long val) {
		int[] result = new int[8];
		int index = 7; // necessary for carry and copy operations
		// if not negative and highword empty
		int highWord = (int)(val >>> 32);
        if (highWord == 0) {
            result = new int[8];
            long sum = (this.seven & LONG_MASK) + val;
            result[7] = (int)sum;
            index = 6;
            
            boolean carry = (sum >>> 32 != 0);
            if (carry) {
                carry = ((result[6] = this.six + 1) == 0);
                index = 5;
                if (carry) {
                	carry = ((result[5] = this.five + 1) == 0);
                	index = 4;
                	if (carry) {
                		carry = ((result[4] = this.four + 1) == 0);
                		index = 3;
                		if (carry) {
                    		carry = ((result[3] = this.three + 1) == 0);
                    		index = 2;
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
                    	}
                	}                	
                }
            }
        } else {
        	result = new int[8];
            long sum = (this.seven & LONG_MASK) + (val & LONG_MASK);
            result[7] = (int)sum;
            index = 6;
            sum = (this.six & LONG_MASK) + (highWord & LONG_MASK) + (sum >>> 32);
            result[6] = (int)sum;
            index = 5;
            boolean carry = (sum >>> 32 != 0);
            if (carry) {
            	carry = ((result[5] = this.five + 1) == 0);
            	index = 4;
            	if (carry) {
            		carry = ((result[4] = this.four + 1) == 0);
            		index = 3;
            		if (carry) {
                		carry = ((result[3] = this.three + 1) == 0);
                		index = 2;
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
                	}
            	}
            }
        }
        
        if (index > -1) {
        	switch (index) {
        		case 7:
        			result[7] = this.seven;
        		case 6:
        			result[6] = this.six;
        		case 5:
        			result[5] = this.five;
        		case 4:
        			result[4] = this.four;
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
	
        return new DbLongLongLongLong(result);
	}
		
	@Override
	public DbNumericType subtract(DbNumericType other) {
		DbLongLongLongLong otherLLLL = (DbLongLongLongLong) other;
		
		BigInteger biThis = new BigInteger(this.getBytes());
		BigInteger biOther = new BigInteger(otherLLLL.getBytes());

		return new DbLongLongLongLong(biThis.subtract(biOther).toByteArray());
	}
	
	@Override
	public DbNumericType multiply(DbNumericType other) {
		DbLongLongLongLong otherLLLL = (DbLongLongLongLong) other;

		BigInteger biThis = new BigInteger(this.getBytes());
		BigInteger biOther = new BigInteger(otherLLLL.getBytes());

		return new DbLongLongLongLong(biThis.multiply(biOther).toByteArray());
	}
	
	@Override
	public DbNumericType divide(DbNumericType divisor) {
		DbLongLongLongLong otherLLLL = (DbLongLongLongLong) divisor;
		
		BigDecimal bdThis = new BigDecimal(new BigInteger(this.getBytes()));		
		BigDecimal bdOther = new BigDecimal(new BigInteger(otherLLLL.getBytes()));
		
		BigDecimal decimalResult = null;
		BigInteger integerResult = null;
		try {
			decimalResult = bdThis.divide(bdOther, 10, RoundingMode.HALF_UP);
		} catch (ArithmeticException ae) {
			try {
				integerResult = new BigInteger(this.getBytes()).divide(new BigInteger(otherLLLL.getBytes()));
			} catch (ArithmeticException ae2) {}
		}		
				
		// not an integer, so return a double - hope it fits...
		if (integerResult == null)
			return DbDouble.create(decimalResult.doubleValue());

		return new DbLongLongLongLong(integerResult.toByteArray());
	}
	
	public DbByte toDbByte() {
		return DbByte.create((byte) (this.seven & LONG_MASK));
	}
	
	public DbShort toDbShort() {
		return DbShort.create((short) (this.seven & LONG_MASK));
	}
	
	public DbInteger toDbInteger() {
		return DbInteger.create((int) (this.seven & LONG_MASK));
	}
	
	public DbLong toDbLong() {
		return DbLong.create(((this.six & LONG_MASK) << 32 | this.seven & LONG_MASK));
	}
	
	public DbFloat toDbFloat() {
		return DbFloat.create(new BigDecimal(new BigInteger(this.getBytes())).floatValue());
	}

	public DbDouble toDbDouble() {
		return DbDouble.create(new BigDecimal(new BigInteger(this.getBytes())).doubleValue());
	}
	
	public DbLongLong toDbLongLong() { 
		byte[] bytes = new byte[16];
		ByteArrayHelper.getIntAsBytes(this.four, bytes, 0);
		ByteArrayHelper.getIntAsBytes(this.five, bytes, 4);
		ByteArrayHelper.getIntAsBytes(this.six, bytes, 8);
		ByteArrayHelper.getIntAsBytes(this.seven, bytes, 12);	
		return DbLongLong.create(bytes);
	}
	
	public static DbLongLongLongLong getMaxLongLongLongLong() {
		return new DbLongLongLongLong(new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE});
	}
	
	public static DbLongLongLongLong getMinLongLongLongLong() {
		return new DbLongLongLongLong(new int[]{Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE});
	}
	
	public static byte[] getBytes(DbTypeBase value) {
		if (value instanceof DbLongLongLongLong)
			return value.getBytes();
		
		if (value instanceof DbLongLong)
			return value.getBytes();

		if (value instanceof DbInteger)
			return new DbLongLongLongLong(((DbInteger)value).getValue()).getBytes();
		
		return new DbLongLongLongLong(((DbLong)value).getValue()).getBytes();
	}	
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { return this.equals(dbTypeObject); }
	
	@Override
	public DbDouble logarithm() {
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
