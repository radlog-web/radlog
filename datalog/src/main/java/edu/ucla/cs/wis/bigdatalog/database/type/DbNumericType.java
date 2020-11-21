package edu.ucla.cs.wis.bigdatalog.database.type;

import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.type.DataType;

public abstract class DbNumericType extends DbTypeBase {
	private static final long serialVersionUID = 1L;

	public DbNumericType() {}
	
	public DbNumericType convertTo(DataType toDataType) {
		return this.castWider(toDataType);
	}
	
	public DbNumericType convertToAllowNarrowing(DataType toDataType) {
		if (DataType.isPromotable(this.getDataType(), toDataType))
			return this.castWider(toDataType);
		return this.cast(toDataType);
	}
	
	private DbNumericType castWider(DataType toDataType) {
		DataType fromDataType = this.getDataType();
		// the assumption in calling this method, is that we are casting from a lower to a higher order type
		switch (toDataType) {
			case INT:
				switch (fromDataType) {
					case INT:
						return this;
					case SHORT:
						return DbInteger.create(((DbShort)this).getValue());
					case BYTE:
						return DbInteger.create(((DbByte)this).getValue());
				}
				break;
			case LONG:
				switch (fromDataType) {
					case LONG:
						return this;
					case INT:
						return DbLong.create(((DbInteger)this).getValue());
					case SHORT:
						return DbLong.create(((DbShort)this).getValue());
					case BYTE:
						return DbLong.create(((DbByte)this).getValue());
				}
				break;
			case LONGLONG:
				switch (fromDataType) {
					case LONGLONG:
						return this;
					case LONG:
						return DbLongLong.create(((DbLong)this).getValue());
					case INT:
						return DbLongLong.create(((DbInteger)this).getValue());
					case SHORT:
						return DbLongLong.create(((DbShort)this).getValue());
					case BYTE:
						return DbLongLong.create(((DbByte)this).getValue());
				}
				break;
			case LONGLONGLONGLONG:
				switch (fromDataType) {
					case LONGLONGLONGLONG:
						return this;
					case INT:
						return DbLongLongLongLong.create(((DbInteger)this).getValue());				
					case LONG:
						return DbLongLongLongLong.create(((DbLong)this).getValue());
					case LONGLONG:
						return DbLongLongLongLong.create(((DbLongLong)this).getBytes());
					case SHORT:
						return DbLongLongLongLong.create(((DbShort)this).getValue());
					case BYTE:
						return DbLongLongLongLong.create(((DbByte)this).getValue());
				}
				break;
			case FLOAT:
				switch (fromDataType) {
					case FLOAT:
						return this;
					case INT:
						return DbFloat.create(((DbInteger)this).getValue());
					case LONG:
						return DbFloat.create(((DbLong)this).getValue());
					case LONGLONG:
						return ((DbLongLong)this).toDbFloat();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)this).toDbFloat();
					case SHORT:
						return DbFloat.create(((DbShort)this).getValue());
					case BYTE:
						return DbFloat.create(((DbByte)this).getValue());
				}
				break;
			case DOUBLE:
				switch (fromDataType) {
					case DOUBLE:
						return this;
					case INT:
						return DbDouble.create(((DbInteger)this).getValue());
					case LONG:
						return DbDouble.create(((DbLong)this).getValue());
					case LONGLONG:
						return ((DbLongLong)this).toDbDouble();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)this).toDbDouble();
					case SHORT:
						return DbDouble.create(((DbShort)this).getValue());
					case BYTE:
						return DbDouble.create(((DbByte)this).getValue());
				}
				break;
			case SHORT:
				if (fromDataType == DataType.SHORT)
					return this;
				else if (fromDataType == DataType.BYTE)
					return DbShort.create(((DbByte)this).getValue());
				break;
			case BYTE:
				if (fromDataType == DataType.BYTE)
					return this;
				break;

		}
		return null;
	}
	
	private DbNumericType cast(DataType toDataType) {
		DataType fromDataType = this.getDataType();
		// the assumption in calling this method, is that we are casting from a lower to a higher order type
		switch (toDataType) {
			case INT:
				switch (fromDataType) {
					case INT:
						return this;
					case LONG:
						return DbInteger.create((int)((DbLong)this).getValue());
					case DOUBLE:
						return DbInteger.create((int) ((DbDouble)this).getValue());
					case FLOAT:
						return DbInteger.create((int) ((DbFloat)this).getValue());
					case LONGLONG:
						return ((DbLongLong)this).toDbInteger();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)this).toDbInteger();
					case SHORT:
						return DbInteger.create(((DbShort)this).getValue());
					case BYTE:
						return DbInteger.create(((DbByte)this).getValue());
				}
				break;
			case LONG:
				switch (fromDataType) {
					case LONG:
						return this;
					case INT:
						return DbLong.create(((DbInteger)this).getValue());
					case DOUBLE:
						return DbLong.create((long) ((DbDouble)this).getValue());
					case FLOAT:
						return DbLong.create((long) ((DbFloat)this).getValue());
					case LONGLONG:
						return ((DbLongLong)this).toDbLong();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)this).toDbLong();
					case SHORT:
						return DbLong.create(((DbShort)this).getValue());
					case BYTE:
						return DbLong.create(((DbByte)this).getValue());
				}
				break;
			case LONGLONG:
				switch (fromDataType) {
					case LONGLONG:
						return this;
					case LONG:
						return DbLongLong.create(((DbLong)this).getValue());
					case INT:
						return DbLongLong.create(((DbInteger)this).getValue());
					case LONGLONGLONGLONG:
						byte[] bytes = ((DbLongLongLongLong)this).getBytes();
						return DbLongLong.create(Arrays.copyOfRange(bytes,  16, bytes.length));
					case DOUBLE:
						return DbLongLong.create((long) ((DbDouble)this).getValue());
					case FLOAT:
						return DbLongLong.create((long) ((DbFloat)this).getValue());
					case SHORT:
						return DbLongLong.create(((DbShort)this).getValue());
					case BYTE:
						return DbLongLong.create(((DbByte)this).getValue());
				}
				break;
			case LONGLONGLONGLONG:
				switch (fromDataType) {
					case LONGLONGLONGLONG:
						return this;
					case INT:
						return DbLongLongLongLong.create(((DbInteger)this).getValue());				
					case LONG:
						return DbLongLongLongLong.create(((DbLong)this).getValue());
					case LONGLONG:
						return DbLongLongLongLong.create(((DbLongLong)this).getBytes());
					case DOUBLE:
						return DbLongLongLongLong.create((long) ((DbDouble)this).getValue());
					case FLOAT:
						return DbLongLongLongLong.create((long) ((DbFloat)this).getValue());
					case SHORT:
						return DbLongLongLongLong.create(((DbShort)this).getValue());
					case BYTE:
						return DbLongLongLongLong.create(((DbByte)this).getValue());
				}
				break;
			case FLOAT:
				switch (fromDataType) {
					case FLOAT:
						return this;
					case INT:
						return DbFloat.create(((DbInteger)this).getValue());
					case LONG:
						return DbFloat.create(((DbLong)this).getValue());
					case LONGLONG:
						return ((DbLongLong)this).toDbFloat();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)this).toDbFloat();
					case DOUBLE:
						return DbFloat.create((float) ((DbDouble)this).getValue());
					case SHORT:
						return DbFloat.create(((DbShort)this).getValue());
					case BYTE:
						return DbFloat.create(((DbByte)this).getValue());
				}
				break;
			case DOUBLE:
				switch (fromDataType) {
					case DOUBLE:
						return this;
					case INT:
						return DbDouble.create(((DbInteger)this).getValue());
					case LONG:
						return DbDouble.create(((DbLong)this).getValue());
					case LONGLONG:
						return ((DbLongLong)this).toDbDouble();
					case LONGLONGLONGLONG:
						return ((DbLongLongLongLong)this).toDbDouble();
					case FLOAT:
						return DbDouble.create(((DbFloat)this).getValue());
					case SHORT:
						return DbDouble.create(((DbShort)this).getValue());
					case BYTE:
						return DbDouble.create(((DbByte)this).getValue());
				}
				break;
			case SHORT:
				switch (fromDataType) {
					case SHORT:
						return this;
					case INT:
						return DbShort.create((short) ((DbInteger)this).getValue());
					case LONG:
						return DbShort.create((short) ((DbLong)this).getValue());
					case LONGLONG:
						return DbShort.create((short) ((DbLongLong)this).toDbInteger().getValue());
					case LONGLONGLONGLONG:
						return DbShort.create((short) ((DbLongLongLongLong)this).toDbInteger().getValue());
					case FLOAT:
						return DbShort.create((short) ((DbFloat)this).getValue());
					case DOUBLE:
						return DbShort.create((short) ((DbDouble)this).getValue());
					case BYTE:
						return DbShort.create(((DbByte)this).getValue());
				}
				break;
			case BYTE:
				switch (fromDataType) {
					case BYTE:
						return this;
					case INT:
						return DbByte.create((byte) ((DbInteger)this).getValue());
					case LONG:
						return DbByte.create((byte) ((DbLong)this).getValue());
					case LONGLONG:
						return DbByte.create((byte) ((DbLongLong)this).toDbInteger().getValue());
					case LONGLONGLONGLONG:
						return DbByte.create((byte) ((DbLongLongLongLong)this).toDbInteger().getValue());
					case FLOAT:
						return DbByte.create((byte) ((DbFloat)this).getValue());
					case DOUBLE:
						return DbByte.create((byte) ((DbDouble)this).getValue());
					case SHORT:
						return DbByte.create((byte) ((DbShort)this).getValue());
				}
				break;
		}
		return null;
	}

	// implementor is addend1
	abstract public DbNumericType add(DbNumericType addend2);
		
	// implementor is minuend
	abstract public DbNumericType subtract(DbNumericType subtrahend);
	
	// implementor is multiplicand
	abstract public DbNumericType multiply(DbNumericType multiplier);
	
	// implementor is dividend
	abstract public DbNumericType divide(DbNumericType divisor);

	abstract public DbDouble logarithm();

	abstract public DbDouble exponential();

    abstract public DbInteger step();

    static Integer step_impl(double val) {
        if (Double.doubleToRawLongBits(val) < 0) // The way to judge whether double is negative
            return 0;
        else
            return 1;
    }
}
