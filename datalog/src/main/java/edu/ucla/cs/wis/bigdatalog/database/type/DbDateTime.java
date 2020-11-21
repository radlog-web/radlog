package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DbDateTime extends DbTypeBase implements BigEncodedType {

	private static final long serialVersionUID = 1L;
	public static SimpleDateFormat dateFormat; 
	public static SimpleDateFormat timeFormat;
	public static SimpleDateFormat dateTimeFormat;

	static {
		DeALSConfiguration deALSConfiguration = new DeALSConfiguration();
		dateFormat = new SimpleDateFormat(deALSConfiguration.getProperty("deals.dateformat"));
		timeFormat = new SimpleDateFormat(deALSConfiguration.getProperty("deals.timeformat"));
		dateTimeFormat = new SimpleDateFormat(dateFormat.toPattern() + " " + timeFormat.toPattern());
	}
	
	private long value; // Date.getTime()
	
	private DbDateTime(long value) {
		this.value = value;
	}
	
	private DbDateTime(Date date) {
		this.value = date.getTime();
	}
	
	public static DbDateTime create(long value) {
		return new DbDateTime(value);
	}
	
	public static DbDateTime create(Date date) {
		return new DbDateTime(date.getTime());
	}
	
	public static DbDateTime create(String value) {
		//example format: yyyy-MM-dd hh:mm:ss
		if (value == null || value.trim().length() == 0)
			return null;
		
		if (DbDateTime.dateFormat.toPattern().length() == value.length()) {
			try {
				return new DbDateTime(DbDateTime.dateFormat.parse(value));
			} catch (ParseException e) {
				return null;
			}
		} else if (DbDateTime.dateTimeFormat.toPattern().length() == value.length()) {
			try {
				return new DbDateTime(DbDateTime.dateTimeFormat.parse(value));
			} catch (ParseException e) {
				return null;
			}
		}
		
		return null;	
	}
	
	public Date getValue() {
		return new Date(this.value);
	}
	
	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;
		
		if (other instanceof DbDateTime)
			return (this.value == ((DbDateTime)other).value);
		
		return false;
	}

	@Override
	public boolean greaterThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof DbDateTime))
			return false;

		return (this.value > ((DbDateTime)other).value);
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

	public String toString() {
		SimpleDateFormat textFormat = new SimpleDateFormat(dateTimeFormat.toPattern() + " a");
		return textFormat.format(this.value); 
	}
	
	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.DATETIME; }	
	
	@Override
	public int getKey() { return -1; }

	@Override
	public long getKeyL() { return this.value; }
	
	@Override
	public DbDateTime copy() { return new DbDateTime(this.value); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {
		return ByteArrayHelper.getLongAsBytes(this.value, bytes, offset);		
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getLongAsBytes(this.value);
	}

	@Override
	public MemoryMeasurement getSizeOf() { 
		return new MemoryMeasurement(8, 8);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbDateTime)dbTypeObject).value); 
	}
}
