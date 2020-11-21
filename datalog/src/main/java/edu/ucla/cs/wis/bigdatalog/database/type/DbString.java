package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbString extends DbTypeBase implements EncodedType {

	private static final long serialVersionUID = 1L;
	private int value;
	private String strValue;
	
	private DbString(int value) {
		this.value = value;
	}
	
	private DbString(int value, String strValue) {
		this.value = value;
		this.strValue = strValue;
	}
	
	public static DbString load(int id, TypeManager typeManager) {
		DbString string = new DbString(id);
		string.load(typeManager);
		return string;
	}
	
	public static DbString load(int id, String value) {
		return new DbString(id, value);
	}
	
	public void load(TypeManager typeManager) {
		this.strValue = typeManager.getString(this.value);
	}
		
	public String getValue() {  return this.strValue; }

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;
		
		if (other instanceof DbString)
			return (this.value == ((DbString)other).value);
		
		return false;
	}

	@Override
	public boolean greaterThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof DbString))
			return false;
		
		return (this.toString().compareTo(other.toString()) > 0);
	}
	
	public int hashCode() {
		// APS 11/24/2013 - we had collisions with list creating with hashcodes
		//return (int)MurmurHash.hash(ByteArrayHelper.getIntAsBytes(this.value));
		//return this.value;
		//this.loadString();
		return this.strValue.hashCode();
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
	public String toString() {  return this.strValue; }
	
	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.STRING; }	
	
	@Override
	public int getKey() { return this.value; }
	
	@Override
	public DbString copy() { return new DbString(this.value, this.strValue); }
	
	@Override
	public int getBytes(byte[] bytes, int offset) {
		return ByteArrayHelper.getIntAsBytes(this.value, bytes, offset);		
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getIntAsBytes(this.value);
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		// count the integer and the length of the string in bytes as both used and allocated
		int used = 4 + this.getValue().length(); 
		return new MemoryMeasurement(used, used);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { 
		return (this.value == ((DbString)dbTypeObject).value); 
	}
}
