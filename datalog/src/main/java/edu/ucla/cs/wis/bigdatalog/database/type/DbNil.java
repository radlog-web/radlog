package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbNil extends DbTypeBase {
	private static final long serialVersionUID = 1L;

	public DbNil() {}
	
	@Override
	public DataType getDataType() { return DataType.UNKNOWN; }

	@Override
	public boolean isConstant() { return false; }

	@Override
	public boolean equals(Object other) {
		if (other == null)
			return false;
		
		return (this == other); 
	}

	@Override
	public boolean greaterThan(DbTypeBase other) { return false; }
	
	@Override
	public boolean lessThan(DbTypeBase other) { return false; }

	@Override
	public int hashCode() { return 0; }

	@Override
	public long hashCodeL() { return 0; }
	
	@Override
	public long hashCodeL(int position) { return 0; }
	
	@Override
	public String toString() { return "nil"; }

	@Override
	public DbNil copy() { return this; }
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) { return this; }

	@Override
	public int getBytes(byte[] bytes, int offset) { return offset+4; }
	
	@Override
	public byte[] getBytes() { return null; }
	
	@Override
	public MemoryMeasurement getSizeOf() {
		return new MemoryMeasurement(0,0);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { return this.equals(dbTypeObject); }
}
