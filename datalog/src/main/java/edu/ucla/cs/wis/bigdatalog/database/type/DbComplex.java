package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.type.ComplexStorageManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbComplex extends DbTypeBase implements EncodedType {
	private static final long serialVersionUID = 1L;
	private DbString 		name;		// Name of the complex object.
	private DbTypeBase[] 	arguments;
	private int 			key;
	
	private DbComplex(int key) {
		this.key = key;
	}
	
	private DbComplex(int key, DbString name, DbTypeBase[] arguments) {
		this.key = key;
		this.name = name;
		this.arguments = arguments;
	}
	
	public static DbComplex load(int id, TypeManager typeManager) {
		DbComplex complex = new DbComplex(id);
		complex.load(typeManager);
		return complex;
	}
	
	public static DbComplex load(int id, DbString name, DbTypeBase[] arguments) {
		return new DbComplex(id, name, arguments);		
	}
	
	public int getArity() { return this.arguments.length; }

	public DbString getName() { return this.name; }

	public DbTypeBase getArgument(int position) { return this.arguments[position]; }
	
	public DbTypeBase[] getArguments() { return this.arguments; }

	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.COMPLEX; }
	
	@Override
	public int getKey() { return this.key; }
	
	@Override
	public int hashCode() {
		//this.loadAttributes();
		
		//final int prime = 31;
		//int result = 1;
		//for (int i = 0; i < this.arguments.length; i++)
		//	result = (int)MurmurHash.hash(ByteArrayHelper.getIntAsBytes(prime * result + this.arguments[i].hashCode()));
				
		int numberOfBytes = this.arguments.length * 4;
		
		if (this.name != null && this.name.getValue().length() > 0)
			numberOfBytes += 4;

		// for extra addition step
		//numberOfBytes += 4;
		
		int offset = 0;
		byte[] bytes = new byte[numberOfBytes];
		
		if (this.name != null && this.name.getValue().length() > 0)
			offset = ByteArrayHelper.getIntAsBytes(this.name.hashCode(), bytes, offset);
		
		int hashCode;
		//int totalHashCode = 0;
		for (int i = 0; i < this.arguments.length; i++) {
			hashCode = this.arguments[i].hashCode();
			//totalHashCode += hashCode;
			offset = ByteArrayHelper.getIntAsBytes(hashCode, bytes, offset);			
		}
		
		//offset = ByteArrayHelper.getIntAsBytes(totalHashCode, bytes, offset);
		
		return (int) MurmurHash.hash(bytes);

		//result = prime * result + Arrays.hashCode(this.arguments);
		//result = prime * result + ((name == null) ? 0 : name.hashCode());
		//return result;
		
		//return (int)MurmurHash.hash(ByteArrayHelper.getIntAsBytes(result));
	}
	
	public long hashCodeL() {
		return hashCodeL(0);
	}
	
	public long hashCodeL(int position) {
		//this.loadAttributes();
		
		int numberOfBytes = this.arguments.length * 8;
		
		if (this.name != null && this.name.getValue().length() > 0)
			numberOfBytes += 8;
		
		int offset = 0;
		byte[] bytes = new byte[numberOfBytes];
		
		if (this.name != null && this.name.getValue().length() > 0)
			offset = ByteArrayHelper.getLongAsBytes(this.name.hashCodeL(), bytes, offset);
		
		for (int i = 0; i < this.arguments.length; i++)
			offset = ByteArrayHelper.getLongAsBytes(this.arguments[i].hashCodeL(), bytes, offset);
				
		return MurmurHash.hash(bytes, position);
		
		// take hash of each pair
		// so hash 0 and 1
		// then hash (hash 0 and 1) with 2, etc
		//long hashCode = 0;
		/*for (int i = 0; i < this.arguments.length; i++) {			
			if (i > 0) {
				byte[] argBytes = new byte[16];
				int argOffset = ByteArrayHelper.getLongAsBytes(hashCode, argBytes, 0);
				ByteArrayHelper.getLongAsBytes(this.arguments[i].hashCodeL(1), argBytes, argOffset);
				hashCode = MurmurHash.hash(argBytes);
			} else {
				hashCode = this.arguments[i].hashCodeL();
			}
			//offset = ByteArrayHelper.getLongAsBytes(this.arguments[i].hashCodeL(), bytes, offset);
			offset = ByteArrayHelper.getLongAsBytes(hashCode, bytes, offset);
		}*/

	}
	
	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
		
		if (this == other)
			return true;
		
		if (!(other instanceof DbComplex))
			return false;
		
		DbComplex otherComplex = (DbComplex)other;
		return this.key == otherComplex.key;	
	}

	@Override
	public boolean greaterThan(DbTypeBase other)  {
		if (other == null)
			return false;
		
		if (!(other instanceof DbComplex))
			return false;
		
		//this.loadAttributes();
		DbComplex otherComplex = (DbComplex)other;
		//otherComplex.loadAttributes();
		//return this.key > otherComplex.key;

		if (this.name != null)
			if (this.name.greaterThan(otherComplex.name))
				return true;
			else if (this.name.lessThan(otherComplex.name))
				return false;
		
		//if (this.name.equals(otherComplex.name)) {
		if (this.arguments != null) {
			if (this.arguments.length > otherComplex.arguments.length)
				return true;

			if (this.arguments.length == otherComplex.arguments.length) {
				for (int i = 0; i < this.arguments.length; i++) {
					if (this.arguments[i].greaterThan(otherComplex.arguments[i]))
						return true;
					else if (this.arguments[i].lessThan(otherComplex.arguments[i]))
						return false;
				}
				return false;
			}
			//return false;
		}
		return false;		
	}
	
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();

		//this.loadAttributes();
		
		if (this.name != null && this.name.getValue().length() > 0)
			stringBuilder.append(this.name.toString());
		
		if (this.getArity() != 0) {
			stringBuilder.append("(");

			for (int i = 0; i < this.getArity(); i++) {
				if (i > 0)
					stringBuilder.append(", ");
				stringBuilder.append(this.arguments[i].toString());				
			}
			stringBuilder.append(")");
		}
		
		return stringBuilder.toString();
	}
	
	@Override
	public DbComplex copy() { return new DbComplex(this.key, this.name, this.arguments); }
	
	public void load(TypeManager typeManager) {
		if (this.name == null) {
			ComplexStorageManager.DbComplexLoader complexLoader = typeManager.getComplex(this.key);
			if (complexLoader != null) {
				this.name = complexLoader.name;
				this.arguments = complexLoader.arguments;
			}
		}
	}
	
	@Override
	public int getBytes(byte[] bytes, int offset) {
		// just the key
		return ByteArrayHelper.getIntAsBytes(this.key, bytes, offset);
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getIntAsBytes(this.key);
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		MemoryMeasurement mm;		
		if (this.name != null) {
			mm = this.name.getSizeOf();
			used += mm.getUsed();
			allocated += mm.getAllocated();			
		}
		
		// 4 bytes for the count of arguments
		used += 4;
		allocated += 4;
		
		if (this.arguments != null) {
			for (int i = 0; i < this.arguments.length; i++) {
				// 1 byte for each byte indicating type of argument
				mm = this.arguments[0].getSizeOf();
				used += mm.getUsed() + 1;
				allocated += mm.getAllocated() + 1;
			}
		}
		
		return new MemoryMeasurement(used, allocated);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { return this.equals(dbTypeObject); }
		
	@Override
	public boolean isValid() { 
		return (this.key != 0);
	}
}
