package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.type.SetStorageManager;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeIntKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeLongKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbSet extends DbTypeBase {
	enum SetType { INT, LONG, BYTES; }
	
	private static final long serialVersionUID = 1L;
	private int id;
	private BPlusTreeStoreStructure<?, ?, ?> set;
	private SetType setType;
	
	private DbSet(int id) {
		this.id = id;
	}
	
	private DbSet(int id, BPlusTreeStoreStructure<?, ?, ?> store) {
		this.id = id;
		this.set = store;
		this.getSetType();
	}

	public static DbSet load(int id, TypeManager typeManager) {
		DbSet set = new DbSet(id);
		set.load(typeManager);
		return set;
	}
	
	public static DbSet load(int id, BPlusTreeStoreStructure<?, ?, ?> store) {
		return new DbSet(id, store);
	}
	
	public int getId() { return this.id; }

	public int getNumberOfEntries() {
		return this.set.getNumberOfEntries();
	}
	
	public DbTypeBase put(DbTypeBase key) {
		if (this.setType == SetType.INT)
			return ((BPlusTreeIntKeysOnly)this.set).insert(key);
		
		return ((BPlusTreeLongKeysOnly)this.set).insert(key);
	}
	
	public DbTypeBase[] put(DbTypeBase[] key) {
		if (this.setType == SetType.LONG)
			return ((BPlusTreeLongKeysOnly)this.set).insert(key);
		
		return ((BPlusTreeByteKeysOnly)this.set).insert(key);
	}
	
	public DbTypeBase get(DbTypeBase key) {	
		if (this.setType == SetType.INT)
			return ((BPlusTreeIntKeysOnly)this.set).get(key);
		
		return ((BPlusTreeLongKeysOnly)this.set).get(key);
	}
	
	public DbTypeBase[] get(DbTypeBase[] key) {	
		if (this.setType == SetType.LONG)
			return ((BPlusTreeLongKeysOnly)this.set).get(key);	
		
		return ((BPlusTreeByteKeysOnly)this.set).get(key);
	}	

	public boolean remove(DbTypeBase key) {
		if (this.setType == SetType.INT)
			return ((BPlusTreeIntKeysOnly)this.set).delete(key);
		
		return ((BPlusTreeLongKeysOnly)this.set).delete(key);
	}
	
	public boolean remove(DbTypeBase[] key) {
		if (this.setType == SetType.LONG)
			return ((BPlusTreeLongKeysOnly)this.set).delete(key);
		
		return ((BPlusTreeByteKeysOnly)this.set).delete(key);
	}
	
	public void clear() {
		this.set.deleteAll();
	}
	
	@Override
	public DataType getDataType() { return DataType.SET; }

	@Override
	public boolean isConstant() { return false; }

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
	
		if (this == other)
			return true;
		
		if (!(other instanceof DbSet))
			return false;
		
		DbSet otherBPlusTree = (DbSet)other;
		return this.id == otherBPlusTree.id;
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
	public String toString() {
		return "Id: " + this.id + " | # of entries: " + this.set.getNumberOfEntries() + " " + this.set.toStringShort();
	}

	@Override
	public DbTypeBase copy() { return new DbSet(this.id, this.set); }

	public void load(TypeManager typeManager) {
		if (this.id > 0) {
			SetStorageManager.DbSetLoader loader = typeManager.getSet(this.id);
			if (loader != null) {
				this.set = loader.set;
				this.getSetType();
			}
		}
	}
	
	private void getSetType() {
		if (this.set instanceof BPlusTreeIntKeysOnly)
			this.setType = SetType.INT;
		else if (this.set instanceof BPlusTreeLongKeysOnly)
			this.setType = SetType.LONG;
		else 
			this.setType = SetType.BYTES;
	}
	
	@Override
	public int getBytes(byte[] bytes, int offset) {
		return ByteArrayHelper.getIntAsBytes(this.id, bytes, offset);
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getIntAsBytes(this.id);
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 4; // for the integer key
		int allocated = 4; // for the integer key
		MemoryMeasurement mm;
		
		if (this.set != null) {
			mm = this.set.getSizeOf();
			used += mm.getUsed();
			allocated += mm.getAllocated();
		}		
		return new MemoryMeasurement(used, allocated);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { return this.equals(dbTypeObject); }
	
	@Override
	public boolean isValid() { 
		return (this.id != 0);
	}
	
	public BPlusTreeStoreStructure<?, ?, ?> getTree() { return this.set; }
}