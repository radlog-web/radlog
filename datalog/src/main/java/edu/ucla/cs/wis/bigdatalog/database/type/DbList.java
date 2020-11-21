package edu.ucla.cs.wis.bigdatalog.database.type;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.type.ListStorageManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DbList extends DbTypeBase implements BigEncodedType {
	private static final long serialVersionUID = 1L;

	public static final DbList NILLIST = new DbList();
	
	private boolean isNil;
	private long key;
	private DbTypeBase head;
	private DbList tail;

	private DbList() {
		this.isNil = true;
	}
	
	public static DbList create() {
		return NILLIST;
	}
		
	private DbList(long key) {
		if (key == 0) {
			this.isNil = true;
		} else {			
			this.key = key;
			this.isNil = false;
		}
	}
	
	private DbList(long key, DbTypeBase head, DbList tail) {
		this(key);
		this.head = head;
		this.tail = tail;
	}
	
	public static DbList load(long key, TypeManager typeManager) {
		DbList list = new DbList(key);
		list.load(typeManager);
		return list;
	}
	
	public static DbList load(long key, DbTypeBase head, DbList tail) {
		return new DbList(key, head, tail);
	}

	public DbTypeBase getHead() { return this.head; }

	public DbList getTail() { return this.tail; }

	@Override
	public boolean isConstant() { return true; }

	@Override
	public DataType getDataType() { return DataType.LIST; }
	
	@Override
	public int getKey() { return -1; }
	
	@Override	
	public long getKeyL() { return this.key; }
	
	public boolean isEmpty() {
		if (this.isNil)
			return true;
		
		if (this.key == 0 && this.head == null)
			return true;
		
		//this.loadList();
		
		return (this.head == null);
	}

//	public int hashCode() {
//		if (this.isEmpty())
//			return 0;
//
//		int hash = this.head.hashCode();
//
//		if (!this.tail.isEmpty())
//			hash += this.tail.hashCode();
//		
//		hash = DbTypeBase.hash(hash);
//
//		return hash;
//	}

	public int getLength() {
		if (this.isEmpty())	 // calls loadlist
			return 0;
		
		if (this.tail == null || this.tail.isNil)
			return 1;
		
		return 1 + this.tail.getLength();
	}
	
	public boolean contains(DbTypeBase dbTypeObject) {
		if (this.isNil)
			return false;
		
		//this.loadList();
		
		if (this.head.equals(dbTypeObject))
			return true;
		
		if (this.tail == null || this.tail.isNil)
			return false;
		
		return this.tail.contains(dbTypeObject);
	}
	
	public DbList insertAt(int position, DbTypeBase dbTypeObject, TypeManager typeManager) {		
		if (position < 0)
			return null;
		
		//this.loadList();
		this.load(typeManager);
		if (position == 0)
			return typeManager.createList(dbTypeObject, this);
		
		if (this.tail == null || this.tail.isNil)
			throw new DatabaseException("Can not insertAt position out of bounds.");
		
		return typeManager.createList(this.head, this.tail.insertAt(position - 1, dbTypeObject, typeManager));
	}
	
	public DbList overwriteAt(int position, DbTypeBase dbTypeObject, TypeManager typeManager) {
		if (position < 0)
			return null;
		
		//this.loadList();
		this.load(typeManager);
		
		if (position == 0)
			return typeManager.createList(dbTypeObject, this.tail);
		
		if (this.tail == null || this.tail.isNil)
			throw new DatabaseException("Can not overwriteAt position out of bounds.");

		return typeManager.createList(this.head, this.tail.overwriteAt(position - 1, dbTypeObject, typeManager));
	}

	public DbList removeAt(int position, TypeManager typeManager) {
		if (position < 0)
			return null;
		
		//this.loadList();
		this.load(typeManager);
		
		if (position == 0)
			return this.tail;
		
		if (this.tail == null || this.tail.isNil)
			throw new DatabaseException("Can not removeAt position out of bounds.");

		return typeManager.createList(this.head, this.tail.removeAt(position - 1, typeManager));
	}	
	
	public DbList append(DbTypeBase dbTypeObject, TypeManager typeManager) {
		//this.loadList();
		this.load(typeManager);
		
		if (this.isNil)
			return typeManager.createList(dbTypeObject, null);
		
		if (this.tail == null || this.tail.isNil) {
			DbList newTail = typeManager.createList(dbTypeObject, null);
			return typeManager.createList(this.head, newTail);
		}
		
		return typeManager.createList(this.head, this.tail.append(dbTypeObject, typeManager));
	}
	
	public DbTypeBase get(int position) {
		if (position < 0)
			return null;
		
		//this.loadList();
		
		if (position == 0)
			return this.head;

		if (this.tail == null || this.tail.isNil)
			throw new DatabaseException("Can not get position out of bounds.");
		
		return this.tail.get(position - 1);
	}
	
	public DbList getSublist(int position) {
		if (position < 1)
			return this;

		//this.loadList();
		
		if (position == 1)
			return this.tail;

		if (this.tail == null || this.tail.isNil)
			throw new DatabaseException("Can not getSublist position out of bounds.");
		
		return this.tail.getSublist(position - 1);
	}

	@Override
	public int hashCode() {
		return hashCode(0);
	}
	
	private int hashCode(int hash) {
		//this.loadList();		
		if (this.head == null)
			return hash;
		//final int prime = 31;
		//int h = hash;
		//h = prime * h + ((head == null) ? 0 : head.hashCode());
		//h = prime * h + ((tail == null) ? 0 : tail.hashCode(h));
		/*final int prime = 31;
		int h = hash;
		h = prime * h + ((head == null) ? 0 : head.hashCode());
		h = prime * h + ((tail == null) ? 0 : tail.hashCode(h));
		return (int)MurmurHash.hash(ByteArrayHelper.getIntAsBytes(h));	*/
				
		// for head
		int numberOfBytes = 4;
		// for tail
		if (this.tail != null && !this.tail.isNil)
			numberOfBytes += 4;
		
		byte[] bytes = new byte[numberOfBytes];
		int offset = ByteArrayHelper.getIntAsBytes(this.head.hashCode(), bytes, 0);
		
		if (this.tail != null && !this.tail.isNil)
			offset = ByteArrayHelper.getIntAsBytes(this.tail.hashCode(), bytes, offset);
		
		return (int)MurmurHash.hash(bytes);
	}
	
	@Override
	public long hashCodeL() {
		return hashCodeL(0);
	}
	
	@Override
	public long hashCodeL(int position) {
		//this.loadList();		
		if (this.head == null)
			return 0;
		
		// for head
		int numberOfBytes = 8;
		// for tail
		if (this.tail != null && !this.tail.isNil)
			numberOfBytes += 8;
		
		byte[] bytes = new byte[numberOfBytes];
		int offset = ByteArrayHelper.getLongAsBytes(this.head.hashCodeL(0), bytes, 0);
		
		if (this.tail != null && !this.tail.isNil)
			offset = ByteArrayHelper.getLongAsBytes(this.tail.hashCodeL(1), bytes, offset);
		
		return MurmurHash.hash(bytes, position);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof DbTypeBase))
			return false;
	
		if (this == other)
			return true;
		
		if (!(other instanceof DbList))
			return false;
		
		DbList otherList = (DbList)other;
		return this.key == otherList.key;
	}

	@Override
	public boolean greaterThan(DbTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof DbList))
			return false;
		
		DbList otherList = (DbList)other;
		return this.key > otherList.key;
	}

	@Override
	public String toString() {
		//this.loadList();
		StringBuilder buf = new StringBuilder();		
		buf.append("[");
		
		if (!this.isEmpty()) {
			DbList list = this;
			int counter = 0;
			String item;
			while (list != null && !list.isEmpty()) {				
				// if head is list and is last item, we flatten 1 level
				if ((list.tail == null/* || list.tail.isNil*/) && (list.getHead() instanceof DbList)) {
					String subList = list.getHead().toString();
					subList = subList.substring(1, subList.length() - 1);
					item = subList;
				} else {
					item = list.getHead().toString();
				}
				
				if (item != null && item.length() > 0) {
					if (counter > 0)				
						buf.append(", ");
					buf.append(item);
				}
				
				list = list.tail;
				counter++;
			}		
		}
		
		buf.append("]");
		return buf.toString();
	}
	
	@Override
	public DbList copy() { return new DbList(this.key, this.head, this.tail); }
	
	public void load(TypeManager typeManager) {
		if (this.isNil)
			return;
		// if head already loaded, we know we've loaded the list
		if (this.head != null)
			return;
	
		ListStorageManager.DbListLoader listLoader = typeManager.getList(this.key);
		if (listLoader != null) {
			this.head = listLoader.head;
			this.tail = listLoader.tail;
		}	
	}
	/*
	private void loadList() {
		if (this.isNil)
			return;
		// if head already loaded, we know we've loaded the list
		if (this.head != null)
			return;
	
		ListStorageManager.DbListLoader listLoader = deALSContext.getDatabase().getTypeManager().getList(this.key);
		if (listLoader != null) {
			//System.out.println("ListLoader.head: " + listLoader.head.toString());
			this.head = listLoader.head;
//			if (listLoader.tail != null)
//				System.out.println("ListLoader.tail: " + listLoader.tail.toString());
//			else
//				System.out.println("ListLoader.tail: null");
			this.tail = listLoader.tail;
		}	
	}*/

	@Override
	public int getBytes(byte[] bytes, int offset) {
		//if (this.isEmpty())
		//	return offset;
				
		return ByteArrayHelper.getLongAsBytes(this.key, bytes, offset);
	}
	
	@Override
	public byte[] getBytes() {
		return ByteArrayHelper.getLongAsBytes(this.key);
	}

	@Override
	public MemoryMeasurement getSizeOf() {
		// if empty, just count one byte for nil
		if (this.isEmpty())
			return new MemoryMeasurement(1,1);
		
		int used = 0;
		int allocated = 0;
		MemoryMeasurement mm;
		//this.loadList();
		if (this.head != null) {
			mm = this.head.getSizeOf();
			// liststoragemanager +1 for head type
			used += mm.getUsed() + 1;
			allocated += mm.getAllocated() + 1;
		}
		
		if (this.tail != null && !this.tail.isNil) {
			mm = this.tail.getSizeOf();
			// liststoragemanager + 8 for tail key
			used += mm.getUsed() + 8;
			allocated += mm.getAllocated() + 8;
		} else {
			// liststoragemanager + 4 for 0 indicating empty tail
			used += 4;
			allocated += 4;
		}
		
		return new MemoryMeasurement(used, allocated);
	}
	
	@Override
	public boolean match(DbTypeBase dbTypeObject) { return this.equals(dbTypeObject); }

}
