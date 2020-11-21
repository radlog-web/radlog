package edu.ucla.cs.wis.bigdatalog.database.index;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class Index<T extends Tuple> 
	implements MemorySize {
	public final boolean DEBUG = false;
	
	protected Relation<T>			relation;
	protected int					bytesPerKey;
	protected final int[] 		indexedColumns;
	protected int					numberOfEntries;
	protected final int 			numberOfKeyColumns;
	
	public Index(Relation<T> relation, int[] indexedColumns) {
		if (relation == null)
			throw new DatabaseException("Index requires a relation.");
		
		this.relation = relation;
		
		if (indexedColumns == null || indexedColumns.length == 0)
			throw new DatabaseException("Index can not be created without at least one column to index on.");
		
		this.indexedColumns = indexedColumns;
		this.numberOfKeyColumns = indexedColumns.length;
	}
	
	public Relation<T> getRelation() { return this.relation; }
	
	public int getNumberOfEntries() { return this.numberOfEntries; }
	
	protected byte[] getKey(DbTypeBase[] columns) {
		byte[] bytes = new byte[this.bytesPerKey];
		int offset = 0;	
		
		for (int i = 0; i < this.indexedColumns.length; i++)
			offset = columns[this.indexedColumns[i]].getBytes(bytes, offset);

		return bytes;
	}
	
	protected int getKeySize() {
		DataType[] schema = this.relation.getSchema();
		int size = 0;
		
		for (int i = 0; i < this.indexedColumns.length; i++)
			size += schema[this.indexedColumns[i]].getNumberOfBytes();
		
		return size;
	}
	
	@SuppressWarnings("unchecked")	
	public void build() {
		if (this.relation.getTupleStore().getNumberOfTuples() > 0) {
			Cursor<T> cursor = (Cursor<T>) this.relation.getDatabase().getCursorManager().createScanCursor(this.relation);
			  
			T tuple = cursor.getEmptyTuple();
			while (cursor.getTuple(tuple) > 0) {
				this.put(tuple);
			}
			
			this.relation.getDatabase().getCursorManager().destroyCursor(cursor);
		}
	}
	
	abstract public boolean put(T tuple);
	
	abstract public boolean exists(Tuple tuple);
	
	abstract public boolean remove(T tuple);
	
	abstract public boolean update(T tuple);
		
	abstract public void clear();
	
	abstract public void resetCounters();
	
	abstract public String[] getFunctions();
	
	abstract public MemoryMeasurement getSizeOf();
}
