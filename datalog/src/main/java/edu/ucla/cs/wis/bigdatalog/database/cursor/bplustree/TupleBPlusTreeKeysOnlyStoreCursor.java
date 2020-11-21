package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeKeysOnlyStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public class TupleBPlusTreeKeysOnlyStoreCursor 
	extends Cursor<Tuple> 
	implements SelectionCursor<Tuple> {

	protected TupleBPlusTreeKeysOnlyStore 	tupleStore;
	protected int[] 						keyColumns;
	protected DbTypeBase[]					keyColumnValues;	
	protected byte[]						keyBytes;
	protected boolean						done;
	
	public TupleBPlusTreeKeysOnlyStoreCursor(Relation<Tuple> relation) {
		super(relation);
		
		if (!(this.relation.getTupleStore() instanceof TupleBPlusTreeKeysOnlyStore))
			throw new DatabaseException("TupleBPlusTreeKeysOnlyStoreCursor can only used on TupleBPlusTreeKeysOnlyStore TupleStores!");
		
		this.tupleStore = (TupleBPlusTreeKeysOnlyStore)this.relation.getTupleStore();
		this.done = false;
		this.keyColumns = this.tupleStore.getKeyColumns();
		this.keyColumnValues = new DbTypeBase[this.keyColumns.length];
	}
	
	@Override
	public int[] getFilterColumns() { return this.keyColumns; }
	
	@Override
	public DbTypeBase[] getFilter() { return this.keyColumnValues; }
		
	public void reset() {	
		this.keyColumnValues = null;
		this.done = false;		
	}
			
	public void reset(DbTypeBase[] indexColumnValues) {
		for (int i = 0; i < this.keyColumns.length; i++)
			this.keyColumnValues[i] = indexColumnValues[i];
	    
		this.keyBytes = this.tupleStore.getKey(this.keyColumnValues); 
		this.done = false;
	}

	@Override
	public int getTuple(Tuple tuple) {
		// there is only 1 entry in the tree that will match
		if (!done && this.tupleStore.get(this.keyBytes) != null) { 
			this.tupleStore.loadTuple(this.keyBytes, tuple);		
			this.done = true;
			return 1;
		}		
		return 0;
	}
	
	@Override
	public void moveNext() {  }
}
