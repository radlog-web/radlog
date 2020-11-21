package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.heap.Heap;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeMultiStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public class TupleBPlusTreeMultiStoreCursor 
	extends Cursor<Tuple> 
	implements SelectionCursor<Tuple> {	
	protected TupleBPlusTreeMultiStore 	tupleStore;
	protected int[] 				keyColumns;
	protected DbTypeBase[]			keyColumnValues;
	protected byte[] 				keyBytes;
	protected Heap					matchingTuples;
	protected int					lastMatchPosition;	
	
	public TupleBPlusTreeMultiStoreCursor(Relation<Tuple> relation) {
		super(relation);
				
		if (!(this.relation.getTupleStore() instanceof TupleBPlusTreeMultiStore))
			throw new DatabaseException("TupleBPlusTreeMultiStoreCursor can only used on TupleBPlusTreeMultiStore TupleStores!");
		
		this.tupleStore = (TupleBPlusTreeMultiStore)this.relation.getTupleStore();
		this.keyColumns = this.tupleStore.getKeyColumns();
		this.keyColumnValues = new DbTypeBase[this.keyColumns.length];
	}
	
	@Override
	public int[] getFilterColumns() { return this.keyColumns; }
	
	@Override
	public DbTypeBase[] getFilter() { return this.keyColumnValues; }
	
	
	public void reset() {
		//this.timesCalled[0]++;
		//long start = System.nanoTime();
		this.matchingTuples = null;
		this.lastMatchPosition = 0;
		this.keyColumnValues = null;
		//this.timeSpent[0] += System.nanoTime() - start;
	}
	
	public void reset(DbTypeBase[] indexColumnValues) {
		//this.timesCalled[0]++;
		//long start = System.nanoTime();
	    //if (indexColumnValues.length != this.tupleStore.getKeyColumns().length)
	    //	throw new DatabaseException("TupleBPlusTreeStoreCursor must use matching key columns from TupleBPlusTreeStore!");
		 
		//this.keyColumnValues = new DbTypeBase[this.keyColumns.length];
		for (int i = 0; i < this.keyColumns.length; i++)
			this.keyColumnValues[i] = indexColumnValues[i];
	    
		this.keyBytes = this.tupleStore.getKey(this.keyColumnValues); 
		
	    this.matchingTuples = null;
		this.lastMatchPosition = 0;		
		
		//this.timeSpent[0] += System.nanoTime() - start;
	}

	@Override
	public int getTuple(Tuple tuple) {
		//this.timesCalled[1]++;
		//long start = System.nanoTime();

		if (this.matchingTuples == null) {			
	    	this.matchingTuples = this.tupleStore.get(this.keyBytes);
	    	this.lastMatchPosition = 0;
	    }
		
		if (this.matchingTuples != null) {			
			byte[] data = null;

			while ((this.lastMatchPosition < this.matchingTuples.getHighWaterMark()) 
					&& ((data = this.matchingTuples.get(this.lastMatchPosition++)) != null)) {
				/* keep spinning until we find a tuple or fail out*/ 
				if (this.tupleStore.loadTuple(this.keyBytes, data, tuple) > 0)
					return 1;
			}
		}
		//if (tuple == null)
		//	this.timesNoResults[1]++;
		//this.timeSpent[1] += System.nanoTime() - start;		
	    return 0;
	}
	
	@Override
	public void moveNext() {  }
}
