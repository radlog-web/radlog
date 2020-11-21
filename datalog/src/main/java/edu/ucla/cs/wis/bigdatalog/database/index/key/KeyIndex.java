package edu.ucla.cs.wis.bigdatalog.database.index.key;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.Index;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

abstract public class KeyIndex<T extends Tuple> 
	extends Index<T> 
	implements MemorySize {
	
	protected TupleStoreBase<T> tupleStore;

	public KeyIndex(Relation<T> relation, int[] keyColumns) {
		super(relation, keyColumns);
	
		this.tupleStore = relation.getTupleStore();
	}	

	public int getNumberOfKeyColumns() { return this.indexedColumns.length; }

	public int getKeyColumn(int position) { return this.indexedColumns[position];}
	
	public int[] getKeyColumns() { return this.indexedColumns; }

	public String[] getFunctions() { return new String[]{"getkey", "hash", "put", "exists", "remove", "update", "split"}; }
		
	public void resetCounters() {		
		/* this.timesCalled = new int[this.numberOfCounters];
		this.timeSpent = new long[this.numberOfCounters];
		this.timesNoResults = new int[this.numberOfCounters];
		
		for (int i = 0; i < this.numberOfCounters; i++) {
			this.timesCalled[i] = 0;
			this.timeSpent[i] = 0;		
			this.timesNoResults[i] = 0;
		}*/
	}	
	
	abstract public String toString();
	
	abstract public MemoryMeasurement getSizeOf();

}
