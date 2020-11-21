package edu.ucla.cs.wis.bigdatalog.database.index.secondary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.Index;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

abstract public class SecondaryIndex<S extends TupleStoreBase> 
	extends Index<AddressedTuple> {

	protected int[] 				unindexedColumns;
	protected int					totalNumberOfColumns;
	protected S 					tupleStore;
	protected AddressedTuple		capturedTuple;

	@SuppressWarnings("unchecked")
	public SecondaryIndex(Relation<AddressedTuple> relation, int[] indexedColumns) {
		super(relation, indexedColumns);
		
		if (relation.getTupleStore() == null)
			throw new DatabaseException("Search Index requires a TupleStore.");
		
		this.tupleStore = (S) relation.getTupleStore();
		this.capturedTuple = this.relation.getEmptyTuple();

		this.totalNumberOfColumns = relation.getArity();
		
		List<Integer> temp = new ArrayList<>();
		for (int i = 0; i < this.totalNumberOfColumns; i++) {
			boolean found = false;
			for (int j = 0; j < indexedColumns.length; j++) {
				if (indexedColumns[j] == i)
					found = true;
			}
			if (!found)
				temp.add(i);
		}
		
		int[] unindexedColumns = null;
		if (temp.size() > 0) {
			unindexedColumns = new int[temp.size()];		
			for (int i = 0; i < temp.size(); i++)
				unindexedColumns[i] = temp.get(i);
		}
		
		// sort columns so we can scan in order
		Arrays.sort(indexedColumns);
		if (unindexedColumns != null)
			Arrays.sort(unindexedColumns);
				
		// APS 7/27/2013
		if (unindexedColumns == null || unindexedColumns.length == 0) {
			this.totalNumberOfColumns = indexedColumns.length;
		} else {
			this.unindexedColumns = unindexedColumns;
			this.totalNumberOfColumns = unindexedColumns.length + indexedColumns.length;
		}
	}
	
	public int getNumberOfIndexedColumns() { return this.indexedColumns.length; }

	public int getIndexedColumn(int position) { return this.indexedColumns[position];}
	
	public int[] getIndexedColumns() { return this.indexedColumns; }
	
	public int[] getUnindexedColumns() { return this.unindexedColumns; }
	
	public boolean isAllColumnsIndexed() { return (this.indexedColumns.length == this.totalNumberOfColumns); }
		
	public void resetCounters() {
		/* commenting out for experimentation - APS 12/2/2013
		
		this.timeSpent = new long[this.numberOfCounters];
		this.timesNoResults = new int[this.numberOfCounters];
		
		for (int i = 0; i < this.numberOfCounters; i++) {
			this.timesCalled[i] = 0;
			this.timeSpent[i] = 0;		
			this.timesNoResults[i] = 0;
		}
		*/
	}
		
	public boolean exists(Tuple tuple) {
		return (this.get(tuple) != null);
	}
	
	abstract public Tuple get(Tuple tuple);
	
	abstract public int[] getSimilar(Tuple tuple);
			
	abstract public String toString();
	
	abstract public String toStringStatistics();

	public String[] getFunctions() { return  new String[]{"getkey", "hash", "put", "exists", "remove", "update", "split", "get", "getSimilar"}; }
}
