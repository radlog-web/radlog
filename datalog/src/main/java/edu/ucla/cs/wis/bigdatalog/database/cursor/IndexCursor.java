package edu.ucla.cs.wis.bigdatalog.database.cursor;

import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class IndexCursor 
	extends FilteredScanCursor<AddressedTuple> {
	protected SecondaryIndex<?> 		index;
	protected int[] 					possibleMatchAddresses;
	protected int						lastMatchPosition;
	protected Tuple 					lookupTuple;
	protected int[]						indexedColumns;
	protected AddressedTupleStore		tupleStore;
	protected final boolean				skipMatch;

	protected IndexCursor(Relation<AddressedTuple> relation, int[] indexedColumns) {
		super(relation, indexedColumns);

		this.tupleStore = (AddressedTupleStore)this.relation.getTupleStore();
		this.index = relation.addSecondaryIndex(indexedColumns);
		this.indexedColumns = indexedColumns;
		
		this.lookupTuple = new Tuple(this.relation.getArity());
		this.possibleMatchAddresses = null;
		if (this.index instanceof BPlusTreeSecondaryIndex)
			this.skipMatch = true;
		else
			this.skipMatch = this.index.isAllColumnsIndexed();		
	}
	
	public SecondaryIndex<?> getIndex() { return this.index; }
	
	public void reset(DbTypeBase[] indexColumnValues) {
		this.setFilterValues(indexColumnValues);

		for (int i = 0; i < this.indexedColumns.length; i++)
			this.lookupTuple.setColumn(this.indexedColumns[i], indexColumnValues[i]);
		
		this.possibleMatchAddresses = null;
		this.lastMatchPosition = 0;
	}

	// this assumes a maintained index
	// if tuples are added/removed after this cursor was created, it might be stale
	public int getTuple(AddressedTuple tuple) {
		// get set if we don't already have it
		if (this.possibleMatchAddresses == null) {
			this.possibleMatchAddresses = this.index.getSimilar(this.lookupTuple);
			this.lastMatchPosition = 0;
		}

		if (this.possibleMatchAddresses != null) {
			while (this.lastMatchPosition < this.possibleMatchAddresses.length) {
				if (this.tupleStore.get(this.possibleMatchAddresses[this.lastMatchPosition++], tuple) > 0) {
					if (this.skipMatch)
						return 1;
					
					if (this.matchTuple(tuple.columns))
						return 1;
				}
			}
		}

		return 0;
	}
	
	// this method is necessary to refresh the cursor after the underlying relation has been modified
	// however, we do not want to reset the cursor to the beginning since we've already processed some tuples 
	public void refresh() {
		if (this.possibleMatchAddresses == null)
			return;
		
		int[] similarTuples = this.index.getSimilar(this.lookupTuple);
		if (similarTuples == null || similarTuples.length == 0) {
			this.possibleMatchAddresses = null;
			return;
		}
		
		if (this.lastMatchPosition  > similarTuples.length)
			this.lastMatchPosition = similarTuples.length;
		this.possibleMatchAddresses = similarTuples;
	}
	
	public void refreshXY() {
		if (this.possibleMatchAddresses == null)
			return;
		
		int[] similarTuples = this.index.getSimilar(this.lookupTuple);
		if (similarTuples == null || similarTuples.length == 0) {
			this.possibleMatchAddresses = null;
			return;
		}
		
		int newLastMatchPosition = 0;
		
		for (int i = 0; i < this.lastMatchPosition; i++) {
			for (int j = 0; j < similarTuples.length; j++) {
				if (this.possibleMatchAddresses[i] == similarTuples[j]) {
					newLastMatchPosition++;
				}
			}
		}
		
		this.possibleMatchAddresses = similarTuples;
		this.lastMatchPosition = newLastMatchPosition;
	}
	
	public void commit(int numberRemoved) {
		if ((this.possibleMatchAddresses == null) || (numberRemoved == 0))
			return;
		
		List<Integer> temp = new ArrayList<>();
		for (int i = 0; i < this.possibleMatchAddresses.length; i++) {
			if (this.possibleMatchAddresses[i] > (numberRemoved - 1))
				temp.add(this.possibleMatchAddresses[i] - numberRemoved);			
		}
		
		int[] similarTuples = new int[temp.size()];
		if (temp.size() > 0) {
			for (int i = 0; i < temp.size(); i++)
				similarTuples[i] = temp.get(i);
		}
		
		if (this.lastMatchPosition  > similarTuples.length)
			this.lastMatchPosition = similarTuples.length;
		this.possibleMatchAddresses = similarTuples;
	}
	
	public void resetCurrentMatches() {
		this.lastMatchPosition = 0;
	}
}
