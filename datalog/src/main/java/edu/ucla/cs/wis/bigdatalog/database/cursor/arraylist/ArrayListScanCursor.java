package edu.ucla.cs.wis.bigdatalog.database.cursor.arraylist;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;

public class ArrayListScanCursor 
	extends Cursor<AddressedTuple> {
	
	protected AddressedTupleStore tupleStore;
	protected int currentTupleAddress; 
	
	public ArrayListScanCursor(Relation<AddressedTuple> relation) {
		super(relation);
		
		this.tupleStore = (AddressedTupleStore)this.relation.getTupleStore();
		this.currentTupleAddress = -1;
	}

	public void reset() {
		this.currentTupleAddress = -1;
	}
	
	public int getTuple(AddressedTuple tuple) {		
		if (this.currentTupleAddress == -1) {
			this.currentTupleAddress = this.tupleStore.getFirstTupleAddress();
			if (this.currentTupleAddress == -1)
				return 0;
		}
		
		for (int i = this.currentTupleAddress; i <= this.tupleStore.getLastTupleAddress(); i++) {
			this.currentTupleAddress++;
			if ((this.tupleStore.get(i, tuple)) > 0)
				return 1;
		}
		return 0;
	}

	
	@Override
	public void moveNext() { if (this.currentTupleAddress != -1) this.currentTupleAddress++; }
}
