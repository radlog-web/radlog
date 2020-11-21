package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

abstract public class RecursiveCursor 
	extends FilteredScanCursor<AddressedTuple> 
	implements FixpointCursor {
	public int baseTupleAddress;
	public int endTupleAddress;
	protected 	AddressedTupleStore tupleStore;
	protected int currentTupleAddress;
	protected AddressedTuple tuple;	

	protected RecursiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) {
		super(relation, filteredColumns);

		if (!(relation.getTupleStore() instanceof AddressedTupleStore))
			throw new DatabaseException("Recursive cursors must use an addressed tuple store");
		
		this.tupleStore = (AddressedTupleStore)relation.getTupleStore();
	    this.baseTupleAddress = -1;
	    this.endTupleAddress = -1;
	    this.currentTupleAddress = -1;
	    this.tuple = this.relation.getEmptyTuple();
	    this.setFirstStage();
	}
	
	protected void setFirstStage() {
		this.baseTupleAddress = this.tupleStore.getFirstTupleAddress();						
		this.endTupleAddress = this.tupleStore.getLastTupleAddress();	
		this.currentTupleAddress = this.baseTupleAddress;
	}

	@Override
	public boolean isFixedPointReached() {
		long lastTupleAddress = this.tupleStore.getLastTupleAddress();
		return (lastTupleAddress == this.endTupleAddress);
	}

	@Override
	public void initialize() {
		this.baseTupleAddress = -1;
		this.endTupleAddress = -1;
		this.currentTupleAddress =-1;
		this.setFirstStage();
	}

	@Override
	public void reset(DbTypeBase[] indexColumnValues) {
		this.setFilterValues(indexColumnValues);
		if (this.tupleStore.get(this.baseTupleAddress, this.tuple) > 0)
			this.currentTupleAddress = this.tuple.address;
		else
			this.currentTupleAddress = -1;
	}

	@Override
	public int getTuple(AddressedTuple tuple) {
		if (this.currentTupleAddress == -1)
			return 0;
		
		while (this.currentTupleAddress <= this.endTupleAddress) {
			if ((this.tupleStore.get(this.currentTupleAddress++, tuple) > 0)
					&& this.matchTuple(tuple.columns))
				return 1;
		}

		return 0;
	}
		
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("Cursor " + this.hashCode() + " for relation " + this.relation.getName());
		output.append("\nthis.baseTupleAddress | this.endTupleAddress [" + this.baseTupleAddress + " | " + this.endTupleAddress + "]");

		return output.toString();
	}
	
	abstract public void beginNextStage(DeALSContext deALSContext, int stageId);
}
