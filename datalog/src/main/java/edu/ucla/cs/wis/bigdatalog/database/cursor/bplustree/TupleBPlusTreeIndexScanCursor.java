package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndexLeaf;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class TupleBPlusTreeIndexScanCursor 
	extends Cursor<AddressedTuple> 
	implements FixpointCursor {
	protected BPlusTreeSecondaryIndex<BPlusTreeSecondaryIndexLeaf<?>> 	index;
	protected AddressedTupleStore						tupleStore;
	protected BPlusTreeSecondaryIndexLeaf<?>		 	currentLeaf;
	protected int										keyIndex;	
	protected TupleAddressArray							currentAddressArray;
	protected int[]										addresses;
	protected int 										addressIndex;
		
	public TupleBPlusTreeIndexScanCursor(Relation<AddressedTuple> relation, BPlusTreeSecondaryIndex<BPlusTreeSecondaryIndexLeaf<?>> index) {
		super(relation);
		
		if (!(relation.getTupleStore() instanceof AddressedTupleStore))
			throw new DatabaseException("Relation must have an AddressedTupleStore to use a TupleBPlusTreeIndexScanCursor cursor.");
		
		this.index = index;
		this.tupleStore = (AddressedTupleStore)relation.getTupleStore();
		this.initialize();
	}
	
	public BPlusTreeSecondaryIndex<BPlusTreeSecondaryIndexLeaf<?>> getIndex() { return this.index; }
	
	public void initialize() {
		this.currentLeaf = this.index.getFirstChild();
		this.currentAddressArray = null;
		this.addresses = null;		
		this.keyIndex = 0;
		this.addressIndex = 0;
	}
	
	public void reset() {
		this.initialize();
	}
	@Override
	public int getTuple(AddressedTuple tuple) {		
		// get leaf
		// get tuple address array
		// get all values from array
		// when out of values in array, get next array
		// when out of arrays, get next leaf
		// when out of leaves, done
		while (this.currentLeaf != null) {
			while ((this.currentAddressArray != null)
					&& (this.addressIndex < this.currentAddressArray.getNumberOfAddresses())) {
				if (this.tupleStore.get(this.addresses[this.addressIndex++], tuple) > 0)
					return 1;
			}
			
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				this.currentAddressArray = this.currentLeaf.getAddressArrays()[this.keyIndex++];
				if (this.currentAddressArray != null && this.currentAddressArray.getNumberOfAddresses() > 0) {
					this.addresses = this.currentAddressArray.getAddresses();
					this.addressIndex = 0;
				}
			} else {
				// out of key/values, so move to next leaf
				this.currentLeaf = (BPlusTreeSecondaryIndexLeaf<?>) this.currentLeaf.getNext();				
				if (this.currentLeaf != null && this.currentLeaf.getHighWaterMark() > 0) {
					this.currentAddressArray = this.currentLeaf.getAddressArrays()[0];
					this.addresses = this.currentAddressArray.getAddresses();
				}
				this.keyIndex = 1;
				this.addressIndex = 0;
			}			
		}
		return 0;
	}

	@Override
	public boolean isFixedPointReached() { 
		return this.index.isEmpty(); 
	}

	public void beginNextStage(BPlusTreeSecondaryIndex<BPlusTreeSecondaryIndexLeaf<?>> index) {
		if (this.index != null)
			((SecondaryIndex<?>)this.index).clear();
		this.relation.removeSecondaryIndex((SecondaryIndex<?>) this.index);
		this.index = index;
	}

	@Override
	public void beginNextStage(DeALSContext deALSContext, int stageId) { }
	
	@Override
	public void moveNext() { this.keyIndex++; }
	
	@Override
	public int getIterationSize() { return this.index.getSize(); }
}