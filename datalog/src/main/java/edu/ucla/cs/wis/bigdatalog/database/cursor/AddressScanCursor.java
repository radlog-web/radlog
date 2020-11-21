package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnlyLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class AddressScanCursor extends Cursor<AddressedTuple> 
	implements FixpointCursor {
	private BPlusTreeIntKeysOnly addresses;
	private BPlusTreeLeaf<?> currentLeaf;
	private AddressedTupleStore tupleStore;
	private int index;
	
	protected AddressScanCursor(Relation<AddressedTuple> relation) {
		super(relation);
		// only use on addressed tuple stores
		if (!(relation.getTupleStore() instanceof AddressedTupleStore))
			throw new DatabaseException("AddressScanCursor required an AddressedTupleStore!");
		
		this.tupleStore = (AddressedTupleStore)relation.getTupleStore();
		this.initialize();
	}
	
	@Override
	public void initialize() {
		this.index = 0;
		this.currentLeaf = null;
	}
	
	@Override
	public void reset() { 
		this.index = 0;
		this.currentLeaf = this.addresses.getFirstChild();
	}

	@Override
	public int getTuple(AddressedTuple tuple) {
		if (this.addresses.isEmpty())
			return 0;

		while (this.currentLeaf != null) {
			if (this.index < this.currentLeaf.getHighWaterMark()) {
				this.tupleStore.get(((BPlusTreeIntKeysOnlyLeaf) this.currentLeaf).getAt(this.index++), tuple);				
				break;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			this.index = 0;
		}
		return 1;
	}

	@Override
	public void moveNext() { this.index++;}

	@Override
	public void beginNextStage(DeALSContext deALSContext, int stageId) {
		// intentionally empty
	}
	
	public void beginNextStage(BPlusTreeIntKeysOnly addresses) {
		this.addresses = addresses;
		this.index = 0;
		this.currentLeaf = this.addresses.getFirstChild();
	}

	@Override
	public boolean isFixedPointReached() {
		return this.addresses.isEmpty();
	}

	@Override
	public int getIterationSize() { return this.addresses.getNumberOfEntries();  }

	@Override
	public AddressedTuple getEmptyTuple() { return (AddressedTuple) this.tupleStore.getEmptyTuple(); }
}
