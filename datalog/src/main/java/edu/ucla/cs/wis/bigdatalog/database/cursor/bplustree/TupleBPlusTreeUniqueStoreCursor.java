package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysbytevalues.BPlusTreeByteKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysbytevalues.BPlusTreeIntKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class TupleBPlusTreeUniqueStoreCursor 
	extends Cursor<Tuple> 
	implements SelectionCursor<Tuple> {
	
	protected TupleBPlusTreeUniqueStore tupleStore;
	protected DbTypeBase[] keyValues;
	protected byte[] keyB;
	protected int keyI;
	protected long keyL;
	protected byte[] leafData;
	protected int leafOffset;
	protected final int keyType;
	protected boolean done;
	
	public TupleBPlusTreeUniqueStoreCursor(Relation<Tuple> relation) {
		super(relation);
		this.tupleStore = (TupleBPlusTreeUniqueStore) relation.getTupleStore();
		if (this.tupleStore.storageStructure instanceof BPlusTreeByteKeysByteValues)
			this.keyType = 1;
		else if (this.tupleStore.storageStructure instanceof BPlusTreeIntKeysByteValues)
			this.keyType = 2;
		else // BPlusTreeLongKeysByteValues
			this.keyType = 3;
	}

	@Override
	public void reset() { }

	@Override
	public void reset(DbTypeBase[] keyValues) {
		this.keyValues = keyValues;
		switch (this.keyType) {
			case 1:
				this.keyB = this.tupleStore.getKey(keyValues);
				break;
			case 2:
				this.keyI = this.tupleStore.getKeyI(keyValues);
				break;
			case 3:
				this.keyL = this.tupleStore.getKeyL(keyValues);
				break;
		}
		this.done = false;
	}

	@Override
	public int[] getFilterColumns() {
		return this.tupleStore.getKeyColumns();
	}

	@Override
	public DbTypeBase[] getFilter() {
		return this.keyValues;
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		if (this.done)
			return 0;
		
		int status = 0;
		switch (this.keyType) {
			case 1:
				status = this.tupleStore.getTuple(this.keyB, tuple);
				break;
			case 2:
				status = this.tupleStore.getTuple(this.keyI, tuple);
				break;
			case 3:
				status = this.tupleStore.getTuple(this.keyL, tuple);
				break;
		}
		this.done = true;
		return status;
	}

	@Override
	public void moveNext() {}
}
