package edu.ucla.cs.wis.bigdatalog.database.index.secondary;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;

abstract public class GeneralKeySecondaryIndex<S extends TupleStoreBase<AddressedTuple>> 
	extends SecondaryIndex<S> {
		
	public GeneralKeySecondaryIndex(Relation<AddressedTuple> relation, int[] indexedColumns) {
		super(relation, indexedColumns);
	}
	
	public AddressedTuple get(Tuple tuple) {
		return this.doGet(this.getKey(tuple.columns), tuple);
	}
	
	public int[] getSimilar(Tuple tuple) {
		return this.doGetSimilar(this.getKey(tuple.columns));
	}
	
	public boolean put(AddressedTuple tuple) {
		return this.doPut(this.getKey(tuple.columns), tuple);	
	}
	
	public boolean remove(AddressedTuple tuple) {	
		return this.doRemove(this.getKey(tuple.columns), tuple);
	}
	
	public boolean update(AddressedTuple tuple) {
		byte[] key = this.getKey(tuple.columns);
		boolean status = this.doRemove(key, tuple);
		if (status)
			this.doPut(key, tuple);

		return status;
	}
	
	public void clear() {
		this.doClear();
	}
	
	abstract protected boolean doPut(byte[] key, AddressedTuple tuple);

	abstract protected AddressedTuple doGet(byte[] key, Tuple tuple);

	abstract protected int[] doGetSimilar(byte[] key);

	abstract protected boolean doRemove(byte[] key, AddressedTuple tuple);
	
	abstract protected void doClear();
}
