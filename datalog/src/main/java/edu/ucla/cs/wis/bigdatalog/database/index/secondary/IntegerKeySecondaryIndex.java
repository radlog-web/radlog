package edu.ucla.cs.wis.bigdatalog.database.index.secondary;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;

abstract public class IntegerKeySecondaryIndex<S extends TupleStoreBase<AddressedTuple>> 
	extends SecondaryIndex<S> {

	public IntegerKeySecondaryIndex(Relation<AddressedTuple> relation, int indexedColumn) {
		super(relation, new int[]{indexedColumn});
	}
	
	public AddressedTuple get(Tuple tuple) {
		int key = this.getKeyI(tuple.columns);
		
		return this.doGet(key, tuple);
	}
	
	public int[] getSimilar(Tuple tuple) {
		return this.doGetSimilar(this.getKeyI(tuple.columns));
	}
	
	public boolean put(AddressedTuple tuple) {
		return this.doPut(this.getKeyI(tuple.columns), tuple);	
	}
	
	public boolean remove(AddressedTuple tuple) {
		return this.doRemove(this.getKeyI(tuple.columns), tuple);
	}
	
	public boolean update(AddressedTuple tuple) {
		int key = this.getKeyI(tuple.columns);
		boolean status = this.doRemove(key, tuple);
		if (status)
			this.doPut(key, tuple);
		
		return status;
	}
	
	public void clear() {
		this.doClear();
	}
	
	protected int getKeyI(DbTypeBase[] columns) {
		return ((EncodedType)columns[this.indexedColumns[0]]).getKey();
	}

	abstract protected boolean doPut(int key, AddressedTuple tuple);

	abstract protected AddressedTuple doGet(int key, Tuple tuple);

	abstract protected int[] doGetSimilar(int key);

	abstract protected boolean doRemove(int key, AddressedTuple tuple);
	
	abstract protected void doClear();
}
