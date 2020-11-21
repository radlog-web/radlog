package edu.ucla.cs.wis.bigdatalog.database.index.secondary;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class LongKeySecondaryIndex<S extends TupleStoreBase<AddressedTuple>> 
	extends SecondaryIndex<S> {
	
	public final int numberOfKeyColumns;
	
	public LongKeySecondaryIndex(Relation<AddressedTuple> relation, int[] indexedColumns) {
		super(relation, indexedColumns);
		boolean status = false;
		// we have to have 8 bytes worth of key
		if (indexedColumns.length == 2) {
			int bytes = relation.getSchema()[indexedColumns[0]].getNumberOfBytes() + relation.getSchema()[indexedColumns[1]].getNumberOfBytes();
			status = (bytes == DataType.LONG.getNumberOfBytes());		
		} else if (indexedColumns.length == 1) {
			status = (relation.getSchema()[indexedColumns[0]].getNumberOfBytes() == DataType.LONG.getNumberOfBytes());			
		}

		if (!status)
			throw new DatabaseException("LongKeySecondaryIndex requires 2 key columns");
		
		this.numberOfKeyColumns = indexedColumns.length;
	}

	public AddressedTuple get(Tuple tuple) {
		return this.doGet(this.getKeyL(tuple.columns), tuple);
	}
	
	public int[] getSimilar(Tuple tuple) {
		return this.doGetSimilar(this.getKeyL(tuple.columns));
	}
	
	public boolean put(AddressedTuple tuple) {
		return this.doPut(this.getKeyL(tuple.columns), tuple);
	}
	
	public boolean remove(AddressedTuple tuple) {
		return this.doRemove(this.getKeyL(tuple.columns), tuple);
	}
	
	public boolean update(AddressedTuple tuple) {			
		long key = this.getKeyL(tuple.columns);
		boolean status = this.doRemove(key, tuple);
		if (status)
			this.doPut(key, tuple);
		
		return status;
	}
	
	public void clear() {
		this.doClear();
	}
	
	protected long getKeyL(DbTypeBase[] columns) {
		if (this.numberOfKeyColumns == 2) {
			long keyPart1 = ((EncodedType)columns[this.indexedColumns[0]]).getKey();
			long keyPart2 = ((EncodedType)columns[this.indexedColumns[1]]).getKey();
			return (keyPart1 << 32) | (keyPart2 & 0xffffffffL);
		}
		
		return ((DbLong)columns[this.indexedColumns[0]]).getValue();
	}

	abstract protected boolean doPut(long key, AddressedTuple tuple);

	abstract protected AddressedTuple doGet(long key, Tuple tuple);

	abstract protected int[] doGetSimilar(long key);

	abstract protected boolean doRemove(long key, AddressedTuple tuple);
	
	abstract protected void doClear();
}
