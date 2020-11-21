package edu.ucla.cs.wis.bigdatalog.database.index.key;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.BigEncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class LongKeyKeyIndex 
	extends KeyIndex {
	
	protected KeyIndexResult result;
	
	public LongKeyKeyIndex(Relation relation, int[] keyColumns) {
		super(relation, keyColumns);
		
		boolean status = false;
		// we have to have 8 bytes worth of key
		if (keyColumns.length == 2) {
			int bytes = relation.getSchema()[keyColumns[0]].getNumberOfBytes() + relation.getSchema()[keyColumns[1]].getNumberOfBytes();
			status = (bytes == DataType.LONG.getNumberOfBytes());		
		} else if (keyColumns.length == 1) {
			status = (relation.getSchema()[keyColumns[0]].getNumberOfBytes() == DataType.LONG.getNumberOfBytes());			
		}

		if (!status)
			throw new DatabaseException("LongKeyIndex requires one 8 byte or two 4 byte key columns");
		
		this.result = new KeyIndexResult();
	}

	public boolean put(Tuple tuple) {
		this.doPut(this.getKeyL(tuple.columns), this.result);
		return this.result.success;
	}
	
	public boolean exists(Tuple tuple) {
		long key = this.getKeyL(tuple.columns);		
		return this.doGet(key);
	}
	
	public boolean remove(Tuple tuple) {
		if (tuple.columns.length < this.indexedColumns.length) 
			return false;

		long key = this.getKeyL(tuple.columns);
		return this.doRemove(key);
	}

	public boolean update(Tuple tuple) {
		boolean status = this.doRemove(this.getKeyL(tuple.columns));
		if (status) {
			this.doPut(this.getKeyL(tuple.columns), this.result);
			return this.result.success;	
		}
		return status;
	}
	
	public void clear() {
		this.doClear();
	}
	
	abstract public void doPut(long key, KeyIndexResult result);

	abstract public boolean doGet(long key);

	abstract public boolean doRemove(long key);
	
	abstract protected void doClear();

	protected long getKeyL(DbTypeBase[] columns) {
		if (this.numberOfKeyColumns == 2) {
			long keyPart1 = ((EncodedType)columns[this.indexedColumns[0]]).getKey();
			long keyPart2 = ((EncodedType)columns[this.indexedColumns[1]]).getKey();
			return (keyPart1 << 32) | (keyPart2 & 0xffffffffL);
		}
		
		return ((BigEncodedType)columns[this.indexedColumns[0]]).getKeyL();
	}
}
