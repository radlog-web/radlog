package edu.ucla.cs.wis.bigdatalog.database.index.key;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;

abstract public class GeneralKeyKeyIndex 
	extends KeyIndex {

	protected KeyIndexResult result;

	public GeneralKeyKeyIndex(Relation<?> relation, int[] keyColumns) {
		super(relation, keyColumns);
		this.result = new KeyIndexResult();
	}

	public boolean put(Tuple tuple) {
		this.doPut(this.getKey(tuple.columns), this.result);
		return this.result.success;	
	}
	
	public boolean exists(Tuple tuple) {
		byte[] key = this.getKey(tuple.columns);
		return this.doGet(key);		
	}
	
	public boolean remove(Tuple tuple) {
		if (tuple.columns.length < this.indexedColumns.length) 
			return false;
		byte[] key = this.getKey(tuple.columns);
		return this.doRemove(key);
	}

	public boolean update(Tuple tuple) {
		boolean status = this.doRemove(this.getKey(tuple.columns));
		if (status) {
			this.doPut(this.getKey(tuple.columns), this.result);
			return this.result.success;
		}

		return status;
	}
	
	public void clear() {
		this.doClear();
	}
		
	abstract public void doPut(byte[] key, KeyIndexResult result);

	abstract public boolean doGet(byte[] key);

	abstract public boolean doRemove(byte[] key);
	
	abstract protected void doClear();
}
