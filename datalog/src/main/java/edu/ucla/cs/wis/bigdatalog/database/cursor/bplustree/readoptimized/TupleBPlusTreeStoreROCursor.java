package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.readoptimized;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.SelectionCursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStoreRO;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleBPlusTreeStoreROCursor 
	extends Cursor<Tuple> 
	implements SelectionCursor<Tuple> {
	protected TupleBPlusTreeStoreRO 	tupleStore;
	protected DataType[]				schema;
	protected DbTypeBase[] 			keyValues;
	protected Tuple[]		 			tuples;
	protected int						numberOfTuples;
	protected int 						index;
	protected final int				arity;
	
	public TupleBPlusTreeStoreROCursor(Relation<Tuple> relation) {
		super(relation);
		this.tupleStore = (TupleBPlusTreeStoreRO) relation.getTupleStore();
		this.schema = this.tupleStore.getSchema();
		this.arity = this.relation.getArity();
	}

	@Override
	public void reset() { }

	@Override
	public void reset(DbTypeBase[] keyValues) {
		this.keyValues = keyValues;
		this.tuples = null;
		this.numberOfTuples = 0;
		this.index = 0;		
	}

	@Override
	public int[] getFilterColumns() { return this.tupleStore.getKeyColumns(); }

	@Override
	public DbTypeBase[] getFilter() { return this.keyValues; }
	
	@Override
	public int getTuple(Tuple tuple) {
		if (this.tuples == null) {
			this.tuples = this.tupleStore.getTuples(this.keyValues);
			if (this.tuples == null)
				return 0;
			this.numberOfTuples = this.tuples.length;
			this.index = 0;
		}

		if (this.index < this.numberOfTuples) {
			tuple.setValues(this.tuples[this.index++]);			
			return 1;
		}
		
		this.tuples = null;
		this.index = 0;
		return 0;
	}

	@Override
	public void moveNext() {this.index++;}
}
