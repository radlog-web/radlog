package edu.ucla.cs.wis.bigdatalog.database.relation;

import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DerivedRelation extends Relation {
	private static final long serialVersionUID = 1L;
	
	protected Tuple templateTuple;
	
	public DerivedRelation(String name, DataType[] schema, TupleStoreBase<?> tupleStore, Database database) {
		this(name, RelationType.DERIVED, schema, tupleStore, true, database);
	}
	
	public DerivedRelation(String name, DataType[] schema, TupleStoreBase<?> tupleStore, boolean addUniqueIndex, Database database) {
		super(name, RelationType.DERIVED, schema, tupleStore, addUniqueIndex, database);
		if (this.tupleStore != null)
			this.templateTuple = this.tupleStore.getEmptyTuple();
	}
	
	public DerivedRelation(String name, RelationType relationType, DataType[] schema, TupleStoreBase<?> tupleStore, 
			boolean addUniqueIndex, Database database) {
		super(name, relationType, schema, tupleStore, addUniqueIndex, database);
		if (this.tupleStore != null)
			this.templateTuple = this.tupleStore.getEmptyTuple();
	}
	
	public int removeSimilarTuples(Tuple tuple) {		
		if (this.secondaryIndexes == null || this.secondaryIndexes.length == 0) 
			throw new DatabaseException("No index to lookup tuple to remove from this relation");

		//System.out.println("removeSimilarTuples for " + tuple.toString());
		int numberRemoved = 0;
		int numberOfColumns = 0;
		for (int i = 0; i < tuple.columns.length; i++) {
			if (tuple.columns[i] != null)
				numberOfColumns++;
		}
		
		int[] columns = new int[numberOfColumns];
		for (int i = 0; i < tuple.columns.length; i++) {
			if (tuple.columns[i] != null)
				columns[i] = i;				
		}	
		
		SecondaryIndex<?> index = this.getSecondaryIndex(columns);
		if (index != null) {
			int[] addresses = index.getSimilar(tuple);
			if (addresses != null) {
				for (int i = 0; i < addresses.length; i++) {
					if (((AddressedTupleStore)this.tupleStore).get(addresses[i], (AddressedTuple) this.templateTuple) > 0) {
						this.remove(this.templateTuple);
						numberRemoved++;
					}
				}
			}
		}
		return numberRemoved;
	}
	
	public void update(Tuple tuple) {
		this.updateTuple(tuple, tuple, true);
	}
	
	public void update(Tuple oldTuple, Tuple tuple) {
		this.updateTuple(oldTuple, tuple, true);
	}
	
	public void updateTuple(Tuple oldTuple, Tuple tuple, boolean isUnique) {
		if (oldTuple instanceof AddressedTuple)
			((AddressedTuple)tuple).address = ((AddressedTuple)oldTuple).address;
		
		// if the change will violate the keys, we fail
		// check newTuple doesn't already exist
		if (!isUnique && !this.canAdd(tuple)) {
			Tuple existingTuple = this.get(tuple);
			if (!existingTuple.equals(tuple))
				return;
		}
		
		this.tupleStore.update(tuple);
		
		if (!isUnique && this.uniqueIndex != null)
			this.uniqueIndex.update(tuple);
		
		for (int i = 0; i < this.secondaryIndexes.length; i++)
			this.secondaryIndexes[i].update(tuple);
	}
	
	public String toStringDetails() {
		StringBuilder retval = new StringBuilder();
		retval.append("  Derived Relation: " + this.name + " ");
		retval.append(" | Tuple Store: " + this.getTupleStore().getClass().getSimpleName() + "]\n");
		
		if (this.uniqueIndex != null)
			retval.append("    " + this.uniqueIndex.getClass().getSimpleName() + " unique index on all columns\n");
		
		if (this.getSecondaryIndexes() != null) {
			for (SecondaryIndex<?> index : this.getSecondaryIndexes()) {
				retval.append("    " + index.getClass().getSimpleName() + " secondary index on columns: " + Arrays.toString(index.getIndexedColumns()) + "\n");
			}
		}
		
		return retval.toString();
	}
}