package edu.ucla.cs.wis.bigdatalog.database.relation;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class QueryFormRelation extends DerivedRelation {

	private static final long serialVersionUID = 1L;

	public QueryFormRelation(String name, DataType[] schema, TupleStoreBase<AddressedTuple> tupleStore, boolean useUniqueKey, Database database) {
		super(name, RelationType.QUERYFORM, schema, tupleStore, useUniqueKey, database);
	}
	
	// APS 11/13/2013
	// this method allows anything to be added - needed for query form relations	
	public void removeUniqueIndex() {
		this.database.getIndexManager().removeIndex(this, this.uniqueIndex);
		this.uniqueIndex.clear();
		this.uniqueIndex = null; 
	}
	
	@Override
	public boolean canAdd(Tuple tuple) {
		if (this.uniqueIndex == null)
			return true;
		
		return super.canAdd(tuple);
	}
	
	@Override
	public Tuple add(Tuple newTuple, boolean isUniqueTuple) {		
		// If required, first check to see if the tuple is already present
		if (isUniqueTuple || this.canAdd(newTuple)) {
			this.tupleStore.add(newTuple);
			
			//if (DEBUG && Runtime.isDebugEnabled())	
			//	System.out.println(newTuple.toString() + " added to " + this.getName());
						
			//if (this.uniqueIndex != null)
			//	this.uniqueIndex.put(newTuple);
			
			for (int i = 0; i < this.secondaryIndexes.length; i++)
				this.secondaryIndexes[i].put(newTuple);

			return newTuple;
	    }
		return null;
	}
}
