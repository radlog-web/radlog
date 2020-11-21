package edu.ucla.cs.wis.bigdatalog.database.relation;

import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class RecursiveRelation extends DerivedRelation {	
	private static final long serialVersionUID = 1L;
	
	public RecursiveRelation(String relationName, DataType[] schema, TupleStoreBase<?> tupleStore, boolean useUniqueKey, Database database) {
		super(relationName, RelationType.RECURSIVE, schema, tupleStore, useUniqueKey, database);
	}
	
	public String toStringDetails() {
		StringBuilder retval = new StringBuilder();
		retval.append("  Recursive Relation: " + this.name + " ");
		retval.append(" | TupleStore: " + this.getTupleStore().getClass().getSimpleName() + "]\n");
		
		if (this.uniqueIndex != null)
			retval.append("    " + this.uniqueIndex.getClass().getSimpleName() + " unique index on all columns\n");
		
		if (this.getSecondaryIndexes() != null) {
			for (SecondaryIndex<?> index : this.getSecondaryIndexes())
				retval.append("    " + index.getClass().getSimpleName() + " secondary index on columns: " + Arrays.toString(index.getIndexedColumns()) + "\n");				
		}
		
		return retval.toString();
	}
}
