package edu.ucla.cs.wis.bigdatalog.database.relation;

import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.AggregatorResult;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AggregateRelation extends DerivedRelation {
	private static final long serialVersionUID = 1L;

	public AggregateRelation(String relationName, DataType[] schema, TupleAggregationStore tupleStore, Database database) {
		super(relationName, RelationType.AGGREGATE, schema, tupleStore, false, database);
	}
	
	public void put(Tuple newTuple, AggregatorResult result) {
		((TupleAggregationStore)this.tupleStore).put(newTuple, result);
	}	
	
	public void setName(String name) { this.name = name; }
}
