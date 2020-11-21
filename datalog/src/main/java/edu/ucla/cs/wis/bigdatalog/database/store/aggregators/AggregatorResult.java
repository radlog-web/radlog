package edu.ucla.cs.wis.bigdatalog.database.store.aggregators;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AggregatorResult implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public AggregatorInsertStatus status;
	public DbTypeBase newTotalValue;
	
	public AggregatorResult(){}
	
	public String toString() {
		return status + " " + newTotalValue;
	}
}
