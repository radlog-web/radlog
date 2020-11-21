package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;

public class FSSum extends DbTypeAggregatorHelper implements Serializable {
	private static final long serialVersionUID = 1L;
	protected TypeManager typeManager;
	public FSSum(TypeManager typeManager) { 
		super(AggregateFunctionType.FSSUM);
		this.typeManager = typeManager;
	}
	
	public DbTypeBase doAggregation(DbTypeBase oldValue, DbTypeBase newValue) {
		if (newValue.greaterThan(DbInteger.create(0)) && newValue.greaterThan(oldValue))
			return newValue;
		return oldValue;
	}
}