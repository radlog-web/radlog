package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;

public class Count extends DbTypeAggregatorHelper implements DbTypeNeedsInitialization, Serializable {
	private static final long serialVersionUID = 1L;
	protected TypeManager typeManager;
	public Count(TypeManager typeManager) { 
		super(AggregateFunctionType.COUNT); 
		this.typeManager = typeManager;
	}
	
	public DbTypeBase doAggregation(DbTypeBase oldValue, DbTypeBase newValue) {
		return ((DbNumericType) oldValue).add(this.typeManager.castToCountDataType(1));
	}

	@Override
	public DbTypeBase doAggregation(DbTypeBase newValue) {
		return this.typeManager.castToCountDataType(1);
	}
}