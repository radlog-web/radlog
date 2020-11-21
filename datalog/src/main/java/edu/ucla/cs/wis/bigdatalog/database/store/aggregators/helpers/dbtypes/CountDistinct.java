package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.helpers.dbtypes;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.DbSet;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CountDistinct extends DbTypeAggregatorHelper implements DbTypeNeedsInitialization, Serializable {
	private static final long serialVersionUID = 1L;
	protected TypeManager typeManager;
	
	public CountDistinct(TypeManager typeManager) { 
		super(AggregateFunctionType.COUNT_DISTINCT);
		this.typeManager = typeManager;
	}

	@Override
	public DbTypeBase doAggregation(DbTypeBase oldValue, DbTypeBase newValue) {
		// here oldValue is the id to the group
		//DbSet set = (DbSet) oldValue;//new DbBPlusTree(oldValue);
		DbInteger setId = (DbInteger)oldValue;
		DbSet set = DbSet.load(setId.getValue(), this.typeManager);
		set.put(newValue);
		return setId;
	}

	@Override
	public DbTypeBase doAggregation(DbTypeBase newValue) {
		DbSet set = this.typeManager.createSet(DataType.INT);
		set.put(newValue);
		return DbInteger.create(set.getId());
	}
}