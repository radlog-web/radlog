package edu.ucla.cs.wis.bigdatalog.database.store.aggregators;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public interface TupleAggregationStoreStructure {
	
	public int[] getKeyColumns();
	
	public DataType[] getKeyColumnTypes();
	
	public int[] getValueColumns();
	
	public DataType[] getValueColumnTypes();
	
	public int getNumberOfEntries();
	
	public void insert(Tuple tuple, AggregatorResult result);

	public int getTuple(DbTypeBase[] keys, Tuple tuple);
	
	public boolean delete(Tuple tuple);
	
	public void deleteAll();
	
	public int getNodeSize();
	
	public MemoryMeasurement getSizeOf();
}
