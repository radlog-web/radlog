package edu.ucla.cs.wis.bigdatalog.database.store.tuple;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

abstract public class TupleStoreBase<T>
	implements MemorySize, Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public final boolean DEBUG = false;
	protected String relationName;
	protected TypeManager typeManager;
	
	public TupleStoreBase() { }
	
	public TupleStoreBase(String relationName, TypeManager typeManager) {
		this.relationName = relationName;
		this.typeManager = typeManager;
	}
	
	// append tuple to end of store and receive address for retrieval later
	abstract public void add(T tuple);
	
	// update tuple in store
	abstract public void update(T tuple);
	
	// find if tuple exists
	abstract public T exists(T tuple);
		
	abstract public void remove(T tuple);
	
	abstract public void removeAll();
	
	abstract public int commit();
	
	abstract public int getNumberOfTuples();
	
	abstract public String toString();
	
	abstract public MemoryMeasurement getSizeOf();
	
	public void resetCounters(){}
	
	abstract public boolean sort();
	
	abstract public Tuple getEmptyTuple();
}
