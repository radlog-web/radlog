package edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;

public abstract class TupleRowPage
	implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int highWaterMark; // maximum number of entries so far into this page
	protected TypeManager typeManager;
	
	public TupleRowPage() {}
	
	public TupleRowPage(TypeManager typeManager) {
		this.typeManager = typeManager;
	}
	
	public int getHighWaterMark() { return this.highWaterMark; }
	
	abstract public int getHeight();
	
	abstract public int getFirstTupleAddress();
	
	abstract public int getLastTupleAddress();
	
	abstract public boolean isEmpty();
	
	abstract public boolean isFull();
	
	abstract public int readTuple(int address, AddressedTuple tuple);
	
	abstract public int appendTuple(AddressedTuple tuple);
	
	abstract public int updateTuple(int address, AddressedTuple tuple);
	
	abstract public void deleteAll();
	
	abstract public void deleteTuple(int address);
	
	abstract public int commit();
	
	abstract public MemoryMeasurement getSizeOf();
}
