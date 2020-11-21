package edu.ucla.cs.wis.bigdatalog.database.store.tuple;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;

abstract public class AddressedTupleStore 
	extends TupleStoreBase<AddressedTuple> {
	private static final long serialVersionUID = 1L;

	public AddressedTupleStore() { super(); }
	
	public AddressedTupleStore(String relationName, TypeManager typeManager) {
		super(relationName, typeManager);
	}
	
	abstract public int getFirstTupleAddress();
	
	abstract public int getLastTupleAddress();
	
	abstract public int getNumberOfTuples();
	
	// get tuple from address
	abstract public int get(int address, AddressedTuple tuple);
	
	abstract public void remove(int address);
	
	abstract public int commit();
}
