package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import java.io.Serializable;
import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public class BPlusTreeTupleValuesGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean status;
	public ArrayList<Tuple> tuples;
	
	public BPlusTreeTupleValuesGetResult() {}
}
