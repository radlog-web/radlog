package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public class ROBPlusTreeGetResult implements Serializable {
	private static final long serialVersionUID = 1L;
	public boolean success;
	public Tuple[] tuples;
	
	public ROBPlusTreeGetResult() {}
}
