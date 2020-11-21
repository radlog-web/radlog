package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public interface BPlusTreeSecondaryIndex<L> {

	public boolean exists(Tuple tuple);

	public boolean isEmpty();
	
	public L getFirstChild();
	
	public int getSize();
	
	public int getHeight();
	
	public int getNodeSize();
}
