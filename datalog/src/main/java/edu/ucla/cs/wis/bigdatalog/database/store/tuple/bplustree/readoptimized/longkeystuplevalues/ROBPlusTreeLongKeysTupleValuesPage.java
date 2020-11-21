package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.longkeystuplevalues;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;

public interface ROBPlusTreeLongKeysTupleValuesPage extends BPlusTreeElement {

	public long getLeftMostLeafKey();
	
	public void get(long key, ROBPlusTreeGetResult result);
		
	public Pair<ROBPlusTreeLongKeysTupleValuesPage, Boolean> insert(long key, Tuple tuple);
	
	public boolean delete(long key);

}
