package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.intkeystuplevalues;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;

public interface ROBPlusTreeIntKeysTupleValuesPage extends BPlusTreeElement {

	public int getLeftMostLeafKey();
	
	public void get(int key, ROBPlusTreeGetResult result);
		
	public Pair<ROBPlusTreeIntKeysTupleValuesPage, Boolean> insert(int key, Tuple tuple);
	
	public boolean delete(int key);

}
