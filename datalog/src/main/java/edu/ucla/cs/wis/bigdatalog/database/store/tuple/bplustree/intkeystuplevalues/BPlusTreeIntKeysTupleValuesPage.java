package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.intkeystuplevalues;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;

public interface BPlusTreeIntKeysTupleValuesPage extends BPlusTreeElement {
	public int getLeftMostLeafKey();
	
	public void get(int key, BPlusTreeTupleValuesGetResult result);
		
	public Pair<BPlusTreeIntKeysTupleValuesPage, Boolean> insert(int key, Tuple tuple);
	
	public boolean delete(int key);

}
