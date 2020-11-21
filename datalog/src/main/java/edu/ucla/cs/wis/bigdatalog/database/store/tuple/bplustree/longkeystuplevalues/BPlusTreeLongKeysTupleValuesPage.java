package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.longkeystuplevalues;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;

public interface BPlusTreeLongKeysTupleValuesPage extends BPlusTreeElement {

	public long getLeftMostLeafKey();
	
	public void get(long key, BPlusTreeTupleValuesGetResult result);

	public Pair<BPlusTreeLongKeysTupleValuesPage, Boolean> insert(long key, Tuple tuple);
	
	public boolean delete(long key);

}
