package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.bytekeystuplevalues;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;

public interface BPlusTreeByteKeysTupleValuesPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public void get(byte[] key, BPlusTreeTupleValuesGetResult result);

	public Pair<BPlusTreeByteKeysTupleValuesPage, Boolean> insert(byte[] key, Tuple tuple);
	
	public boolean delete(byte[] key);

}
