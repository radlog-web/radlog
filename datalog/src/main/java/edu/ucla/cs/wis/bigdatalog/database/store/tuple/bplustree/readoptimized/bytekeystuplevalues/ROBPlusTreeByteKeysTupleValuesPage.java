package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.bytekeystuplevalues;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;

public interface ROBPlusTreeByteKeysTupleValuesPage extends BPlusTreeElement {

	public byte[] getLeftMostLeafKey();
	
	public void get(byte[] key, ROBPlusTreeGetResult result);
		
	public Pair<ROBPlusTreeByteKeysTupleValuesPage, Boolean> insert(byte[] key, Tuple tuple);
	
	public boolean delete(byte[] key);

}
