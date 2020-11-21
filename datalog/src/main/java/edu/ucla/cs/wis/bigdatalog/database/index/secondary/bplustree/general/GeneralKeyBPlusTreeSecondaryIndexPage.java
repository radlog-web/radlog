package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.general;

import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface GeneralKeyBPlusTreeSecondaryIndexPage extends BPlusTreeElement {
	public byte[] getLeftMostLeafKey();
	
	public TupleAddressArray get(byte[] key);
	
	public void insert(byte[] key, int address, GeneralKeyBPlusTreeSecondaryIndexResult result);
	
	public boolean delete(byte[] key, int address);
}
