package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.integerkey;

import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface IntegerKeyBPlusTreeSecondaryIndexPage extends BPlusTreeElement {
	
	public int getLeftMostLeafKey();
	
	public TupleAddressArray get(int key);
	
	public void insert(int key, int address, IntegerKeyBPlusTreeSecondaryIndexResult result);

	public boolean delete(int key, int address);
}
