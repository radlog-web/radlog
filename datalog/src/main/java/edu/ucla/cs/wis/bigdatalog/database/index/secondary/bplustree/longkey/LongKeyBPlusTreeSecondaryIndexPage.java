package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.longkey;

import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;

public interface LongKeyBPlusTreeSecondaryIndexPage extends BPlusTreeElement {
			
	public long getLeftMostLeafKey();
	
	public TupleAddressArray get(long key);
	
	public void insert(long key, int address, LongKeyBPlusTreeSecondaryIndexResult result);
		
	public boolean delete(long key, int address);
}
