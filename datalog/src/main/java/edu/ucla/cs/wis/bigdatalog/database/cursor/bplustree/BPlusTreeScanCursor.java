package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTree;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysonly.BPlusTreeIntKeysOnlyLeaf;

/*This class is for retrieving addresses from the set of modified addresses
 * APS 7/3/2014 */
public class BPlusTreeScanCursor {
	
	protected int					keyIndex;	
	protected BPlusTreeLeaf<?> 	currentLeaf;
	protected BPlusTree 			bPlusTree;

	public BPlusTreeScanCursor(BPlusTree bPlusTree) {		
		this.bPlusTree = bPlusTree;
		
		this.initialize();
	}
	
	public void initialize() {
		this.currentLeaf = this.bPlusTree.getFirstChild();
		this.keyIndex = 0;
	}
	
	public void reset() {
		this.initialize();
	}
	
	public Integer get() {
		// get leaf
		// get tuple address array
		// get all values from array
		// when out of values in array, get next array
		// when out of arrays, get next leaf
		// when out of leaves, done
		Integer element = null;
		
		while (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				element = ((BPlusTreeIntKeysOnlyLeaf)this.currentLeaf).getKeys()[this.keyIndex++];
				break;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			this.keyIndex = 0;
		}
		
		return element;
	}
}