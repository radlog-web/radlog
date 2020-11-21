package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.raw;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnly;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysonly.BPlusTreeByteKeysOnlyLeaf;

public class BPlusTreeByteKeysOnlyCursor {

	public class Result {
		public byte[] value;
		
		public Result(int bytesPerKey) {
			this.value = new byte[bytesPerKey];
		}
	}
	
	protected int						keyIndex;	
	protected BPlusTreeByteKeysOnlyLeaf currentLeaf;
	protected BPlusTreeByteKeysOnly 	tree;
	protected int 						bytesPerKey;
	
	public BPlusTreeByteKeysOnlyCursor(BPlusTreeByteKeysOnly tree) {
		this.tree = tree;
		this.initialize();
	}
	
	public void initialize() {
		this.currentLeaf = this.tree.getFirstChild();
		this.keyIndex = 0;
		this.bytesPerKey = this.tree.getBytesPerKey();
	}
	
	public void reset() {
		this.initialize();
	}
	
	public int get(Result result) {
		// get leaf
		// get tuple address array
		// get all values from array
		// when out of values in array, get next array
		// when out of arrays, get next leaf
		// when out of leaves, done
		while (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				System.arraycopy(this.currentLeaf.getKeys(), (this.keyIndex * this.bytesPerKey), result.value, 0, this.bytesPerKey);
				this.keyIndex++;
				return 0;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = this.currentLeaf.getNext();
			this.keyIndex = 0;
		}
		
		return -1;
	}	

}
