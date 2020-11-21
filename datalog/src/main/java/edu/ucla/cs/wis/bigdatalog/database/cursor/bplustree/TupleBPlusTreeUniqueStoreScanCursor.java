package edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeByteKeysLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeGeneralLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysbytevalues.BPlusTreeIntKeysByteValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysbytevalues.BPlusTreeLongKeysByteValuesLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

public class TupleBPlusTreeUniqueStoreScanCursor 
	extends Cursor<Tuple> {

	protected TupleBPlusTreeUniqueStore tupleStore;
	protected BPlusTreeGeneralLeaf<?> currentLeaf;
	protected int		keyIndex;	
	protected int		bytesPerKey;
	protected int		bytesPerValue;
	
	public TupleBPlusTreeUniqueStoreScanCursor(Relation<Tuple> relation) {
		super(relation);
		if (!(relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore))
			throw new DatabaseException("TupleBPlusTreeUniqueStoreScanCursor can only be used on a TupleBPlusTreeUniqueStore.");
		
		this.tupleStore = (TupleBPlusTreeUniqueStore)relation.getTupleStore();
		this.initialize();
	}
	
	private void initialize() {
		this.currentLeaf = (BPlusTreeGeneralLeaf<?>) this.tupleStore.getFirstChild();
		this.keyIndex = 0;
		this.bytesPerKey = this.tupleStore.getBytesPerKey();
		this.bytesPerValue = this.tupleStore.getBytesPerValue();
	}
	
	public int getOffset() { return this.keyIndex * this.bytesPerValue; }
	
	public byte[] getValues() { return this.currentLeaf.getValues(); }
	
	public void reset() {
		this.initialize();
	}
	
	public int getTuple(Tuple tuple) {
		// get leaf
		// get key & values (heap)
		// get all values from heap
		// when out of values in heap, get next heap
		// when out of heaps, get next leaf
		// when out of leaves, done
		byte[] 	valueBytes;
		while (this.currentLeaf != null) {
			if (this.keyIndex < this.currentLeaf.getHighWaterMark()) {
				
				valueBytes = new byte[this.bytesPerValue];
				System.arraycopy(this.currentLeaf.getValues(), this.keyIndex * this.bytesPerValue, valueBytes, 0, this.bytesPerValue);
								
				if (this.currentLeaf instanceof BPlusTreeByteKeysLeaf) {
					byte[] keyBytes = new byte[this.bytesPerKey];
					System.arraycopy(((BPlusTreeByteKeysLeaf)this.currentLeaf).getKeys(), 
							this.keyIndex * this.bytesPerKey, keyBytes, 0, this.bytesPerKey);
					if (this.tupleStore.loadTuple(keyBytes, valueBytes, tuple) > 0) {
						this.keyIndex++;
						return 1;
					}
				} else if (this.currentLeaf instanceof BPlusTreeLongKeysByteValuesLeaf) {
					long key = ((BPlusTreeLongKeysByteValuesLeaf)this.currentLeaf).getKeys()[this.keyIndex];
					if (this.tupleStore.loadTuple(key, valueBytes, tuple) > 0) {
						this.keyIndex++;
						return 1;
					}
				} else {
					int key = ((BPlusTreeIntKeysByteValuesLeaf)this.currentLeaf).getKeys()[this.keyIndex];
					if (this.tupleStore.loadTuple(key, valueBytes, tuple) > 0) {
						this.keyIndex++;
						return 1;
					}
				}
				
				this.keyIndex++;
				break;
			}
			
			// out of keys, so move to next leaf
			this.currentLeaf = (BPlusTreeGeneralLeaf<?>) this.currentLeaf.getNext();
			this.keyIndex = 0;						
		}

		return 0;
	}	
	
	@Override
	public void moveNext() { this.keyIndex++; }
}