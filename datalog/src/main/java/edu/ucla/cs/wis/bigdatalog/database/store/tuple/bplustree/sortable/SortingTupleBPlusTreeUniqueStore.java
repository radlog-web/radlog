package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.sortable;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysbytevalues.BPlusTreeByteKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchResult;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStorageStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// B-Tree tuple store for relations with keys and values (i.e. not only keys) 
// hope first column is not a float
public class SortingTupleBPlusTreeUniqueStore 
	extends TupleBPlusTreeUniqueStore implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int[] keySortOrder;
	
	public SortingTupleBPlusTreeUniqueStore() { super(); }
	
	public SortingTupleBPlusTreeUniqueStore(String relationName, DataType[] schema, TupleStoreConfiguration configuration, int nodeSize, 
			TypeManager typeManager) {
		super(relationName, schema, configuration, nodeSize, typeManager);
	}
	
	@Override
	protected void initializeStorageStructure(TupleStoreConfiguration configuration) {
		this.keySortOrder = configuration.keySortOrder;
		this.storageStructure = new BPlusTreeByteKeysByteValues(this.nodeSize, this.bytesPerKey, this.bytesPerValue, 
				this.keyColumns, this.keySortOrder, this.keyColumnTypes, this.valueColumns, this.valueColumnTypes, 
				this.typeManager);		
	}
	
	@Override
	public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor) {
		RangeSearchResult result = new RangeSearchResult();		

		((RangeSearchableStorageStructure<byte[]>)this.storageStructure).getTuple((byte[])searchKeys.startKey, (byte[])searchKeys.endKey, result);

		if (result.success) {
			cursor.initialize(result.leaf, result.index, searchKeys);
			return 1;
		}
		
		return 0;
	}

	@Override
	public String toString() {		
		if (this.getNumberOfTuples() == 0)
			return "Relation is empty";
		
		StringBuilder output = new StringBuilder();
		output.append("bytesPerKey | bytesPerValue : [" + this.bytesPerKey + " | " + this.bytesPerValue + "]");		
		output.append("key columns : " + Arrays.toString(this.keyColumns));
		output.append("sort order : " + Arrays.toString(this.keySortOrder));
		output.append(this.storageStructure.toString());
		return output.toString();
	}
}
