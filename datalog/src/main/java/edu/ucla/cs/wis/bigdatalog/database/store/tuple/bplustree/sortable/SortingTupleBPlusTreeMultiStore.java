package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.sortable;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.sortable.bytekeysheap.BPlusTreeByteKeysHeapValues;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeMultiStore;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// B-Tree tuple store
// hope first column is not a float
public class SortingTupleBPlusTreeMultiStore 
	extends TupleBPlusTreeMultiStore implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int[] keySortOrder;
	
	public SortingTupleBPlusTreeMultiStore() { super(); }
	
	public SortingTupleBPlusTreeMultiStore(String relationName, DataType[] schema, TupleStoreConfiguration configuration, 
			int nodeSize, boolean useOrderedHeap, TypeManager typeManager) {
		super(relationName, schema, configuration, nodeSize, useOrderedHeap, typeManager);		
	}

	@Override
	protected void initializeStorageStructure(TupleStoreConfiguration configuration) {
		this.keySortOrder = configuration.keySortOrder;
		this.storageStructure = new BPlusTreeByteKeysHeapValues(this.nodeSize, this.bytesPerKey, this.bytesPerValue, 
				this.keyColumns, this.keySortOrder, this.keyColumnTypes, this.valueColumns, 
				this.valueColumnTypes, /*this.deALSConfiguration, */this.typeManager);
	}
	
	@Override
	public String toString() {		
		if (this.getNumberOfTuples() == 0)
			return "Relation is empty";
		
		StringBuilder output = new StringBuilder();		
		output.append("bytesPerKey | bytesPerTuple : [" + this.bytesPerKey + " | " + this.bytesPerValue + "]");		
		output.append("key columns : " + Arrays.toString(this.keyColumns));
		output.append("sort order : " + Arrays.toString(this.keySortOrder));
		output.append(this.storageStructure.toString());
		return output.toString();
	}
}
