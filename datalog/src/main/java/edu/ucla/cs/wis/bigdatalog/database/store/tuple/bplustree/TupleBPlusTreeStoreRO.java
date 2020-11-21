package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchKeys;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeTupleStoreLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.TupleBPlusTreeTupleStoreROStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.bytekeystuplevalues.ROBPlusTreeByteKeysTupleValues;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.intkeystuplevalues.ROBPlusTreeIntKeysTupleValues;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.longkeystuplevalues.ROBPlusTreeLongKeysTupleValues;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleBPlusTreeStoreRO 
	extends BPlusTreeTupleStore 
	implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public TupleBPlusTreeTupleStoreROStructure bPlusTree;
	
	public TupleBPlusTreeStoreRO() { super(); }
	
	public TupleBPlusTreeStoreRO(String relationName, DataType[] schema, TupleStoreConfiguration configuration, int nodeSize, 
			TypeManager typeManager) {
		super(relationName, schema, configuration.keyColumns, nodeSize, typeManager);

		this.schema = schema;
		this.initialize();
	}
	
	protected void initialize() {
		super.initialize();
		//this.nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.tuplestores.robplustree.nodesize"));
		if (this.bytesPerKey == 4)
			this.bPlusTree = new ROBPlusTreeIntKeysTupleValues(this.nodeSize, this.keyColumns, this.keyColumnTypes);
		else if (this.bytesPerKey == 8)
			this.bPlusTree = new ROBPlusTreeLongKeysTupleValues(this.nodeSize, this.keyColumns, this.keyColumnTypes);
		else
			this.bPlusTree = new ROBPlusTreeByteKeysTupleValues(this.nodeSize, this.bytesPerKey, this.keyColumns, this.keyColumnTypes);
	}
	
	public int getHeight() { return this.bPlusTree.getHeight(); }
	
	public int getNodeSize() { return this.nodeSize; }
	
	@Override
	public void add(Tuple tuple) {
		this.bPlusTree.insert(tuple);	
	}
	
	@Override
	public void update(Tuple tuple) {
		// do nothing
	}
		
	@Override
	public int getTuple(DbTypeBase[] keyColumns, Tuple tuple) {
		Tuple[] tuples = this.bPlusTree.get(keyColumns);
		if (tuples == null)
			return 0;
	
		// APS 7/16/2014
		// get get the first tuple - yes this is poorly designed
		Tuple temp = tuples[0];
		for (int i = 0; i < tuple.columns.length; i++)
			tuple.columns[i] = temp.columns[i];
		return 1;
	}
	
	public Tuple[] getTuples(DbTypeBase[] keyColumns) {
		return this.bPlusTree.get(keyColumns);
	}
	
	@Override
	public int getTuple(RangeSearchKeys<?> searchKeys, RangeSearchResultCursor cursor) {
		return 0;
	}

	@Override
	public void remove(Tuple tuple) {
		this.bPlusTree.delete(tuple);
	}

	@Override
	public void removeAll() {
		this.bPlusTree.deleteAll();
	}

	@Override
	public int commit() { return 0; }

	@Override
	public int getNumberOfTuples() {
		return this.bPlusTree.getNumberOfEntries();
	}

	@Override
	public String toString() {		
		if (this.getNumberOfTuples() == 0)
			return "Relation is empty";
		
		StringBuilder output = new StringBuilder();
		
		output.append("bytesPerKey | bytesPerValue : [" + this.bytesPerKey + " | " + this.bytesPerValue + "]");		
		output.append("key columns : " + Arrays.toString(this.keyColumns));
		output.append(this.bPlusTree.toString());
		return output.toString();
	}
	
	public ROBPlusTreeTupleStoreLeaf getFirstChild() {
		return this.bPlusTree.getFirstChild();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {
		return this.bPlusTree.getSizeOf();
	}	
	
	@Override
	public boolean sort() { return true; }
}
