package edu.ucla.cs.wis.bigdatalog.database.store.tuple;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;

public class TupleStoreConfiguration implements Serializable {		
	private static final long serialVersionUID = 1L;
	
	public TupleStoreType tupleStoreType;
	public int[] keyColumns;
	public boolean uniqueValue;
	public boolean trackModifiedTuples;
	public AggregateInfo[] aggregateInfos;
	public int[] keySortOrder;
	
	public TupleStoreConfiguration() {}
	
	public void setUniqueValue(boolean uniqueValue) {
		this.uniqueValue = uniqueValue;
	}

	public void setTrackModifiedTuples(boolean trackModifiedTuples) {
		this.trackModifiedTuples = trackModifiedTuples;
	}

	public void setAggregateInfos(AggregateInfo[] aggregateInfos) {
		this.aggregateInfos = aggregateInfos;
	}
	
	public TupleStoreConfiguration(TupleStoreType tupleStoreType) {
		this(tupleStoreType, null);		
	}
	
	public TupleStoreConfiguration(TupleStoreType tupleStoreType, int[] keyColumns) {
		this(tupleStoreType, keyColumns, null);
	}
	
	public TupleStoreConfiguration(TupleStoreType tupleStoreType, int[] keyColumns, int[] keySortOrder) {
		this.tupleStoreType = tupleStoreType;
		this.keyColumns = keyColumns;
		
		if (keySortOrder != null)
			this.keySortOrder = keySortOrder;
		
		switch (this.tupleStoreType) {
			case BPlusTree:
			case BPlusTreeKeysOnly:
			case TupleBPlusTree:
			case SortingBPlusTree:
				if ((this.keyColumns == null) || (this.keyColumns.length == 0))
					throw new DatabaseException("BPlusTree tuple stores must have key columns specified.");
				
				if (this.tupleStoreType == TupleStoreType.SortingBPlusTree)
					if ((this.keySortOrder == null) || (this.keySortOrder.length == 0))
						throw new DatabaseException("Sortable BPlusTree tuple stores must have key column sort order specified.");
		}
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Tuple Store Type: " + this.tupleStoreType.name());
		retval.append(" | key columns: " + Arrays.toString(this.keyColumns));
		retval.append(" | unique value: " + this.uniqueValue);
		retval.append(" | track insertions: " + this.trackModifiedTuples);
		retval.append(" | key sort order: " + Arrays.toString(this.keySortOrder));
		return retval.toString();
	}
}