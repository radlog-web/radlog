package edu.ucla.cs.wis.bigdatalog.database.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree.GeneralKeyBPlusTreeKeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.key.hash.GeneralKeyHashKeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.key.hash.LongKeyHashKeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.general.GeneralKeyBPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.integerkey.IntegerKeyBPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.longkey.LongKeyBPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.hash.GeneralKeyHashSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.hash.IntegerKeyHashSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.hash.LongKeyHashSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.relation.RelationType;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.exception.DeALSException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class IndexManager {
	
	public enum IndexType { Default, Hash, BPlusTree; }
	
	private HashMap<Relation<?>, List<Index<?>>> indexes;

	private DeALSConfiguration config;
	
	public IndexManager(DeALSContext deALSContext) {
		config = deALSContext.getConfiguration();
	}
	
	public HashMap<Relation<?>, List<Index<?>>> getIndexes() {
		if (this.indexes == null)
			this.indexes = new HashMap<>();
			
		return this.indexes;
	}
	
	public void addIndex(Relation<?> relation, Index<?> index) {
		if (this.indexes == null)
			this.indexes = new HashMap<>();
			
		List<Index<?>> relationIndexes = new ArrayList<>();
		if (this.indexes.containsKey(relation))
			relationIndexes = this.indexes.get(relation);
		
		if (!relationIndexes.contains(index))
			relationIndexes.add(index);
		
		this.indexes.put(relation,  relationIndexes);		
	}
	
	public void removeIndex(Relation<?> relation, Index<?> index) {
		List<Index<?>> relationIndexes = this.indexes.get(relation);
		relationIndexes.remove(index);
	}
	
	public void clearNonBaseIndexes() {
		if (this.indexes == null)
			return;
		
		List<Relation<?>> removeRelations = new ArrayList<>();
		for (Map.Entry<Relation<?>, List<Index<?>>> entry : this.indexes.entrySet()) {
			if (entry.getKey().getRelationType() != RelationType.BASE) 
				removeRelations.add(entry.getKey());
		}
		
		for (Relation<?> relation : removeRelations)
			this.indexes.remove(relation);		
	}
	
	public void clear() {
		if (this.indexes != null)
			this.indexes.clear();
		this.indexes = null;
	}
	
	public void resetIndexCounters() {
		if (this.indexes != null)
			for (Map.Entry<Relation<?>, List<Index<?>>> entry : this.indexes.entrySet())
				for (Index<?> index : entry.getValue()) 
					index.resetCounters();		
	}
	
	public SecondaryIndex<?> createSecondaryIndex(Relation<AddressedTuple> relation, int[] columns) {
		return createSecondaryIndex(relation, columns, IndexType.Default);
	}
	
	public SecondaryIndex<?> createSecondaryIndex(Relation<AddressedTuple> relation, int[] columns, IndexType indexType) {
		return createSecondaryIndex(relation, columns, indexType, true);
	}
	
	public SecondaryIndex<?> createSecondaryIndex(Relation<AddressedTuple> relation, int[] columns, IndexType indexType, boolean build) {
		SecondaryIndex<?> index = null;
		
		if (indexType == IndexType.Default) {
			if (this.config.compareProperty("deals.database.indexes.secondary.type", "bplustree"))
				indexType = IndexType.BPlusTree;
			else
				indexType = IndexType.Hash;
		}
		
		switch(indexType) {
			case BPlusTree:
				int nodeSize = Integer.parseInt(this.config.getProperty("deals.database.indexes.bplustree.nodesize"));
				if (columns.length == 1) {
					DataType columnType = relation.getSchema()[columns[0]];
					if ((columnType == DataType.INT) || (columnType == DataType.STRING))
						index = new IntegerKeyBPlusTreeSecondaryIndex(relation, columns[0], nodeSize);
					else if (columnType == DataType.LONG)						
						index = new LongKeyBPlusTreeSecondaryIndex(relation, columns, nodeSize);
				} else if (columns.length == 2) {
					DataType columnType1 = relation.getSchema()[columns[0]];
					DataType columnType2 = relation.getSchema()[columns[1]];
					if ((columnType1 == DataType.INT || columnType1 == DataType.STRING) && 
							(columnType2 == DataType.STRING || columnType2 == DataType.INT))
					index = new LongKeyBPlusTreeSecondaryIndex(relation, columns, nodeSize);
				}
				
				if (index == null)
					index = new GeneralKeyBPlusTreeSecondaryIndex(relation, columns, nodeSize);
				break;
			default:
				double splitPolicy = Double.parseDouble(this.config.getProperty("deals.database.indexes.hash.splitpolicy"));
				int directorySize = Integer.parseInt(this.config.getProperty("deals.database.indexes.hash.directorysize"));
				int segmentSize = Integer.parseInt(this.config.getProperty("deals.database.indexes.hash.segmentsize"));
				int numberOfInitialBuckets = Integer.parseInt(this.config.getProperty("deals.database.indexes.hash.numberofinitialbuckets"));
				
				if (columns.length == 1) {
					DataType columnType = relation.getSchema()[columns[0]];
				
					if ((columnType == DataType.INT) || (columnType == DataType.STRING))
						index = new IntegerKeyHashSecondaryIndex(relation, columns[0], splitPolicy, directorySize, segmentSize, numberOfInitialBuckets);				
				} else if (columns.length == 2) {
					DataType columnType1 = relation.getSchema()[columns[0]];
					DataType columnType2 = relation.getSchema()[columns[1]];
					if (((columnType1 == DataType.INT) || (columnType1 == DataType.STRING))
						&& ((columnType2 == DataType.INT) || (columnType2 == DataType.STRING)))
						index = new LongKeyHashSecondaryIndex(relation, columns, splitPolicy, directorySize, segmentSize, numberOfInitialBuckets);
				}
				
				if (index == null)
					index = new GeneralKeyHashSecondaryIndex(relation, columns, splitPolicy, directorySize, segmentSize, numberOfInitialBuckets);
				//System.out.println(index.getClass().getSimpleName() + " " + Arrays.toString(columns));
				break;
		}
		
		if (build)
			index.build();

		addIndex(relation, index);
		
		return index;
	}
	
	public BPlusTreeSecondaryIndex<?> createBPlusTreeSecondaryIndex(Relation<AddressedTuple> relation, int[] columns) {
		SecondaryIndex<?> index = null;
		int nodeSize = Integer.parseInt(this.config.getProperty("deals.database.indexes.bplustree.nodesize"));
		if (relation.getTupleStore() instanceof AddressedTupleStore) {
			if (columns.length == 1) {
				DataType columnType = relation.getSchema()[columns[0]];
				if ((columnType == DataType.INT) || (columnType == DataType.STRING))
					index = new IntegerKeyBPlusTreeSecondaryIndex(relation, columns[0], nodeSize);
				else if (columnType == DataType.LONG)
					index = new LongKeyBPlusTreeSecondaryIndex(relation, columns, nodeSize);
				
			} else if (columns.length == 2) {
				DataType columnType1 = relation.getSchema()[columns[0]];
				DataType columnType2 = relation.getSchema()[columns[1]];
				if ((columnType1 == DataType.INT || columnType1 == DataType.STRING) && 
						(columnType2 == DataType.STRING || columnType2 == DataType.INT))
				index = new LongKeyBPlusTreeSecondaryIndex(relation, columns, nodeSize);
			}
			
			if (index == null)
				index = new GeneralKeyBPlusTreeSecondaryIndex(relation, columns, nodeSize);
		} else {
			if (columns.length == 2) {
				/*DataType columnType1 = relation.getSchema()[columns[0]];
				DataType columnType2 = relation.getSchema()[columns[1]];
				if ((columnType1 == DataType.INTEGER || columnType1 == DataType.STRING) && 
						(columnType2 == DataType.STRING || columnType2 == DataType.INTEGER))
				index = new LongKeyOnlyBPlusTreeSecondaryIndex(relation, columns);*/
				throw new DeALSException("why use this?");
			}
		}

		addIndex(relation, index);
		return (BPlusTreeSecondaryIndex<?>)index;
	}
		
	public KeyIndex<?> createKeyIndex(Relation<?> relation, int[] columns) {
		KeyIndex<?> index;
		if (this.config.compareProperty("deals.database.indexes.key.type", "bplustree")) {
			int nodeSize = Integer.parseInt(this.config.getProperty("deals.database.indexes.bplustree.nodesize"));

			if ((columns.length == 1) && (relation.getSchema()[columns[0]] == DataType.LONG))
				index = new edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree.LongKeyBPlusTreeKeyIndex(relation, columns, nodeSize);
			else if ((columns.length == 2)
					&& ((relation.getSchema()[columns[0]].getNumberOfBytes() 
							+ relation.getSchema()[columns[1]].getNumberOfBytes()) == DataType.LONG.getNumberOfBytes()))
					index = new edu.ucla.cs.wis.bigdatalog.database.index.key.bplustree.LongKeyBPlusTreeKeyIndex(relation, columns, nodeSize);
			else
			index = new GeneralKeyBPlusTreeKeyIndex(relation, columns, nodeSize);
		} else {
			double splitPolicy = Double.parseDouble(this.config.getProperty("deals.database.indexes.hash.splitpolicy"));
			int directorySize = Integer.parseInt(this.config.getProperty("deals.database.indexes.hash.directorysize"));
			int segmentSize = Integer.parseInt(this.config.getProperty("deals.database.indexes.hash.segmentsize"));
			int numberOfInitialBuckets = Integer.parseInt(this.config.getProperty("deals.database.indexes.hash.numberofinitialbuckets"));
			
			if ((columns.length == 1) && (relation.getSchema()[columns[0]] == DataType.LONG))
				index = new LongKeyHashKeyIndex(relation, columns, splitPolicy, directorySize, segmentSize, numberOfInitialBuckets);
			else if ((columns.length == 2) 
					&& ((relation.getSchema()[columns[0]].getNumberOfBytes() 
							+ relation.getSchema()[columns[1]].getNumberOfBytes()) == DataType.LONG.getNumberOfBytes()))
				index = new LongKeyHashKeyIndex(relation, columns, splitPolicy, directorySize, segmentSize, numberOfInitialBuckets);
			else
				index = new GeneralKeyHashKeyIndex(relation, columns, splitPolicy, directorySize, segmentSize, numberOfInitialBuckets);
		}
		addIndex(relation, index);
		return index;
	}
	
	public static boolean isMatch(int[] indexedColumns1, int[] indexedColumns2) {
		if (indexedColumns1 == null || indexedColumns1.length == 0)
			return false;
		
		if (indexedColumns2 == null || indexedColumns2.length == 0)
			return false;
		
		for (int column : indexedColumns1) {
			boolean found = false;
			for (int indexedColumn : indexedColumns2) {
				if (column == indexedColumn)
					found = true;
			}
			if (!found)
				return false;
		}
		
		for (int column : indexedColumns2) {
			boolean found = false;
			for (int indexedColumn : indexedColumns1) {
				if (column == indexedColumn)
					found = true;
			}
			if (!found)
				return false;
		}
	
		return true;
	}
	
	public static boolean isRangeQueryMatch(int[] indexedColumns1, int[] indexedColumns2) {
		if (indexedColumns1 == null || indexedColumns1.length == 0)
			return false;
		
		if (indexedColumns2 == null || indexedColumns2.length == 0)
			return false;
		
		if (indexedColumns1.length == indexedColumns2.length)
			return false;
		
		if (indexedColumns1.length < indexedColumns2.length)
			return false;

		for (int i = 0; i < indexedColumns2.length; i++)
			if (indexedColumns1[i] != indexedColumns2[i])
				return false;
		
		return true;
	}
}
