package edu.ucla.cs.wis.bigdatalog.database.cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Database;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate.AggregateAggregatorScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate.AggregateBPlusTreeScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate.AggregateIndexCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.aggregate.AggregateHeapScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.arraylist.ArrayListScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeIndexScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeKeysOnlyStoreCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeKeysOnlyStoreScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeMultiStoreCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeMultiStoreScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeStoreCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeStoreScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeUniqueStoreCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeUniqueStoreFilteredScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.TupleBPlusTreeUniqueStoreScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.RangeSearchCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.forest.AggregatorBPlusTreeForestIntKeysIntValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.forest.AggregatorBPlusTreeForestLongKeysIntValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeByteKeysDbTypeValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeIntKeysDbTypeValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeIntKeysFloatValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeIntKeysIntValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeLongKeysDbTypeValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeLongKeysFloatValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeLongKeysIntValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.AggregatorBPlusTreeStoreScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.scan.TupleAggregationStoreFilteredScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.sssp.SSSPBPlusTreeForestIntKeysIntValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.aggregationstores.sssp.SSSPBPlusTreeForestLongKeysIntValuesScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.GeneralChangeTrackerScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.IntBitmapChangeTrackerScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.IntIntBitmapChangeTrackerScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.IntTreeChangeTrackerScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.ChangeTrackerCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.changetracker.LongTreeChangeTrackerScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeByteKeysDbTypeValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeIntKeysDbTypeValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeIntKeysFloatValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeIntKeysIntValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeLongKeysDbTypeValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeLongKeysFloatValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.AggregatorBPlusTreeLongKeysIntValuesRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.RangeSearchResultCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.TupleBPlusTreeUniqueStoreByteKeysRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.TupleBPlusTreeUniqueStoreIntKeysRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.rangesearch.TupleBPlusTreeUniqueStoreLongKeysRangeSearchResultScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.readoptimized.TupleBPlusTreeStoreROCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.bplustree.readoptimized.TupleBPlusTreeStoreROScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.cursor.heap.HeapScanCursor;
import edu.ucla.cs.wis.bigdatalog.database.index.IndexManager;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.SecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndexLeaf;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.bytekeysbytevalues.BPlusTreeByteKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.intkeysbytevalues.BPlusTreeIntKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.longkeysbytevalues.BPlusTreeLongKeysByteValues;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.rangesearch.RangeSearchableStore;
import edu.ucla.cs.wis.bigdatalog.database.store.changetracking.ChangeTracker;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypedbkeyvalues.AggregatorBPlusTreeByteKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.bytekeysdbtypevalues.AggregatorBPlusTreeByteKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.forests.AggregatorBPlusTreeForestIntKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.forests.AggregatorBPlusTreeForestLongKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypedbkeyvalues.AggregatorBPlusTreeIntKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysdbtypevalues.AggregatorBPlusTreeIntKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysfloatvalues.AggregatorBPlusTreeIntKeysFloatValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.intkeysintvalues.AggregatorBPlusTreeIntKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypedbkeyvalues.AggregatorBPlusTreeLongKeysDbTypeDbKeyValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysdbtypevalues.AggregatorBPlusTreeLongKeysDbTypeValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysfloatvalues.AggregatorBPlusTreeLongKeysFloatValues;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree.longkeysintvalues.AggregatorBPlusTreeLongKeysIntValues;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeKeysOnlyStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeMultiStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStoreRO;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateInfo;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CursorManager {
	private HashMap<Relation<?>, List<Cursor<?>>> cursors;
	private Database database;
	
	public CursorManager(Database database) {
		this.database = database;
	}
	
	public HashMap<Relation<?>, List<Cursor<?>>> getCursors() {
		if (this.cursors == null)
			this.cursors = new HashMap<>();
			
		return this.cursors;
	}
	
	public void addCursor(Relation<?> relation, Cursor<?> cursor) {
		if (this.cursors == null)
			this.cursors = new HashMap<>();
			
		List<Cursor<?>> relationCursors = new ArrayList<>();
		if (this.cursors.containsKey(relation))
			relationCursors = this.cursors.get(relation);
		
		if (!relationCursors.contains(cursor))
			relationCursors.add(cursor);
		
		this.cursors.put(relation,  relationCursors);		
	}
	
	public void destroyCursor(Relation<?> relation, Cursor<?> cursor) {
		if (this.cursors.containsKey(relation))
			this.cursors.get(relation).remove(cursor);		
	}
	
	public void clear() {
		if (this.cursors != null)
			this.cursors.clear();
		this.cursors = null;
	}
	
	public void resetCursorCounters() {
		if (this.cursors != null) {
			for (Map.Entry<Relation<?>, List<Cursor<?>>> entry : this.cursors.entrySet())
				for (Cursor<?> cursor : entry.getValue()) 
					if (cursor != null)
						cursor.resetCounters();
		}
	}
	
	public Cursor<?> createCursor(Relation<?> relation, int[] filterColumns) {
		if (filterColumns == null || filterColumns.length == 0)
			return createScanCursor(relation);
		
		// we don't add secondary indexes to bplustree tuplestores
		if (relation.getTupleStore() instanceof BPlusTreeTupleStore) {
			BPlusTreeTupleStore ts = ((BPlusTreeTupleStore)relation.getTupleStore());				
			int[] keyColumns = ts.getKeyColumns(); 
			
			if (IndexManager.isMatch(keyColumns, filterColumns)) {
				Cursor<?> cursor = (Cursor<?>)createIndexCursor(relation, filterColumns);
				addCursor(relation, cursor);
				return cursor;
			} else if (IndexManager.isRangeQueryMatch(keyColumns, filterColumns)) {
				Cursor<?> cursor = (Cursor<?>)createRangeSearchResultCursor(relation);
				cursor = new RangeSearchCursor(relation);
				addCursor(relation, cursor);
				return cursor;
			}
		} else {
			SecondaryIndex<?> index = relation.getSecondaryIndex(filterColumns);		
			if (index == null) {
				// if base relation, add index (which also builds the index)
				// if derived relation or recursive relation, its more complicated...
				//if (relation.getRelationType() == RelationType.BASE)
					index = relation.addSecondaryIndex(filterColumns);

				if (index == null)
					throw new DatabaseException("Index missing on relation " + relation.getName());
			}

			return (Cursor<?>)createIndexCursor(relation, filterColumns);
		}
		
		Cursor<?> cursor = createFilteredScanCursor(relation, filterColumns);
		addCursor(relation, cursor);
		
		return cursor;
	}
	
	public FilteredScanCursor<?> createFilteredScanCursor(Relation relation, int[] filterColumns) {
		Cursor<?> cursor = null;
		if (relation.getTupleStore() instanceof TupleUnorderedHeapStore)
			cursor = new FilteredScanCursor<>(relation, filterColumns);
		else if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore)
			cursor = new TupleBPlusTreeUniqueStoreFilteredScanCursor(relation, filterColumns);
		else if (relation.getTupleStore() instanceof TupleAggregationStore)
			cursor = new TupleAggregationStoreFilteredScanCursor(relation, filterColumns);
		return (FilteredScanCursor<?>) cursor;
	}
	
	public SelectionCursor<?> createIndexCursor(Relation<?> relation, int[] filterColumns) {
		SelectionCursor<?> cursor;
		if (relation.getTupleStore() instanceof TupleBPlusTreeMultiStore) {
			cursor = new TupleBPlusTreeMultiStoreCursor((Relation<Tuple>)relation);
		} else  if (relation.getTupleStore() instanceof TupleBPlusTreeKeysOnlyStore) {
			cursor = new TupleBPlusTreeKeysOnlyStoreCursor((Relation<Tuple>) relation);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore) {
			cursor = new TupleBPlusTreeUniqueStoreCursor((Relation<Tuple>)relation);
		} else if (relation.getTupleStore() instanceof TupleAggregationStore) {
			cursor = new RangeSearchCursor(relation);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeStore) {
			cursor = new TupleBPlusTreeStoreCursor((Relation<Tuple>) relation);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeStoreRO) {
			cursor = new TupleBPlusTreeStoreROCursor((Relation<Tuple>) relation);
		} else {
			relation.addSecondaryIndex(filterColumns);
			cursor = new IndexCursor((Relation<AddressedTuple>) relation, filterColumns);
		}
		addCursor(relation, (Cursor<?>)cursor);
		return cursor;
	}
	
	public Cursor<?> createScanCursor(Relation<?> relation) {
		Cursor<?> cursor;
		if (relation.getTupleStore() instanceof TupleUnorderedHeapStore) {
			cursor = new HeapScanCursor((Relation<AddressedTuple>) relation);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeMultiStore) {
			cursor = new TupleBPlusTreeMultiStoreScanCursor((Relation<Tuple>)relation);
		} else  if (relation.getTupleStore() instanceof TupleBPlusTreeKeysOnlyStore) {
			cursor = new TupleBPlusTreeKeysOnlyStoreScanCursor((Relation<Tuple>)relation);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore) {
			cursor = new TupleBPlusTreeUniqueStoreScanCursor((Relation<Tuple>)relation);
		} else if (relation.getTupleStore() instanceof TupleAggregationStore) {
			//if (((TupleAggregationStore)relation.getTupleStore()).storageStructure instanceof BPlusTree)
			return this.createBPlusTreeAggregationStoreScanCursor((AggregateRelation) relation);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeStore) {
			cursor = new TupleBPlusTreeStoreScanCursor((Relation<Tuple>) relation);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeStoreRO) {
			cursor = new TupleBPlusTreeStoreROScanCursor((Relation<Tuple>) relation);
		} else {
			cursor = new ArrayListScanCursor((Relation<AddressedTuple>) relation);
		}
		
		addCursor(relation, cursor);
		
		return cursor;
	}
	
	public AggregatorBPlusTreeStoreScanCursor<?,?> createBPlusTreeAggregationStoreScanCursor(AggregateRelation relation) {
		AggregatorBPlusTreeStoreScanCursor<?,?> cursor = null;
		TupleAggregationStoreStructure tass = ((TupleAggregationStore)relation.getTupleStore()).storageStructure;
		if (tass instanceof AggregatorBPlusTreeIntKeysIntValues)
			cursor = new AggregatorBPlusTreeIntKeysIntValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeIntKeysFloatValues)
			cursor = new AggregatorBPlusTreeIntKeysFloatValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeLongKeysIntValues)
			cursor = new AggregatorBPlusTreeLongKeysIntValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeLongKeysFloatValues)
			cursor = new AggregatorBPlusTreeLongKeysFloatValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeIntKeysDbTypeValues)
			cursor = new AggregatorBPlusTreeIntKeysDbTypeValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeLongKeysDbTypeValues)
			cursor = new AggregatorBPlusTreeLongKeysDbTypeValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeIntKeysDbTypeDbKeyValues)
			cursor = new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeLongKeysDbTypeDbKeyValues)
			cursor = new AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeForestLongKeysIntValues)
			cursor = new AggregatorBPlusTreeForestLongKeysIntValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeForestIntKeysIntValues)
			cursor = new AggregatorBPlusTreeForestIntKeysIntValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeByteKeysDbTypeValues)
			cursor = new AggregatorBPlusTreeByteKeysDbTypeValuesScanCursor(relation);
		else if (tass instanceof AggregatorBPlusTreeByteKeysDbTypeDbKeyValues)
			cursor = new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesScanCursor(relation);
		addCursor(relation, cursor);
		
		return cursor;
	}
	
	public RangeSearchResultCursor<?,?,?> createRangeSearchResultCursor(Relation<?> relation) {
		if (relation.getTupleStore() instanceof RangeSearchableStore) {
			Cursor<?> cursor = null;
			if (relation.getTupleStore() instanceof TupleAggregationStore) {
				TupleAggregationStoreStructure tass = ((TupleAggregationStore)relation.getTupleStore()).storageStructure;
				if (tass instanceof AggregatorBPlusTreeLongKeysDbTypeDbKeyValues)
					cursor = new AggregatorBPlusTreeLongKeysDbTypeDbKeyValuesRangeSearchResultScanCursor((AggregateRelation) relation);				
				else if (tass instanceof AggregatorBPlusTreeLongKeysIntValues)
					cursor = new AggregatorBPlusTreeLongKeysIntValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeLongKeysFloatValues)
					cursor = new AggregatorBPlusTreeLongKeysFloatValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeLongKeysDbTypeValues)
					cursor = new AggregatorBPlusTreeLongKeysDbTypeValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeIntKeysIntValues)
					cursor = new AggregatorBPlusTreeIntKeysIntValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeIntKeysFloatValues)
					cursor = new AggregatorBPlusTreeIntKeysFloatValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeIntKeysDbTypeValues)
					cursor = new AggregatorBPlusTreeIntKeysDbTypeValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeIntKeysDbTypeDbKeyValues)
					cursor = new AggregatorBPlusTreeIntKeysDbTypeDbKeyValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeByteKeysDbTypeDbKeyValues)
					cursor = new AggregatorBPlusTreeByteKeysDbTypeDbKeyValuesRangeSearchResultScanCursor((AggregateRelation) relation);
				else if (tass instanceof AggregatorBPlusTreeByteKeysDbTypeValues)
					cursor = new AggregatorBPlusTreeByteKeysDbTypeValuesRangeSearchResultScanCursor((AggregateRelation) relation);
			} else {
				if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore) {
					TupleBPlusTreeStoreStructure storageStructure = ((TupleBPlusTreeUniqueStore)relation.getTupleStore()).storageStructure;
					if (storageStructure instanceof BPlusTreeIntKeysByteValues)
						cursor = new TupleBPlusTreeUniqueStoreIntKeysRangeSearchResultScanCursor(relation);
					else if (storageStructure instanceof BPlusTreeLongKeysByteValues)
						cursor = new TupleBPlusTreeUniqueStoreLongKeysRangeSearchResultScanCursor(relation);
					else if (storageStructure instanceof BPlusTreeByteKeysByteValues)
						cursor = new TupleBPlusTreeUniqueStoreByteKeysRangeSearchResultScanCursor(relation);
				}
			}
			addCursor(relation, cursor);
			return (RangeSearchResultCursor<?,?,?>) cursor;
		}
		return null;
	}

	public FixpointCursor createFixpointCursor(Relation<?> relation, int[] filteredColumns) {
		return createNaiveCursor((Relation<AddressedTuple>) relation, filteredColumns);
	}
	
	public NaiveCursor createNaiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) {		
		NaiveCursor cursor = new NaiveCursor(relation, filteredColumns);
		addCursor(relation, cursor);
		return cursor;
	}
	
	public FixpointCursor createSemiNaiveFixpointCursor(Relation<?> relation, int[] filteredColumns) {
		return createSemiNaiveCursor(relation, filteredColumns);
	}
	
	public SemiNaiveCursor createSemiNaiveCursor(Relation<?> relation, int[] filteredColumns) {
		SemiNaiveCursor cursor = new SemiNaiveCursor((Relation<AddressedTuple>) relation, filteredColumns);
		addCursor(relation, cursor);
		return cursor;
	}
	
	public TupleBPlusTreeIndexScanCursor createTupleBPlusTreeIndexScanCursor(Relation<AddressedTuple> relation, int[] indexedColumns) {
		// first check if we already have an index of this type on the relation
		// create new index for this cursor
		BPlusTreeSecondaryIndex<BPlusTreeSecondaryIndexLeaf<?>> index = 
				(BPlusTreeSecondaryIndex<BPlusTreeSecondaryIndexLeaf<?>>) this.database.getIndexManager().createBPlusTreeSecondaryIndex(relation, indexedColumns);
		// add it to the relation
		relation.addSecondaryIndex((SecondaryIndex<?>)index);

		return createTupleBPlusTreeIndexScanCursor(relation, index);
	}	

	public TupleBPlusTreeIndexScanCursor createTupleBPlusTreeIndexScanCursor(Relation<AddressedTuple> relation, 
			BPlusTreeSecondaryIndex<BPlusTreeSecondaryIndexLeaf<?>> index) {
		TupleBPlusTreeIndexScanCursor cursor = new TupleBPlusTreeIndexScanCursor(relation, index);
		addCursor(relation, cursor);
		return cursor;
	}
	
	public ChangeTrackerCursor<?> createChangeTrackerCursor(Relation<?> relation, DeALSConfiguration deALSConfiguration) {
		if (relation.getTupleStore() instanceof TupleAggregationStore) {
			TupleAggregationStoreStructure tass = ((TupleAggregationStore)relation.getTupleStore()).storageStructure;
			return getChangeTrackerCursor((AggregateRelation) relation, tass.getKeyColumns(), tass.getKeyColumnTypes(), deALSConfiguration);
		} else if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore) {
			TupleBPlusTreeUniqueStore bplusTree = ((TupleBPlusTreeUniqueStore)relation.getTupleStore());
			return getChangeTrackerCursor((DerivedRelation) relation, bplusTree.getKeyColumns(), bplusTree.getKeyColumnTypes(), deALSConfiguration);
		} else if (relation.getTupleStore() instanceof TupleUnorderedHeapStore) {
			//TupleUnorderedHeapStore heap = ((TupleUnorderedHeapStore)relation.getTupleStore());
			return getChangeTrackerCursor((DerivedRelation) relation, new int[]{0}, new DataType[]{DataType.INT}, deALSConfiguration);
			//return getChangeTrackerCursor((DerivedRelation) relation, heap.getKeyColumns(), heap.getKeyColumnTypes(), deALSConfiguration);
		}
		return null;
	}
	
	private ChangeTrackerCursor<?> getChangeTrackerCursor(DerivedRelation relation, int[] keyColumns, 
			DataType[] keyColumnTypes, DeALSConfiguration deALSConfiguration) {		
		ChangeTrackerCursor<?> cursor = null;
		switch (ChangeTracker.getChangeTrackerType(keyColumns, keyColumnTypes, deALSConfiguration)) {
			case 1:
				cursor = new IntBitmapChangeTrackerScanCursor(relation);
				break;
			case 2:
				cursor = new IntTreeChangeTrackerScanCursor(relation);
				break;
			case 3:
				cursor = new LongTreeChangeTrackerScanCursor(relation);
				break;
			case 4:
				cursor = new IntIntBitmapChangeTrackerScanCursor(relation);
				break;
			case 5:
				cursor = new GeneralChangeTrackerScanCursor(relation);
				break;
		}
		
		addCursor(relation, cursor);
		return cursor;
	}
	
	public XYCursor createXYCursor(Relation<AddressedTuple> relation, int[] filteredColumns, 
			DeALSContext deALSContext, Database database) {
		XYCursor cursor = new XYCursor(relation, filteredColumns, deALSContext, database);
		addCursor(relation, cursor);
		return cursor;
	}
	
	public IndexNaiveCursor createIndexNaiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) {
		IndexNaiveCursor cursor = new IndexNaiveCursor(relation, filteredColumns);	
		addCursor(relation, cursor);
		return cursor;
	}
	
	public IndexSemiNaiveCursor createIndexSemiNaiveCursor(Relation<AddressedTuple> relation, int[] filteredColumns) {
		IndexSemiNaiveCursor cursor = new IndexSemiNaiveCursor(relation, filteredColumns);
		addCursor(relation, cursor);
		return cursor;
	}
		
	public void destroyCursor(Cursor<?> cursor) {
		if (cursor != null)
			destroyCursor(cursor.getRelation(), cursor);
	}

	public Cursor<?> createAggregateCursor(Relation<?> relation, int numberOfKeyColumns, int[] filteredColumns, 
			AggregateInfo[] aggregateInfos, TypeManager typeManager) {
		Cursor<?> cursor = null;
		if (filteredColumns == null || filteredColumns.length == 0) {
			if (relation.getTupleStore() instanceof TupleAggregationStore)
				cursor = new AggregateAggregatorScanCursor((AggregateRelation) relation, numberOfKeyColumns, aggregateInfos, this, typeManager);
			else if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore)
				cursor = new AggregateBPlusTreeScanCursor((DerivedRelation) relation, numberOfKeyColumns, aggregateInfos, typeManager);
			else if (relation.getTupleStore() instanceof TupleUnorderedHeapStore)
				cursor = new AggregateHeapScanCursor((Relation<AddressedTuple>) relation, numberOfKeyColumns, aggregateInfos, typeManager);
		} else {
			// use scan for b+trees because tree is already indexed 
			// just scan it and track which tuples have been produced so as to not produce the same multiple times
			if (relation.getTupleStore() instanceof TupleAggregationStore)
				cursor = new AggregateAggregatorScanCursor((AggregateRelation) relation, numberOfKeyColumns, aggregateInfos, this, typeManager);
			else if (relation.getTupleStore() instanceof TupleBPlusTreeUniqueStore) 
				cursor = new AggregateBPlusTreeScanCursor((DerivedRelation) relation, numberOfKeyColumns, aggregateInfos, typeManager);
			else if (relation.getTupleStore() instanceof TupleUnorderedHeapStore)
				cursor = new AggregateIndexCursor(relation, filteredColumns, numberOfKeyColumns, aggregateInfos, typeManager);			
		}
		addCursor(relation, cursor);
		return cursor;
	}

	public Cursor<?> createSSSPCursor(Relation<?> relation) {
		Cursor<?> cursor = null;
		if (relation.getArity() == 3)
			if (relation.getSchema()[0].getNumberOfBytes() + relation.getSchema()[1].getNumberOfBytes() == 8)
				cursor = new SSSPBPlusTreeForestLongKeysIntValuesScanCursor((AggregateRelation) relation);			
		
		if (cursor == null)
			cursor = new SSSPBPlusTreeForestIntKeysIntValuesScanCursor((Relation<Tuple>) relation);
		
		addCursor(relation, cursor);
		return cursor;
	}
}
