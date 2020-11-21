package edu.ucla.cs.wis.bigdatalog.database.store.tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.relation.RelationType;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeKeysOnlyStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeMultiStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeStoreRO;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.sortable.SortingTupleBPlusTreeMultiStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.sortable.SortingTupleBPlusTreeUniqueStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.heap.TupleUnorderedHeapStore;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class TupleStoreManager {
		
	public enum TupleStoreType { 
		Aggregation, BPlusTree, BPlusTreeKeysOnly, SortingBPlusTree, 
		TupleArray, TupleBPlusTree, UnorderedHeap
	}
	
	private static HashMap<String, List<TupleStoreBase<?>>> tupleStores;

	public static HashMap<String, List<TupleStoreBase<?>>> getTupleStores() {
		if (tupleStores == null)
			tupleStores = new HashMap<>();
			
		return tupleStores;
	}
	
	public static void addTupleStore(String relationNameIdentifier, TupleStoreBase<?> tupleStore) {
		if (tupleStores == null)
			tupleStores = new HashMap<>();

		List<TupleStoreBase<?>> relationTupleStores = new ArrayList<>();
		if (tupleStores.containsKey(relationNameIdentifier))
			relationTupleStores = tupleStores.get(relationNameIdentifier);
		
		if (!relationTupleStores.contains(tupleStore))
			relationTupleStores.add(tupleStore);
		
		tupleStores.put(relationNameIdentifier, relationTupleStores);		
	}
	
	public static void removeTupleStore(Relation<?> relation, TupleStoreBase<?> tupleStore) {
		String relationNameIdentifier = createRelationNameIdentifier(relation.getName(), relation.getArity());
		if (tupleStores.containsKey(relationNameIdentifier))
			tupleStores.get(relationNameIdentifier).remove(tupleStore);
	}
	
	public static void clearTupleStores() { tupleStores.clear(); }
	
	public static void resetTupleStoreCounters() {
		if (tupleStores != null)
			for (Map.Entry<String, List<TupleStoreBase<?>>> entry : tupleStores.entrySet())
				for (TupleStoreBase<?> tupleStore : entry.getValue()) 
					tupleStore.resetCounters();		
	}

	public static TupleStoreBase<?> doCreateTupleStore(String relationName, RelationType relationType, DataType[] schema, 
			TupleStoreConfiguration configuration, DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		
		int nodeSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.store.aggregators.bplustree.nodesize"));
		boolean useOrderedHeap =  Boolean.parseBoolean(deALSConfiguration.getProperty("deals.database.tuplestores.bplustree.useorderedheap"));
		int pageSize = Integer.parseInt(deALSConfiguration.getProperty("deals.database.tuplestores.unorderedheap.pagesize"));
		
		switch (configuration.tupleStoreType) {
			case Aggregation:
				if (configuration.keyColumns.length > 0)
					return new TupleAggregationStore(relationName, schema, configuration, nodeSize, typeManager);
			case BPlusTree:
				if (configuration.uniqueValue)
					return new TupleBPlusTreeUniqueStore(relationName, schema, configuration, nodeSize, typeManager);
					
				return new TupleBPlusTreeMultiStore(relationName, schema, configuration, nodeSize, useOrderedHeap, typeManager);				
			case BPlusTreeKeysOnly:
				return new TupleBPlusTreeKeysOnlyStore(relationName, schema, nodeSize, typeManager);
			case SortingBPlusTree:
				if (configuration.uniqueValue || schema.length == configuration.keyColumns.length)
					return new SortingTupleBPlusTreeUniqueStore(relationName, schema, configuration, nodeSize, typeManager);
				return new SortingTupleBPlusTreeMultiStore(relationName, schema, configuration, nodeSize, useOrderedHeap, typeManager);
			case TupleArray:
				return new TupleArrayStore(relationName, schema, typeManager);
			case TupleBPlusTree:
				if (relationType == RelationType.BASE)
					return new TupleBPlusTreeStoreRO(relationName, schema, configuration, nodeSize, typeManager);
				return new TupleBPlusTreeStore(relationName, schema, configuration, nodeSize, typeManager);
			case UnorderedHeap: {
				return new TupleUnorderedHeapStore(relationName, schema, configuration, pageSize, deALSConfiguration, typeManager);
			}
		}
		
		throw new DatabaseException("Unknown tuple store type.  Cannot create tuple store!");
	}
	
	public static TupleStoreBase<?> createTupleStore(String relationName, RelationType relationType, DataType[] schema, 
			TupleStoreConfiguration configuration, DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		TupleStoreBase<?> tupleStore;
		switch (relationType) {
		case BASE:
			tupleStore = createTupleStoreForBaseRelation(relationName, schema, configuration, deALSConfiguration, typeManager);
			break;
		default:
			tupleStore = createTupleStoreForDerivedRelation(relationName, schema, configuration, deALSConfiguration, typeManager);
			break;
		}
		addTupleStore(createRelationNameIdentifier(relationName, schema.length), tupleStore);
		return tupleStore;
	}
	
	private static TupleStoreBase<?> createTupleStoreForRelation(String relationName, RelationType relationType, DataType[] schema, 
			TupleStoreConfiguration configuration, DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		if (configuration == null)
			configuration = new TupleStoreConfiguration(TupleStoreType.UnorderedHeap);
		
		if ((configuration.tupleStoreType == TupleStoreType.BPlusTree) 
				&& ((schema.length == configuration.keyColumns.length) || (configuration.keyColumns.length == 0)))			
			configuration.tupleStoreType = TupleStoreType.BPlusTreeKeysOnly;

		return doCreateTupleStore(relationName, relationType, schema, configuration, deALSConfiguration, typeManager);
	}

	private static TupleStoreBase<?> createTupleStoreForBaseRelation(String relationName, DataType[] schema, 
			TupleStoreConfiguration configuration, DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		if (schema == null)
			throw new DatabaseException("schema is null for base relation " + relationName);
		return doCreateTupleStore(relationName, RelationType.BASE, schema, configuration, deALSConfiguration, typeManager);
	}
	
	private static TupleStoreBase<?> createTupleStoreForDerivedRelation(String relationName, DataType[] schema, 
			TupleStoreConfiguration configuration, DeALSConfiguration deALSConfiguration, TypeManager typeManager) {
		if (schema == null || !isSchemaComplete(schema)) {
			if (configuration == null)
				configuration = new TupleStoreConfiguration(TupleStoreType.TupleArray);
			else
				configuration.tupleStoreType = TupleStoreType.TupleArray;
		}
		return createTupleStoreForRelation(relationName, RelationType.DERIVED, schema, configuration, deALSConfiguration, typeManager);
	}
	
	private static String createRelationNameIdentifier(String relationName, int arity) {
		return relationName + "|" + arity;
	}

	private static boolean isSchemaComplete(DataType[] schema) {
		for (DataType dataType : schema)
			if (dataType == DataType.UNKNOWN || dataType == DataType.ANY)
				return false;
		
		return true;
	}
}
