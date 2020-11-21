package edu.ucla.cs.wis.bigdatalog.database;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.file.*;

import static java.nio.file.StandardOpenOption.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.database.relation.DerivedRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.AggregateRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.BaseRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.QueryFormRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.RecursiveRelation;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.relation.RelationType;
import edu.ucla.cs.wis.bigdatalog.database.store.aggregators.TupleAggregationStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreBase;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreConfiguration;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.TupleStoreManager.TupleStoreType;
import edu.ucla.cs.wis.bigdatalog.exception.RelationManagerException;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate.udaframework.fs.FSAggregateRelationNodeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.system.DeALSConfiguration;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class RelationManager implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(RelationManager.class.getName());

	public ArrayList<BaseRelation<?>>	baseRelations;
	public ArrayList<DerivedRelation>	derivedRelations;
	public ArrayList<RecursiveRelation> recursiveRelations;
	private DeALSConfiguration 			deALSConfiguration;
	private Database					database;
	
	public RelationManager(DeALSConfiguration deALSConfiguration, Database database) {
		this.deALSConfiguration = deALSConfiguration;
		this.database = database;
		this.initRelationLists();
	}

	public RelationManager(ObjectInputStream stream) {		
		try {
			this.initializeRelationLists(stream);
		} catch (Exception e) {
			if (e.getMessage() != null)
				logger.error(e.getMessage());
		}
	}

	public int getNumberOfRelations() { 
		return this.baseRelations.size() + this.recursiveRelations.size() + this.derivedRelations.size(); 
	}
	
	public int getNumberOfBaseRelations() { return this.baseRelations.size(); }
	
	public int getNumberOfRecursiveRelations() { return this.recursiveRelations.size(); }
	
	public int getNumberOfDerivedRelations() { return this.derivedRelations.size(); }
	
	public List<Relation<?>> getAllRelations() {
		List<Relation<?>> allRelations = new ArrayList<>();
		allRelations.addAll(this.baseRelations);
		for (Relation<?> relation : this.derivedRelations)
			allRelations.add(relation);
		for (Relation<?> relation : this.recursiveRelations)
			allRelations.add(relation);
		return allRelations;
	}
	
	public List<BaseRelation<?>> getBaseRelations() { return this.baseRelations; }
	
	public List<DerivedRelation> getDerivedRelations() { return this.derivedRelations; }
	
	public List<RecursiveRelation> getRecursiveRelations() { return this.recursiveRelations; }

	private void initRelationLists(){
		this.baseRelations = new ArrayList<>();
		this.recursiveRelations = new ArrayList<>();
		this.derivedRelations = new ArrayList<>();
	}

	public void initializeRelationLists(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {		
		long numberOfBaseRelations = inputStream.readLong();
		
		this.baseRelations = new ArrayList<>();

		for (int i = 0; i < numberOfBaseRelations; i++) {
			BaseRelation<?> relation = (BaseRelation<?>)inputStream.readObject();
			this.baseRelations.add(relation); 
	    }

		this.recursiveRelations = new ArrayList<>();
		this.derivedRelations = new ArrayList<>();
	}
	
	public boolean clear() {
		this.baseRelations.clear();
		this.recursiveRelations.clear();
		this.derivedRelations.clear();
		return true;
	}
	
	public boolean save(Path filePath) {
		ObjectOutputStream stream = null;
		boolean status = false;
		
		try {
			stream = new ObjectOutputStream(new FileOutputStream(filePath.toString()));
		} catch (FileNotFoundException fnfe) {
			OutputStream os = null;
			try {
				os = Files.newOutputStream(filePath, CREATE);
				stream = new ObjectOutputStream(os);
			} catch (IOException ioe) {
				ioe.printStackTrace();
				if (os != null) {
					try {
						os.close();
					} catch (Exception ex) {/* do nothing */ }
					os = null;
				}		
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			if (stream != null)
				try {stream.close();} catch (Exception ex) {/* do nothing */}
		}

		if (stream != null) {
			try {
				status = this.saveState(stream);
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
			try {
				stream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
		return status;
	}

	private boolean saveState(ObjectOutputStream stream) throws IOException {
		boolean status = this.saveRelations(stream);
		return status;
	}

	private boolean saveRelations(ObjectOutputStream stream) throws IOException {
		stream.writeLong(this.getNumberOfBaseRelations());

		for (int i = 0; i < this.getNumberOfBaseRelations(); i++)
			stream.writeObject(this.baseRelations.get(i));
		return true;
	}

	public Relation<?> getRelation(String relationName, int arity) {
		for (Relation<?> relation : this.getAllRelations()) {
			if (relationName.equals(relation.getName()) 
					&& (relation.getArity() == arity))
				return relation;
		}

		return null;
	}

	public BaseRelation<?> getBaseRelation(String relationName) {
		// only one base relation with a predicate name regardless of arity
		for (BaseRelation<?> baseRelation : this.baseRelations) {
			if (relationName.equals(baseRelation.getName())) 
				return baseRelation;
		}

		return null;
	}

	public RecursiveRelation getRecursiveRelation(String relationName, int arity) {
		for (RecursiveRelation recursiveRelation : this.recursiveRelations) {
			if (relationName.equals(recursiveRelation.getName())
					&& (recursiveRelation.getArity() == arity)) {
				return recursiveRelation;
			}
		}

		return null;
	}
	
	public DerivedRelation getDerivedRelation(String relationName, int arity) {
		for (DerivedRelation derivedRelation : this.derivedRelations) {
			if (relationName.equals(derivedRelation.getName()) 
					&& (derivedRelation.getArity() == arity))
				return derivedRelation;
		}

		return null;
	}

	public BaseRelation<?> createBaseRelation(String relationName, DataType[] schema) {
		return createBaseRelation(relationName, schema, new TupleStoreConfiguration(TupleStoreType.TupleArray));
	}
	
	@SuppressWarnings("unchecked")
	public BaseRelation<?> createBaseRelation(String relationName, DataType[] schema, TupleStoreConfiguration tsc) {
		BaseRelation<?> baseRelation = this.getBaseRelation(relationName);
		
		if (baseRelation == null) {
			TupleStoreBase<?> tupleStore = TupleStoreManager.createTupleStore(relationName, RelationType.BASE, schema, tsc, 
					this.deALSConfiguration, this.database.getTypeManager());
			baseRelation = new BaseRelation(relationName, schema, tupleStore, this.database);
			this.baseRelations.add(baseRelation);
		}

		return baseRelation;
	}
		
	public RecursiveRelation createRecursiveRelation(String relationName, DataType[] schema, boolean addUniqueIndex) {
		return createRecursiveRelation(relationName, schema, null, addUniqueIndex);
	}
	
	public RecursiveRelation createRecursiveRelation(String relationName, DataType[] schema, 
			TupleStoreConfiguration tsc, boolean addUniqueIndex) {
		RecursiveRelation recursiveRelation = this.getRecursiveRelation(relationName, schema.length);

		if (recursiveRelation == null) {
			TupleStoreBase<?> tupleStore = TupleStoreManager.createTupleStore(relationName, RelationType.RECURSIVE, schema, tsc, 
					this.deALSConfiguration, this.database.getTypeManager());
			
			recursiveRelation = new RecursiveRelation(relationName, schema, tupleStore, addUniqueIndex, this.database);
			this.recursiveRelations.add(recursiveRelation);
		}
		
		return recursiveRelation;
	}
	
	public DerivedRelation createDerivedRelation(String relationName, DataType[] schema) {
		return createDerivedRelation(relationName, schema, null, true);
	}
	
	public DerivedRelation createDerivedRelation(String relationName, DataType[] schema, boolean addUniqueIndex) {
		return createDerivedRelation(relationName, schema, new TupleStoreConfiguration(TupleStoreType.UnorderedHeap), addUniqueIndex);
	}
	
	public DerivedRelation createDerivedRelation(String relationName, DataType[] schema, TupleStoreConfiguration tsc, boolean addUniqueIndex) {
		DerivedRelation derivedRelation = this.getDerivedRelation(relationName, schema.length);
		if (derivedRelation == null) {
			TupleStoreBase<?> tupleStore = TupleStoreManager.createTupleStore(relationName, RelationType.DERIVED, schema, tsc, 
					this.deALSConfiguration, this.database.getTypeManager());
						
			if (tupleStore instanceof AddressedTupleStore)
				derivedRelation = new DerivedRelation(relationName, schema, tupleStore, addUniqueIndex, this.database);
			else
				derivedRelation = new DerivedRelation(relationName, schema, tupleStore, addUniqueIndex, this.database);
			this.derivedRelations.add(derivedRelation);
		}

		return derivedRelation;
	}
	
	public String getNextDerivedRelationName(String relationName, int arity) {
		String name = relationName;
		int index = 1;
		while (this.getDerivedRelation(name, arity) != null)
			name = relationName + "_" + index++;
		
		return name;
	}
	
	@SuppressWarnings("unchecked")
	public QueryFormRelation createQueryFormRelation(String relationName, DataType[] schema, boolean addUniqueIndex) {	
		DerivedRelation derivedRelation = this.getDerivedRelation(relationName, schema.length);
			
		if (derivedRelation != null)
			throw new RelationManagerException("Query form already exists.  Cannot be recreated!");
			 	
		TupleStoreBase<AddressedTuple> tupleStore = (TupleStoreBase<AddressedTuple>) TupleStoreManager.createTupleStore(relationName, 
				RelationType.DERIVED, schema, null, this.deALSConfiguration, this.database.getTypeManager());

		QueryFormRelation queryFormRelation = new QueryFormRelation(relationName, schema, tupleStore, addUniqueIndex, this.database);
		this.derivedRelations.add(queryFormRelation);

		return queryFormRelation;
	}
	
	public DerivedRelation createAggregateRelationHeap(String relationName, DataType[] schema, int[] keyColumns) {
		return createAggregateRelationHeap(relationName, schema, keyColumns, false);
	}
	
	public DerivedRelation createAggregateRelationHeap(String relationName, DataType[] schema, int[] keyColumns, boolean isChangeTrackingStore) {
		DerivedRelation aggregateRelation = this.getDerivedRelation(relationName, schema.length);
		if (aggregateRelation == null) {		
			TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.UnorderedHeap, keyColumns);
			tsc.setTrackModifiedTuples(isChangeTrackingStore);
			TupleStoreBase<?> tupleStore = TupleStoreManager.createTupleStore(relationName, RelationType.DERIVED, schema, tsc, 
					this.deALSConfiguration, this.database.getTypeManager());
			aggregateRelation = new DerivedRelation(relationName, schema, tupleStore, false, this.database);
			this.derivedRelations.add(aggregateRelation);
	
			if (keyColumns.length > 0)
				aggregateRelation.addSecondaryIndex(keyColumns);
		}
		return aggregateRelation;
	}
	
	public DerivedRelation createAggregateRelationBPlusTree(String relationName, DataType[] schema, int[] keyColumns) {
		return createAggregateRelationBPlusTree(relationName, schema, keyColumns, false);
	}
	
	public DerivedRelation createAggregateRelationBPlusTree(String relationName, DataType[] schema, int[] keyColumns, boolean isChangeTrackingStore) {
		DerivedRelation aggregateRelation = this.getDerivedRelation(relationName, schema.length);
		if (aggregateRelation == null) {
			TupleStoreConfiguration tsc = new TupleStoreConfiguration(TupleStoreType.BPlusTree, keyColumns);
			tsc.setUniqueValue(true);
			tsc.setTrackModifiedTuples(isChangeTrackingStore);
			TupleStoreBase<?> tupleStore = TupleStoreManager.createTupleStore(relationName, RelationType.DERIVED, schema, tsc, 
					this.deALSConfiguration, this.database.getTypeManager());
			aggregateRelation = new DerivedRelation(relationName, schema, tupleStore, false, this.database);
			this.derivedRelations.add(aggregateRelation);
		}
		return aggregateRelation;
	}
	
	public AggregateRelation createAggregateRelationAggregator(String relationName, DataType[] schema, TupleStoreConfiguration tsc) {
		DerivedRelation aggregateRelation = this.getDerivedRelation(relationName, schema.length);
		if (aggregateRelation == null) {
			TupleAggregationStore tupleStore = (TupleAggregationStore)TupleStoreManager.createTupleStore(relationName, 
					RelationType.AGGREGATE, schema, tsc, this.deALSConfiguration, this.database.getTypeManager());
			
			aggregateRelation = new AggregateRelation(relationName, schema, tupleStore, this.database);
			this.derivedRelations.add(aggregateRelation);
		}

		return (AggregateRelation) aggregateRelation;
	}
	
	public void deleteBaseRelation(String relationName) {
		BaseRelation<?> relation;

		if ((relation = this.getBaseRelation(relationName)) != null)
			this.deleteBaseRelation(relation);
	}

	public void deleteBaseRelation(BaseRelation<?> relation) {
		// Delete the relation entry from the base relation list
		for (BaseRelation<?> baseRelation : this.baseRelations) {
			if (baseRelation.getName().equals(relation.getName()) 
					&& baseRelation.getArity() == relation.getArity()) {
				synchronized (this) {
					baseRelation.delete();
					this.baseRelations.remove(baseRelation);
				}
				
				break;
			}
		}
	}
	
	public void deleteRecursiveRelation(RecursiveRelation relation) {
		// Delete the relation entry from the recursive relation list
		for (RecursiveRelation recursiveRelation : this.recursiveRelations) {
			if (recursiveRelation.getName().equals(relation.getName()) 
					&& recursiveRelation.getArity() == relation.getArity()) {
				synchronized (this) {
					recursiveRelation.delete();
					this.recursiveRelations.remove(recursiveRelation);
				}
				break;
			}
		}
	}
	
	public void deleteDerivedRelation(String relationName, int arity) {
		Relation<?> relation;

		if ((relation = this.getDerivedRelation(relationName, arity)) != null)
			this.deleteDerivedRelation((DerivedRelation) relation);
		else if ((relation = this.getRecursiveRelation(relationName, arity)) != null)
			this.deleteRecursiveRelation((RecursiveRelation) relation);
	}
	
	public void deleteDerivedRelation(DerivedRelation relation) {
		// Delete the relation entry from the derived relation list
		for (DerivedRelation derivedRelation : this.derivedRelations) {
			if (derivedRelation.getName().equals(relation.getName()) 
					&& derivedRelation.getArity() == relation.getArity()) {
				synchronized (this) {
					derivedRelation.delete();
					this.derivedRelations.remove(derivedRelation);
				}				
				break;
			}
		}
	}
/*
	public boolean matchesSchema(String relationName, Tuple tuple) {
		BaseRelation<?> relation = this.getBaseRelation(relationName);
		
		if (relation != null) {
			DataType[] schema = relation.getSchema();
			// if no schema, everything matches
			if (schema == null)
				return true;
			
			return tuple.matchesSchema(schema); 		
		}			
		return true;
	}*/
	
	public static RelationManager restore(String filename) {
		RelationManager relationManager = null;
		ObjectInputStream stream = null;
		try {
			stream = new ObjectInputStream(new FileInputStream(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (stream != null)
	    	relationManager = new RelationManager(stream);
	    
		return relationManager;
	}
	
	public boolean truncateRelation(String relationName) {
		BaseRelation<?> relation = this.getBaseRelation(relationName);
		if (relation == null)
			return false;
		
		relation.removeAllTuples();
		relation.commit();
		return true;
	}
	
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		MemoryMeasurement size;
		for (Relation<?> relation : this.baseRelations) {
			size = relation.getSizeOf();
			used += size.getUsed();
			allocated += size.getAllocated();
		}
		
		for (Relation<?> relation : this.derivedRelations) {
			size = relation.getSizeOf();
			used += size.getUsed();
			allocated += size.getAllocated();
		}
		
		for (Relation<?> relation :this.recursiveRelations) {
			size = relation.getSizeOf();
			used += size.getUsed();
			allocated += size.getAllocated();
		}
		
		return new MemoryMeasurement(used, allocated);
	}
	
	public String toStringMemoryMeasured() {
		StringBuilder output = new StringBuilder();
		output.append(this.toStringSizeOfDetailed());
		return output.toString();
	}	
	
	public String toStringSizeOfDetailed() {
		StringBuilder output = new StringBuilder();
		for (Relation<?> relation : this.baseRelations)
			output.append(relation.getName() + "|" + relation.getArity() + "(" + Arrays.toString(relation.getSchema()) + ")\n" + relation.toStringSizeOfDetailed() + "\n");
			//output.append(relation.getName() + "|" + relation.getArity() + " = " + relation.getSizeOf().toString() + "\n");
						
		for (Relation<?> relation : this.derivedRelations)
			output.append(relation.getName() + "|" + relation.getArity() + "(" + Arrays.toString(relation.getSchema()) + ")\n" + relation.toStringSizeOfDetailed() + "\n");
		//output.append(relation.getName() + "|" + relation.getArity() + " = " + relation.getSizeOf().toString() + "\n");

		for (Relation<?> relation :this.recursiveRelations)
			output.append(relation.getName() + "|" + relation.getArity() + "(" + Arrays.toString(relation.getSchema()) + ")\n" + relation.toStringSizeOfDetailed() + "\n");
		//output.append(relation.getName() + "|" + relation.getArity() + " = " + relation.getSizeOf().toString() + "\n");
		
		output.append("Total " + this.getSizeOf().toString());
		
		return output.toString();
	}
	
	public String toStringMemoryMeasuredFS() {
		StringBuilder output = new StringBuilder();
		output.append(this.toStringSizeOfFSDetailed());
		return output.toString();
	}
	
	public String toStringSizeOfFSDetailed() {
		StringBuilder output = new StringBuilder();
		int used = 0;
		int allocated = 0;
		
		for (Relation<?> relation : this.derivedRelations) {
			if (FSAggregateRelationNodeBase.isFSAggregateRelation(relation.getName())) {
				MemoryMeasurement relationMM = 	relation.getSizeOf();
				used += relationMM.getUsed();
				allocated += relationMM.getAllocated();
				if (output.length() > 0)
					output.append("\n");
				output.append(relation.getName() + "|" + relation.getArity() + "(" + Arrays.toString(relation.getSchema()) + "): " + relationMM.toString());
			}
		}
		
		if (allocated > 0)
			output.insert(0, "Total FS " + new MemoryMeasurement(used, allocated).toString() + "\n");
		
		return output.toString();
	} 
	
	public MemoryMeasurement getMemoryUsageFS() {
		int used = 0;
		int allocated = 0;
		
		for (Relation<?> relation : this.derivedRelations) {
			if (FSAggregateRelationNodeBase.isFSAggregateRelation(relation.getName())) {
				MemoryMeasurement relationMM = 	relation.getSizeOf();
				used += relationMM.getUsed();
				allocated += relationMM.getAllocated();
			}
		}
		return new MemoryMeasurement(used, allocated);
	}

}
