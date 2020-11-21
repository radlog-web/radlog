package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

/*  Linear Hashing Index
implemented from "Dynamic Hash Tables" from Per-Ake Larson 1988
	http://www.diku.dk/hjemmesider/ansatte/henglein/papers/larson1988.pdf
changes from original are:
  - element has another another linked list under it
  - h2() is used during split()
This should be used with scan (list) data stores - not btrees 
  
Data Structure:
DIRECTORY has N SEGMENTS
SEGMENT has M BUCKETS
BUCKET has array of entries

Position of tuple within directory is determined by hash / segmentSize
Position of tuple within segment is determined by hash % segmentSize

There are up to N * M Buckets in the struture, dynamically allocated as needed until N * M are allocated
A Bucket will contain all entries hashed to that bucket number.  

more help on linear hashing and dynamic hashing:
http://cgi.di.uoa.gr/~ad/MDE515/e_ds_linearhashing.pdf
http://www.dcc.unicamp.br/~celio/mc326/hashing/dynamic-hashing-enbody.pdf
*/
public abstract class HashTable<B extends Bucket> 
	implements KeyValueStoreStructure, MemorySize, Serializable {
	private static final long serialVersionUID = 1L; 
	protected int			numberOfInitialBuckets;
	protected int 			directorySize;
	protected int			segmentSize;
	protected double		splitPolicy;

	protected DataType[]	valueTypes;
	protected int			bytesPerValue;

	protected int				upperBoundNumberOfBuckets;
	protected int 				nextBucketToSplit;
	protected int				numberOfAllocatedBuckets;
	protected B[][]			directory;
	protected int				numberOfEntries;
	protected TypeManager		typeManager;

	public HashTable() { super(); }
	
	public HashTable(DataType[] valueTypes, double splitPolicy, int directorySize, int segmentSize, int numberOfInitialBuckets, TypeManager typeManager) {
		if (valueTypes == null)
			throw new DatabaseException("Hash Table can not be created without at least one value type.");
		
		this.valueTypes = valueTypes;

		int bytesPerValue = 0;
		for (int i = 0; i < this.valueTypes.length; i++)
			bytesPerValue += this.valueTypes[i].getNumberOfBytes();
		
		this.bytesPerValue = bytesPerValue;		
		this.splitPolicy = splitPolicy;
		this.directorySize = directorySize;
		this.segmentSize = segmentSize;
		this.numberOfInitialBuckets = numberOfInitialBuckets;
		this.typeManager = typeManager;
	}

	protected void initialize() {
		this.upperBoundNumberOfBuckets 	= this.numberOfInitialBuckets;
		this.numberOfAllocatedBuckets	= this.numberOfInitialBuckets;
		this.nextBucketToSplit 			= 0;		
		this.numberOfEntries 			= 0;
	}

	public int getNumberOfEntries() { return this.numberOfEntries; }

	@Override
	public DataType getValueDataType() { return this.valueTypes[0]; }
	
	protected long h1(long hash) {
		return (hash & (this.upperBoundNumberOfBuckets - 1));
	}
	
	protected long h2(long hash) {
		return (hash & ((this.upperBoundNumberOfBuckets * 2) - 1));
	}
	
	protected double getLoadFactor() {
		return Double.valueOf(this.numberOfEntries) / this.numberOfAllocatedBuckets;
	}
	
	// gets the bucket position within a segment
	protected int getBucketPositionWithinSegment(int bucketNumber) {
		return bucketNumber % this.segmentSize;
	}
	
	// gets the segment position within the directory
	protected int getSegmentPositionWithinDirectory(int bucketNumber) {
		return bucketNumber / this.segmentSize;
	}
	
	protected B getBucket(int bucketNumber) {
		return this.directory[bucketNumber / this.segmentSize][bucketNumber % this.segmentSize];
	}
	
	protected void setBucket(int bucketNumber, B bucket) {
		this.directory[bucketNumber / this.segmentSize][bucketNumber % this.segmentSize] = bucket;
	}

	protected int getBucketNumber(long hash) {
		int bucketNumber = (int) this.h1(hash);//(hash & (this.upperBoundNumberOfBuckets - 1));
		if (bucketNumber < 0)
			bucketNumber += this.upperBoundNumberOfBuckets;
		
		// we use the 2nd hash function if the 1st hash function returned a bucket position that has already been split this round
		if (bucketNumber < this.nextBucketToSplit) {
			bucketNumber = (int) this.h2(hash);//(hash & ((this.upperBoundNumberOfBuckets * 2) - 1));
			if (bucketNumber < 0)
				bucketNumber += (this.upperBoundNumberOfBuckets * 2);
		}
		
		return bucketNumber;
	}
	
	public void clear() {
		for (int i = 0; i < this.numberOfAllocatedBuckets; i++) {
			this.getBucket(i).clear();
			this.setBucket(i, null);
		}
				
		this.initialize();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {
		int used = 0;
		int allocated = 0;
		if (this.directory != null) {
			MemoryMeasurement sizes;
			for (int i = 0; i < (int)Math.ceil(Double.valueOf(this.upperBoundNumberOfBuckets) / Double.valueOf(this.segmentSize)); i++) {
				for (int j = 0; j < this.segmentSize; j++) {
					if (this.directory[i][j] != null) {
						sizes = this.directory[i][j].getSizeOf();
						used += sizes.getUsed(); 
						allocated += sizes.getAllocated();
					}
				}
			}
		}
		return new MemoryMeasurement(used, allocated);
	}
	
	public String toStringShort() {
		StringBuilder retval = new StringBuilder();
		if (this.directory != null) {
			for (int i = 0; i < (int)Math.ceil(Double.valueOf(this.upperBoundNumberOfBuckets) / Double.valueOf(this.segmentSize)); i++) {
				for (int j = 0; j < this.segmentSize; j++) {
					if (this.directory[i][j] != null)
						retval.append(this.directory[i][j].toStringShort());
				}
			}
		}		
		return retval.toString();
	}
	
	abstract public void get(DbTypeBase key, KeyValueStoreGetResult result);
	
	abstract public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result);
	
	abstract public boolean remove(DbTypeBase key);
	
	
}