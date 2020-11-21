package edu.ucla.cs.wis.bigdatalog.database.index.secondary.hash;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.IntegerKeySecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.ByteArrayHelper;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

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
      BUCKET has linked list of BUCKETENTRY
      BUCKETENTRY has linked list of data items
      
      Position of tuple within directory is determined by hash / segmentSize
      Position of tuple within segment is determined by hash % segmentSize
      
      There are up to N * M Buckets in the struture, dynamically allocated as needed until N * M are allocated
      There are infinity BucketEntries in the structure
      A Bucket will contain all tuples hashed to that bucket number.  Tuples are stored in BucketEntrys
      A BucketEntry will contain all items with the same hash key 

    more help on linear hashing and dynamic hashing:
      http://cgi.di.uoa.gr/~ad/MDE515/e_ds_linearhashing.pdf
      http://www.dcc.unicamp.br/~celio/mc326/hashing/dynamic-hashing-enbody.pdf
*/
public class IntegerKeyHashSecondaryIndex 
	extends IntegerKeySecondaryIndex<AddressedTupleStore> 
	implements Serializable {
	private static Logger logger = LoggerFactory.getLogger(IntegerKeyHashSecondaryIndex.class.toString());
	
	private static final long serialVersionUID = 1L;

	protected int							numberOfInitialBuckets;
	protected IntegerKeyBucketOfTupleAddresses[][]	directory;
	protected int 							directorySize;
	protected int							segmentSize;
	protected int							upperBoundNumberOfBuckets;
	protected int 							nextBucketToSplit;
	protected int							numberOfAllocatedBuckets;
	protected int							bucketCapacity;
	protected double						splitPolicy;

	public IntegerKeyHashSecondaryIndex(Relation<AddressedTuple> relation, int indexedColumn, double splitPolicy, 
			int directorySize, int segmentSize, int numberOfInitialBuckets) {
		super(relation, indexedColumn);

		/*this.splitPolicy = Double.parseDouble(DeALSContext.getConfiguration().getProperty("deals.database.indexes.hash.splitpolicy"));
		this.directorySize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.indexes.hash.directorysize"));
		this.segmentSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.indexes.hash.segmentsize"));
		this.numberOfInitialBuckets = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.indexes.hash.numberofinitialbuckets"));
		*/
		this.splitPolicy = splitPolicy;
		this.directorySize = directorySize;
		this.segmentSize = segmentSize;
		this.numberOfInitialBuckets = numberOfInitialBuckets;
		this.initialize();
	}
	
	private void initialize() {	
		this.directory 					= new IntegerKeyBucketOfTupleAddresses[this.directorySize][];
		this.upperBoundNumberOfBuckets 	= this.numberOfInitialBuckets;
		this.numberOfAllocatedBuckets	= this.numberOfInitialBuckets;
		this.nextBucketToSplit 			= 0;
		
		this.numberOfEntries 			= 0;
		
		int numberOfSegments = (int)Math.ceil(Double.valueOf(this.upperBoundNumberOfBuckets) / Double.valueOf(segmentSize)); 
		this.bytesPerKey = this.getKeySize();
		
		// initialize the segments
		for (int i = 0; i < numberOfSegments; i++)
			this.directory[i] = new IntegerKeyBucketOfTupleAddresses[segmentSize];
		
		// initialize the buckets
		for (int i = 0; i < this.numberOfAllocatedBuckets; i++)
			this.directory[this.getSegmentPositionWithinDirectory(i)][this.getBucketPositionWithinSegment(i)] = new IntegerKeyBucketOfTupleAddresses();
	}
	
	@Override
	public void doClear() {
		this.initialize();
	}

	@Override
	public boolean doPut(int key, AddressedTuple tuple) {
		long hash = hash(key);
		
		// returns false if key already in bucket
		if (this.getBucket(this.getBucketNumber(hash)).put(key, hash, tuple.address)) {
			this.numberOfEntries++;
			
			if (this.getLoadFactor() > this.splitPolicy)
				this.split();
			return true;
		}
		return false;
	}
	
	@Override
	public AddressedTuple doGet(int key, Tuple tuple) {
		long hash = hash(key);
			
		TupleAddressArray entry = this.getBucket(this.getBucketNumber(hash)).get(key/*, hash*/);
		
		if (entry != null) {
			int[] addresses = entry.getAddresses();

			for (int i = 0; i < addresses.length; i++) {
				if ((this.tupleStore.get(addresses[i], this.capturedTuple) > 0)
					&& tuple.equals(this.capturedTuple.columns, this.totalNumberOfColumns))
						return this.capturedTuple;
			}
		}
		
		return null;
	}
	
	@Override
	protected int[] doGetSimilar(int key) {
		long hash = hash(key);

		TupleAddressArray entry = this.getBucket(this.getBucketNumber(hash)).get(key/*, hash*/);
		if (entry != null)
			return entry.getAddresses();
		
		return null;
	}
	
	public boolean doRemove(int key, AddressedTuple tuple) {
		long hash = hash(key);
		boolean status = this.getBucket(this.getBucketNumber(hash)).remove(key, /*hash, */tuple.address);
		if (status)
			this.numberOfEntries--;
		
		return status;
	}
		
	private int getBucketNumber(long hash) {
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
	
	private static long hash(int key) {
		//return Hashing.murmur3_32().hashBytes(key).asInt() & 0x7fffffff;
		return MurmurHash.hash(ByteArrayHelper.getIntAsBytes(key));			
	}	
	
	private long h1(long hash) {
		return (hash & (this.upperBoundNumberOfBuckets - 1));
	}
	
	private long h2(long hash) {
		return (hash & ((this.upperBoundNumberOfBuckets * 2) - 1));
	}
	
	private double getLoadFactor() {
		return Double.valueOf(this.numberOfEntries) / this.numberOfAllocatedBuckets;
	}
	
	// gets the bucket position within a segment
	private int getBucketPositionWithinSegment(int bucketNumber) {
		return bucketNumber % this.segmentSize;
	}
	
	// gets the segment position within the directory
	private int getSegmentPositionWithinDirectory(int bucketNumber) {
		return bucketNumber / this.segmentSize;
	}
	
	private IntegerKeyBucketOfTupleAddresses getBucket(int bucketNumber) {
		return this.directory[bucketNumber / this.segmentSize][bucketNumber % this.segmentSize];
	}
	
	private void setBucket(int bucketNumber, IntegerKeyBucketOfTupleAddresses bucket) {
		this.directory[bucketNumber / this.segmentSize][bucketNumber % this.segmentSize] = bucket;
	}

	private void split() {
		if ((this.nextBucketToSplit + this.upperBoundNumberOfBuckets) < (this.directorySize * this.segmentSize)) {					
			IntegerKeyBucketOfTupleAddresses bucketToSplit = this.getBucket(this.nextBucketToSplit);
			
			int oldBucketNumber = this.nextBucketToSplit;
			int newBucketNumber = this.nextBucketToSplit + this.upperBoundNumberOfBuckets;
			
			// allocate a new segment when we fill the previous 
			if ((newBucketNumber % this.segmentSize) == 0)
				this.directory[newBucketNumber / this.segmentSize] = new IntegerKeyBucketOfTupleAddresses[segmentSize];
						
			IntegerKeyBucketOfTupleAddresses newBucket = new IntegerKeyBucketOfTupleAddresses();
			
			this.setBucket(newBucketNumber, newBucket);
			
			this.numberOfAllocatedBuckets++;
			
			int bucketNumber;
			int[] keys = bucketToSplit.getKeys();
			long[] hashes = bucketToSplit.getHashes();
			TupleAddressArray[] entries = bucketToSplit.getEntries();
			
			HashSet<Integer> positionsOfKeysToRemove = new HashSet<>();
			// reallocate the entries between the two buckets
			for (int i = 0; i < bucketToSplit.getNumberOfKeys(); i++) {
				bucketNumber = (int)this.h2(hashes[i]);
				if (bucketNumber < 0)
					bucketNumber += (this.upperBoundNumberOfBuckets * 2);

				if ((bucketNumber != newBucketNumber) && (bucketNumber != oldBucketNumber)) {
					logger.error("Not a valid hash for splitting this bucket");
					throw new DatabaseException("Not a valid hash for splitting this bucket");
				}

				// if it has been hashed to the new bucket, add it to the new bucket and remove it from the old
				// otherwise, leave in the old bucket
				// since entries are stored in order, prepend to entry list in bucket
				if (bucketNumber == newBucketNumber) {
					positionsOfKeysToRemove.add(i);
					newBucket.putFromSplit(keys[i], hashes[i], entries[i]);
				}
			}
			
			if (positionsOfKeysToRemove.size() > 0)
				bucketToSplit.remove(positionsOfKeysToRemove);
			
			this.nextBucketToSplit++;
			
			if (this.nextBucketToSplit == this.upperBoundNumberOfBuckets) {
				this.upperBoundNumberOfBuckets *= 2;
				this.nextBucketToSplit = 0;
			}
		}
	}
		
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("columns indexed: " + Arrays.toString(this.indexedColumns) + "\n");
		output.append("directory size: " + this.directorySize + "\n");
		output.append("segment size: " + this.segmentSize + "\n");	
		output.append("upperBoundNumberOfBuckets: " + this.upperBoundNumberOfBuckets + "\n");
		output.append("numberOfAllocatedBuckets: " + this.numberOfAllocatedBuckets + "\n");
		output.append("nextBucketToSplit: " + this.nextBucketToSplit + "\n");
		output.append("splitPolicy: " + this.splitPolicy  + "\n");
		output.append("totalSize: " + this.numberOfEntries  + "\n");
		output.append("loadFactor: " + this.getLoadFactor() + "\n");
		output.append("buckets:\n");
		for (int i = 0; i < this.upperBoundNumberOfBuckets + this.nextBucketToSplit; i++) {
			output.append("    bucket: " + i + ", " + this.getBucket(i).toString(this.tupleStore) + "\n");
		}
		return output.toString();
	}
	
	public String toStringStatistics() {
		StringBuilder output = new StringBuilder();
		output.append("\n////START - Tuple Hash Index Statistics - START /////\n");
		output.append("columns indexed: " + Arrays.toString(this.indexedColumns) + "\n");
		output.append("directory size: " + this.directorySize + "\n");
		output.append("segment size: " + this.segmentSize + "\n");	
		output.append("upperBoundNumberOfBuckets: " + this.upperBoundNumberOfBuckets + "\n");
		output.append("numberOfAllocatedBuckets: " + this.numberOfAllocatedBuckets + "\n");
		output.append("nextBucketToSplit: " + this.nextBucketToSplit + "\n");
		output.append("splitPolicy: " + this.splitPolicy  + "\n");
		output.append("totalSize: " + this.numberOfEntries  + "\n");
		output.append("loadFactor: " + this.getLoadFactor() + "\n");
		//output.append("use bloom filter: " + this.useBloom + "\n");
		output.append("////END - Tuple Hash Index Statistics - END /////");
		return output.toString();
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
}
