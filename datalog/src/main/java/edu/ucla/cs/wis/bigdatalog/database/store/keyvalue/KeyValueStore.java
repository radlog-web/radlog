package edu.ucla.cs.wis.bigdatalog.database.store.keyvalue;

import java.io.Serializable;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.database.store.buffer.DynamicBuffer;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;

/*  Base on a Linear Hashing Index
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
public class KeyValueStore implements Serializable {
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(KeyValueStore.class.toString());
	
	private final static int DEFAULT_SIZE_OF_DIRECTORY = 512;//256;
	private final static int DEFAULT_SIZE_OF_SEGMENT = 512;//256;
	private final static int INITIAL_NUMBER_OF_BUCKETS = 256;
	// when the loadfactor reaches SPLIT_POLICY, a bucket is split
	private final static double SPLIT_POLICY = 5;

	protected BucketOfKeyValues[][]	directory;
	protected int 					directorySize;
	protected int					segmentSize;
	protected int					upperBoundNumberOfBuckets;
	protected int 					nextBucketToSplit;
	protected int					numberOfAllocatedBuckets;
	protected int					bucketCapacity;
	protected double				splitPolicy;
	protected int					totalSize;
	protected DynamicBuffer			binaryStore;

	public KeyValueStore() {
		this.initialize(DEFAULT_SIZE_OF_DIRECTORY, DEFAULT_SIZE_OF_SEGMENT, INITIAL_NUMBER_OF_BUCKETS);
	}
	
	private void initialize(int directorySize, int segmentSize, int initialNumberOfBuckets) {
		// initialize data structures
		this.directorySize 				= directorySize;
		this.segmentSize				= segmentSize;
		this.directory 					= new BucketOfKeyValues[directorySize][];
		this.upperBoundNumberOfBuckets 	= initialNumberOfBuckets;
		this.numberOfAllocatedBuckets	= initialNumberOfBuckets;
		this.nextBucketToSplit 			= 0;
		
		this.splitPolicy 				= SPLIT_POLICY;
		this.totalSize 					= 0;
		
		this.binaryStore				= new DynamicBuffer();
		
		int numberOfSegments = (int)Math.ceil(Double.valueOf(this.upperBoundNumberOfBuckets) / Double.valueOf(segmentSize)); 

		// initialize the segments
		for (int i = 0; i < numberOfSegments; i++)
			this.directory[i] = new BucketOfKeyValues[segmentSize];
		
		// initialize the buckets
		for (int i = 0; i < this.numberOfAllocatedBuckets; i++)
			this.directory[this.getSegmentPositionWithinDirectory(i)][this.getBucketPositionWithinSegment(i)] = new BucketOfKeyValues();
	}

	public void clear() {
		for (int i = 0; i < this.numberOfAllocatedBuckets; i++)
			this.getBucket(i).clear();
		
		this.initialize(this.directorySize, this.segmentSize, INITIAL_NUMBER_OF_BUCKETS);
	}
	
	public boolean isEmpty() { return (this.totalSize == 0); }

	public int getSize() { return this.totalSize; }
	
	public int getAggregateBucketSize() {
		int size = 0;
		for (int i = 0; i < this.numberOfAllocatedBuckets; i++)
			size += this.getBucket(i).getSize();

		return size;
	}
	
	public void put(long key, byte[] value) {
		long hash = hash(key);
		int length = value.length;
		
		byte[] data = new byte[value.length + 4];
		data[0] = (byte)((length >> 24) & 0xFF);
		data[1] = (byte)((length >> 16) & 0xFF);
		data[2] = (byte)((length >> 8) & 0xFF);
		data[3] = (byte)(length & 0xFF);

		// copy the values into 1
		System.arraycopy(value, 0, data, 4, value.length);

		// write the length of the value, followed by the value
		int valueAddress = this.binaryStore.append(data);
		
		this.getBucket(this.getBucketNumber(hash)).put(hash, key, valueAddress);
		this.totalSize++;

		if (this.getLoadFactor() > this.splitPolicy)
			this.split();
	}
	
	public byte[] get(long key) {
		long hash = hash(key);
		long valueAddress = this.getBucket(this.getBucketNumber(hash)).get(hash, key);
		if (valueAddress < 0)
			return null;

		byte[] lengthBytes = this.binaryStore.read(valueAddress, 4);
		int length = ((lengthBytes[0] & 0xFF) << 24 
				| (lengthBytes[1] & 0xFF) << 16 
				| (lengthBytes[2] & 0xFF) << 8 
				| (lengthBytes[3] & 0xFF));

		return this.binaryStore.read(valueAddress + 4, length);
	}

	public boolean remove(long key) {
		long hash = hash(key);

		boolean status = this.getBucket(this.getBucketNumber(hash)).remove(hash, key);
		if (status)
			this.totalSize--;
			
		return status;
	}
	
	private static long hash(long key) {
		long hash = key;

		if (hash < 0)
			hash = hash * -1;

		return hash;
	}
	
	private int getBucketNumber(long hash) {
		int bucketNumber = (int)this.h1(hash);

		// we use the 2nd hash function if the 1st hash function returned a bucket position that has already been split this round
		if (bucketNumber < this.nextBucketToSplit)
			bucketNumber = (int)this.h2(hash);
		
		return bucketNumber;
	}
	
	private long h1(long hash) {		
		return (hash % this.upperBoundNumberOfBuckets);
	}
	
	private long h2(long hash) {
		return (hash % (this.upperBoundNumberOfBuckets * 2));
	}
	
	private double getLoadFactor() {
		return Double.valueOf(this.totalSize) / Double.valueOf(this.upperBoundNumberOfBuckets + this.nextBucketToSplit);
	}
	
	// gets the bucket position within a segment
	private int getBucketPositionWithinSegment(int bucketNumber) {
		return bucketNumber % this.segmentSize;
	}
	
	// gets the segment position within the directory
	private int getSegmentPositionWithinDirectory(int bucketNumber) {
		return bucketNumber / this.segmentSize;
	}
	
	private BucketOfKeyValues getBucket(int bucketNumber) {
		return this.directory[this.getSegmentPositionWithinDirectory(bucketNumber)][this.getBucketPositionWithinSegment(bucketNumber)];
	}
	
	private void setBucket(int bucketNumber, BucketOfKeyValues bucket) {
		this.directory[this.getSegmentPositionWithinDirectory(bucketNumber)][this.getBucketPositionWithinSegment(bucketNumber)] = bucket;
	}
		
	private void split() {
		if ((this.nextBucketToSplit + this.upperBoundNumberOfBuckets) < (this.directorySize * this.segmentSize)) {					
			BucketOfKeyValues bucketToSplit = this.getBucket(this.nextBucketToSplit);
			
			int oldBucketNumber = this.nextBucketToSplit;
			int newBucketNumber = this.nextBucketToSplit + this.upperBoundNumberOfBuckets;
			
			// allocate a new segment when we fill the previous 
			if ((newBucketNumber % this.segmentSize) == 0)
				this.directory[newBucketNumber / this.segmentSize] = new BucketOfKeyValues[segmentSize];
						
			BucketOfKeyValues newBucket = new BucketOfKeyValues();
			
			this.setBucket(newBucketNumber, newBucket);
			
			this.numberOfAllocatedBuckets++;
			
			int bucketNumber;
			long[] entry;

			// reallocate the entries between the two buckets
			for (int i = bucketToSplit.getNumberOfEntries() - 1; i >= 0; i--) {
				entry = bucketToSplit.getEntry(i);
				bucketNumber = (int) h2(entry[BucketOfKeyValues.HASH]);
				
				if ((bucketNumber != newBucketNumber) && (bucketNumber != oldBucketNumber)) {
					logger.error("Not a valid hash for splitting this bucket");
					throw new DatabaseException("Not a valid hash for splitting this bucket");
				}
				
				// if it has been hashed to the new bucket, add it to the new bucket and remove it from the old
				// otherwise, leave in the old bucket
				// since entries are stored in order, prepend to entry list in bucket
				if (bucketNumber == newBucketNumber) {
					newBucket.prepend(entry);
					bucketToSplit.remove(entry);
				}
			}
			this.nextBucketToSplit++;
			
			if (this.nextBucketToSplit == this.upperBoundNumberOfBuckets) {
				this.upperBoundNumberOfBuckets *= 2;
				this.nextBucketToSplit = 0;
			}
		}
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("directory size: " + this.directorySize + "\n");
		retval.append("segment size: " + this.segmentSize + "\n");	
		retval.append("upperBoundNumberOfBuckets: " + this.upperBoundNumberOfBuckets + "\n");
		retval.append("numberOfAllocatedBuckets: " + this.numberOfAllocatedBuckets + "\n");
		retval.append("nextBucketToSplit: " + this.nextBucketToSplit + "\n");
		retval.append("splitPolicy: " + this.splitPolicy  + "\n");
		retval.append("totalSize: " + this.totalSize  + "\n");
		retval.append("loadFactor: " + this.getLoadFactor() + "\n");
		retval.append("buckets:\n");
		for (int i = 0; i < this.upperBoundNumberOfBuckets + this.nextBucketToSplit; i++)
			retval.append("    bucket: " + i + ", " + this.getBucket(i).toString() + "\n");
		
		return retval.toString();
	}
}
