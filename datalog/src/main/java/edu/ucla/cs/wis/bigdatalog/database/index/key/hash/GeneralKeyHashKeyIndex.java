package edu.ucla.cs.wis.bigdatalog.database.index.key.hash;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.database.index.key.GeneralKeyKeyIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.key.KeyIndexResult;

public class GeneralKeyHashKeyIndex extends GeneralKeyKeyIndex implements MemorySize, Serializable {
	private static Logger logger = LoggerFactory.getLogger(GeneralKeyHashKeyIndex.class.toString());
	
	private static final long serialVersionUID = 1L;
	
	//private static final int DEFAULT_SIZE_OF_DIRECTORY = 512;
	//private static final int DEFAULT_SIZE_OF_SEGMENT = 512;
	//private static final int INITIAL_NUMBER_OF_BUCKETS = 512;
	// when the loadfactor reaches SPLIT_POLICY, a bucket is split
	//private static final double SPLIT_POLICY = 10;

	protected final int						numberOfInitialBuckets;
	protected GeneralKeyBucket[][]			directory;
	protected final int 					directorySize;
	protected final int						segmentSize;
	protected int							upperBoundNumberOfBuckets;
	protected int 							nextBucketToSplit;
	protected int							numberOfAllocatedBuckets;
	protected int							bucketCapacity;
	protected final double					splitPolicy;
	public long								timeSpentGetBucketNumber;
	public long								timeSpentGetBucket;
	public long								timeSpentGetLoadFactor;
	public long								timeSpentDoPut;
	
	public GeneralKeyHashKeyIndex(Relation relation, int[] keyColumns, double splitPolicy, 
			int directorySize, int segmentSize, int numberOfInitialBuckets) {
		super(relation, keyColumns);
		
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
		
		this.build();
	}
	
	private void initialize() {
		// initialize data structures
		this.directory 					= new GeneralKeyBucket[this.directorySize][];
		this.upperBoundNumberOfBuckets 	= this.numberOfInitialBuckets;
		this.numberOfAllocatedBuckets	= this.numberOfInitialBuckets;
		this.nextBucketToSplit 			= 0;
		
		this.numberOfEntries 			= 0;
		
		int numberOfSegments = (int)Math.ceil(Double.valueOf(this.upperBoundNumberOfBuckets) / Double.valueOf(this.segmentSize)); 

		// initialize the segments
		for (int i = 0; i < numberOfSegments; i++)
			this.directory[i] = new GeneralKeyBucket[this.segmentSize];
		
		this.bytesPerKey = this.getKeySize();

		// initialize the buckets
		for (int i = 0; i < this.numberOfAllocatedBuckets; i++)
			this.directory[this.getSegmentPositionWithinDirectory(i)][this.getBucketPositionWithinSegment(i)] = new GeneralKeyBucket(this.bytesPerKey);
		
		this.timeSpentGetBucketNumber = 0;
		this.timeSpentGetBucket = 0;
		this.timeSpentGetLoadFactor = 0;
	}

	public void doClear() {				
		this.initialize();
	}

	public void doPut(byte[] key, KeyIndexResult result) {
		long hash = hash(key);
		
		// if we return false, we already have the key		
		if (this.getBucket(this.getBucketNumber(hash)).put(key, hash)) {
			this.numberOfEntries++;
			if (this.getLoadFactor() > this.splitPolicy)
				this.split();
			result.success = true;
			return;
		}
		result.success = false;		
	}
	
	public boolean doGet(byte[] key) {
		long hash = hash(key);		
		return this.getBucket(this.getBucketNumber(hash)).get(key, hash);
	}
		
	public boolean doRemove(byte[] key) {
		long hash = hash(key);
		boolean status = this.getBucket(this.getBucketNumber(hash)).remove(key, hash);

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
	
	private static long hash(byte[] key) {
		//int hash = Hashing.murmur3_32().hashBytes(key).asInt() & 0x7fffffff;
		return MurmurHash.hash(key);
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
	
	private GeneralKeyBucket getBucket(int bucketNumber) {
		return this.directory[bucketNumber / this.segmentSize][bucketNumber % this.segmentSize];
	}
	
	private void setBucket(int bucketNumber, GeneralKeyBucket bucket) {
		//this.directory[this.getSegmentPositionWithinDirectory(bucketNumber)][this.getBucketPositionWithinSegment(bucketNumber)] = bucket;
		this.directory[bucketNumber / this.segmentSize][bucketNumber % this.segmentSize] = bucket;
	}

	private void split() {
		if ((this.nextBucketToSplit + this.upperBoundNumberOfBuckets) < (this.directorySize * this.segmentSize)) {					
			GeneralKeyBucket bucketToSplit = this.getBucket(this.nextBucketToSplit);
			
			int oldBucketNumber = this.nextBucketToSplit;
			int newBucketNumber = this.nextBucketToSplit + this.upperBoundNumberOfBuckets;
			
			// allocate a new segment when we fill the previous 
			if ((newBucketNumber % this.segmentSize) == 0)
				this.directory[newBucketNumber / this.segmentSize] = new GeneralKeyBucket[this.segmentSize];
						
			GeneralKeyBucket newBucket = new GeneralKeyBucket(this.bytesPerKey);
			
			this.setBucket(newBucketNumber, newBucket);
			
			this.numberOfAllocatedBuckets++;
			
			int bucketNumber;
			byte[] keys = bucketToSplit.getKeys();
			long[] hashes = bucketToSplit.getHashes();
			LinkedList<Integer> positionsOfKeysToRemove = new LinkedList<>();
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
				// we add new entry to newbucket here, and remove from oldbucket in bulk
				if (bucketNumber == newBucketNumber) {
					positionsOfKeysToRemove.push(i);
					newBucket.putFromSplit(Arrays.copyOfRange(keys, i * this.bytesPerKey, (i+1) * this.bytesPerKey), hashes[i]);
				}
			}
			
			if (positionsOfKeysToRemove.size() > 0)
				bucketToSplit.removeKeys(positionsOfKeysToRemove);
			
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
			output.append("    bucket: " + i + ", " + this.getBucket(i).toString() + "\n");
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
		output.append("////END - Tuple Hash Index Statistics - END /////");
		return output.toString();
	}
	
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
