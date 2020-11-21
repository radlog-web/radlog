package edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.hashtable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.database.index.MurmurHash;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStoreGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.type.keyvaluestore.KeyValueStorePutResult;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class GeneralHashTable 
	extends HashTable<GeneralBucket> 
	implements Serializable {
	private static Logger logger = LoggerFactory.getLogger(GeneralHashTable.class.toString());
	
	private static final long serialVersionUID = 1L;

	protected int				bytesPerKey;
	protected DataType[]		keyTypes;
	private HashTableGetResult 	getResult;
	private HashTablePutResult	putResult;

	public GeneralHashTable() { super(); }
	
	public GeneralHashTable(DataType[] keyTypes, DataType[] valueTypes,  double splitPolicy, 
			int directorySize, int segmentSize, int numberOfInitialBuckets, TypeManager typeManager) {
		super(valueTypes,  splitPolicy, directorySize, segmentSize, numberOfInitialBuckets, typeManager);
		if (keyTypes == null)
			throw new DatabaseException("HashTable can not be created without at least one key type.");
		
		this.keyTypes = keyTypes;
		
		int bytesPerKey = 0;
		for (int i = 0; i < this.keyTypes.length; i++)
			bytesPerKey += this.keyTypes[i].getNumberOfBytes();
		
		this.bytesPerKey = bytesPerKey;
		this.getResult = new HashTableGetResult(this.bytesPerValue);
		this.putResult = new HashTablePutResult(this.bytesPerValue);
		this.initialize();
	}
	
	protected void initialize() {	
		super.initialize();
		this.directory 	= new GeneralBucket[this.directorySize][];
		
		int numberOfSegments = (int)Math.ceil(Double.valueOf(this.upperBoundNumberOfBuckets) / Double.valueOf(segmentSize)); 
		
		// initialize the segments
		for (int i = 0; i < numberOfSegments; i++)
			this.directory[i] = new GeneralBucket[segmentSize];
		
		// initialize the buckets
		for (int i = 0; i < this.numberOfAllocatedBuckets; i++)
			this.directory[this.getSegmentPositionWithinDirectory(i)][this.getBucketPositionWithinSegment(i)] 
					= new GeneralBucket(this.bytesPerKey, this.bytesPerValue);
	}
	
	public DataType[] getKeyTypes() { return this.keyTypes; }
	
	@Override
	public void get(DbTypeBase key, KeyValueStoreGetResult result) {
		byte[] keyBytes = key.getBytes();
		long hash = hash(keyBytes);
		
		this.getBucket(this.getBucketNumber(hash)).get(keyBytes, hash, this.getResult);		
		if (!this.getResult.success) {
			result.success = false;
			return;
		}
		
		result.value = DbTypeBase.loadFrom(this.getValueDataType(), this.getResult.value, this.typeManager);
		result.success = true;
	}

	@Override
	public void put(DbTypeBase key, DbTypeBase value, KeyValueStorePutResult result) {
		byte[] keyBytes = key.getBytes();
		byte[] valueBytes = value.getBytes();
		
		long hash = hash(keyBytes);		

		this.getBucket(this.getBucketNumber(hash)).put(keyBytes, hash, valueBytes, this.putResult);
		if (this.putResult.status == KeyValueOperationStatus.NEW)
			this.numberOfEntries++;
			
		if (this.getLoadFactor() > this.splitPolicy)
			this.split();
		
		result.status = this.putResult.status;
		if (result.status == KeyValueOperationStatus.UPDATE)
			result.oldValue = DbTypeBase.loadFrom(this.getValueDataType(), this.putResult.oldValue, this.typeManager);//result.oldValue.load(this.putResult.oldValue);
	}
	
	@Override
	public boolean remove(DbTypeBase key) {
		byte[] keyBytes = key.getBytes();
		long hash = hash(keyBytes);
		boolean status = this.getBucket(this.getBucketNumber(hash)).remove(keyBytes, hash);
		if (status)
			this.numberOfEntries--;
		return status;
	}

	private static long hash(byte[] key) {	
		return MurmurHash.hash(key);
	}

	private void split() {
		if ((this.nextBucketToSplit + this.upperBoundNumberOfBuckets) < (this.directorySize * this.segmentSize)) {					
			GeneralBucket bucketToSplit = this.getBucket(this.nextBucketToSplit);
			
			int oldBucketNumber = this.nextBucketToSplit;
			int newBucketNumber = this.nextBucketToSplit + this.upperBoundNumberOfBuckets;
			
			// allocate a new segment when we fill the previous 
			if ((newBucketNumber % this.segmentSize) == 0)
				this.directory[newBucketNumber / this.segmentSize] = new GeneralBucket[segmentSize];
						
			GeneralBucket newBucket = new GeneralBucket(this.bytesPerKey, this.bytesPerValue);
			
			this.setBucket(newBucketNumber, newBucket);
			
			this.numberOfAllocatedBuckets++;
			
			int bucketNumber;
			byte[] keys = bucketToSplit.getKeys();
			long[] hashes = bucketToSplit.getHashes();
			byte[] values = bucketToSplit.getValues();
						
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
				if (bucketNumber == newBucketNumber) {
					positionsOfKeysToRemove.push(i);
					newBucket.putFromSplit(Arrays.copyOfRange(keys, i * this.bytesPerKey, (i + 1) * this.bytesPerKey), 
							hashes[i], Arrays.copyOfRange(values, i * this.bytesPerValue, (i + 1) * this.bytesPerValue));
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
		StringBuilder retval = new StringBuilder();
		retval.append("key types : " + Arrays.toString(this.keyTypes) + "\n");
		retval.append("value types : " + Arrays.toString(this.valueTypes) + "\n");
		retval.append("directory size: " + this.directorySize + "\n");
		retval.append("segment size: " + this.segmentSize + "\n");	
		retval.append("upperBoundNumberOfBuckets: " + this.upperBoundNumberOfBuckets + "\n");
		retval.append("numberOfAllocatedBuckets: " + this.numberOfAllocatedBuckets + "\n");
		retval.append("nextBucketToSplit: " + this.nextBucketToSplit + "\n");
		retval.append("splitPolicy: " + this.splitPolicy  + "\n");
		retval.append("totalSize: " + this.numberOfEntries  + "\n");
		retval.append("loadFactor: " + this.getLoadFactor() + "\n");
		retval.append("buckets:\n");
		for (int i = 0; i < this.upperBoundNumberOfBuckets + this.nextBucketToSplit; i++)
			retval.append("    bucket: " + i + ", " + this.getBucket(i).toString() + "\n");
		
		return retval.toString();
	}
	
	public String toStringStatistics() {
		StringBuilder retval = new StringBuilder();
		retval.append("\n////START - Tuple Hash Index Statistics - START /////\n");
		retval.append("key types : " + Arrays.toString(this.keyTypes) + "\n");
		retval.append("value types : " + Arrays.toString(this.valueTypes) + "\n");
		retval.append("directory size: " + this.directorySize + "\n");
		retval.append("segment size: " + this.segmentSize + "\n");	
		retval.append("upperBoundNumberOfBuckets: " + this.upperBoundNumberOfBuckets + "\n");
		retval.append("numberOfAllocatedBuckets: " + this.numberOfAllocatedBuckets + "\n");
		retval.append("nextBucketToSplit: " + this.nextBucketToSplit + "\n");
		retval.append("splitPolicy: " + this.splitPolicy  + "\n");
		retval.append("totalSize: " + this.numberOfEntries  + "\n");
		retval.append("loadFactor: " + this.getLoadFactor() + "\n");
		retval.append("////END - Tuple Hash Index Statistics - END /////");
		return retval.toString();
	}
}
