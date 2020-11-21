package edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.bytekeysonly;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeOperationStatus;
import edu.ucla.cs.wis.bigdatalog.database.store.type.set.bplustree.BPlusTreeStoreSetStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.measurement.MemorySize;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version of the b+tree is for the DbSet type

// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class BPlusTreeByteKeysOnly 
	extends BPlusTreeStoreSetStructure<BPlusTreeByteKeysOnlyPage, BPlusTreeByteKeysOnlyLeaf, BPlusTreeByteKeysOnlyNode> 
	implements MemorySize, Serializable {
	private static final long serialVersionUID = 1L;
		
	protected BPlusTreeByteKeysOnlyInsertResult insertResult;
	
	public BPlusTreeByteKeysOnly() { super(); }
			
	public BPlusTreeByteKeysOnly(int nodeSize, int bytesPerKey, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, bytesPerKey, keyColumns, keyColumnTypes);
		this.insertResult = new BPlusTreeByteKeysOnlyInsertResult();
		this.initialize();
	}

	@Override
	protected BPlusTreeByteKeysOnlyPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeByteKeysOnlyLeaf(this.nodeSize, this.bytesPerKey);
		
		return new BPlusTreeByteKeysOnlyNode(this.nodeSize, this.bytesPerKey);
	}
		
	public DbTypeBase[] insert(DbTypeBase[] key) {
		byte[] keyBytes = this.getKey(key);
				
		// case 1 - room to insert in root node
		this.rootNode.insert(keyBytes, this.insertResult);		
		if (this.insertResult.status == BPlusTreeOperationStatus.NEW)
			this.numberOfEntries++;

		if (this.insertResult.newPage != null) {
			// case 2 we have grow the tree as the root node was split 
			BPlusTreeByteKeysOnlyNode newRoot = (BPlusTreeByteKeysOnlyNode)this.allocatePage();
			newRoot.children[0] = this.rootNode;
			newRoot.children[1] = this.insertResult.newPage;
			byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
			System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
			
			newRoot.highWaterMark += 2;
			this.rootNode = newRoot;
		}
		return key;
	}
	
	public DbTypeBase[] get(DbTypeBase[] key) {
		byte[] valueBytes = this.get(getKey(key));
		if (valueBytes == null)
			return null;
		return key;
	}
		
	public byte[] get(byte[] key) {
		if (this.rootNode == null)
			return null;
		
		return this.rootNode.get(key);
	}
	
	@Override
	public boolean delete(DbTypeBase key) {
		byte[] keyBytes = key.getBytes();
		return this.delete(keyBytes);
	}
	
	public boolean delete(byte[] key) {
		if (this.rootNode == null)
			return false;
		
		boolean status = this.rootNode.delete(key); 
		if (status)
			this.numberOfEntries--;
		return status;		
	}	
	
	public void deleteAll() {
		if (this.rootNode == null)
			return;
			
		this.rootNode.deleteAll();
		this.rootNode = null;
		this.initialize();
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("\n////START - B+ Tree Keys Values - START /////\n");
		if (this.rootNode != null)
			retval.append(this.rootNode.toString(0));
		else
			retval.append("empty");
		retval.append("\n////END - B+ Tree Keys Values - END /////\n");
		return retval.toString();
	}
	
	public String toStringShort() {
		StringBuilder retval = new StringBuilder();
		if (this.rootNode != null)
			retval.append(this.rootNode.toStringShort());
		else
			retval.append("empty");
		return retval.toString();
	}
	
	public String toStringStatistics() {
		StringBuilder retval = new StringBuilder();
		retval.append("\n////START - B+ Tree Keys Values Statistics - START /////\n");		
		if (this.rootNode == null) {
			retval.append("empty");
		} else {
			retval.append("height: " + this.getHeight() + "\n");
			retval.append("# of entries: " + this.getNumberOfEntries() + "\n");
		}
		retval.append("////END - B+ Tree Keys Values Statistics - END /////\n");
		return retval.toString();
	}
	
	public BPlusTreeByteKeysOnlyLeaf getFirstChild() {
		if (this.getHeight() == -1)
			return null;
		
		if (this.getHeight() == 0)
			return (BPlusTreeByteKeysOnlyLeaf)this.rootNode; 
		
		return ((BPlusTreeByteKeysOnlyNode)this.rootNode).getFirstChild();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {
		return rootNode.getSizeOf();
	}
	
	public byte[] getKey(DbTypeBase[] columns) {
		byte[] bytes = new byte[this.bytesPerKey];
		int offset = 0;
		for (int i = 0; i < this.keyColumnTypes.length; i++)
			offset = columns[this.keyColumns[i]].getBytes(bytes, offset);
		
		return bytes; 
	}
}
