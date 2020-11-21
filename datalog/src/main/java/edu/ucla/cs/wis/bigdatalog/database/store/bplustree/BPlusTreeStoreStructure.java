package edu.ucla.cs.wis.bigdatalog.database.store.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.BigEncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

abstract public class BPlusTreeStoreStructure<P extends BPlusTreeElement, 
		L extends BPlusTreeLeaf<L>, N extends BPlusTreeNode<P, L>> 
	implements BPlusTree, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected int nodeSize;
	protected int bytesPerKey;
	protected int[] keyColumns;
	protected DataType[] keyColumnTypes;
	protected int numberOfEntries;
	protected P rootNode;
	
	protected BPlusTreeStoreStructure(){}
	
	public BPlusTreeStoreStructure(int nodeSize, int bytesPerKey, int[] keyColumns, DataType[] keyColumnTypes) {
		this.nodeSize = nodeSize;
		this.bytesPerKey = bytesPerKey;
		this.keyColumns = keyColumns;
		this.keyColumnTypes = keyColumnTypes;
	}
			
	public int getNodeSize() { return this.nodeSize; }
	
	public int getBytesPerKey() { return this.bytesPerKey; }
	
	public int[] getKeyColumns() { return this.keyColumns; }
	
	public DataType[] getKeyColumnTypes() { return this.keyColumnTypes; }
	
	protected void initialize() {
		this.numberOfEntries = 0;
		this.rootNode = this.allocatePage();
	}
		
	public int getHeight() {
		if (this.rootNode.isEmpty())
			return -1;
		
		return this.rootNode.getHeight();
	}
	
	public boolean isEmpty() {
		return this.rootNode.isEmpty();
	}
		
	public int getNumberOfEntries() { return this.numberOfEntries; } 

	public void deleteAll() {
		if (this.rootNode.isEmpty())
			return;

		this.rootNode.deleteAll();
		this.rootNode = null;
		this.initialize();
	}
	
	public byte[] getKey(DbTypeBase[] columns) {
		byte[] bytes = new byte[this.bytesPerKey];
		int offset = 0;
		for (int i = 0; i < this.keyColumnTypes.length; i++)
			offset = columns[this.keyColumns[i]].getBytes(bytes, offset);
		
		return bytes; 
	}
	
	public int getKeyI(DbTypeBase[] columns) {
		//if (this.keyColumnTypes[0] == DataType.INTEGER)
			//return ((DbInteger)columns[this.keyColumns[0]]).value;
		
		return ((EncodedType)columns[this.keyColumns[0]]).getKey();
	}
	
	protected long getKeyL(DbTypeBase[] columns) {
		if (this.keyColumns.length == 2) {
			int keyPart1 = ((EncodedType)columns[this.keyColumns[0]]).getKey();
			int keyPart2 = ((EncodedType)columns[this.keyColumns[1]]).getKey();
			/*if (this.keyColumnTypes[0] == DataType.INTEGER)
				keyPart1 = ((DbInteger)columns[this.keyColumns[0]]).value;
			else
				keyPart1 = ((DbString)columns[this.keyColumns[0]]).value;
			
			if (this.keyColumnTypes[1] == DataType.INTEGER)
				keyPart2 = ((DbInteger)columns[this.keyColumns[1]]).value;
			else
				keyPart2 = ((DbString)columns[this.keyColumns[1]]).value;
			
			System.out.println(keyPart1);
			System.out.println(keyPart2);
			System.out.println("upper key: " + ((long)keyPart1 << 32));
			System.out.println("lower key: " + (keyPart2 & 0xffffffffL));
			System.out.println("lower key: " + keyPart2);
			System.out.println("key      : " + (((long)keyPart1 << 32) | (keyPart2 & 0xffffffffL)) + " " + Arrays.toString(columns));*/
			return ((long)keyPart1 << 32) | (keyPart2 & 0xffffffffL);
		}
		
		//return ((DbLong)columns[this.keyColumns[0]]).value;
		return ((BigEncodedType)columns[this.keyColumns[0]]).getKeyL();
	}
		
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("\n////START - B+ Tree - START /////\n");
		if (this.rootNode.isEmpty())
			retval.append("empty");
		else
			retval.append(this.rootNode.toString(0));
					
		retval.append("\n////END - B+ Tree - END /////\n");
		return retval.toString();
	}
	
	public String toStringShort() {
		StringBuilder retval = new StringBuilder();
		if (this.rootNode.isEmpty())
			retval.append("empty");
		else
			retval.append(this.rootNode.toStringShort());
			
		return retval.toString();
	}
	
	public String toStringStatistics() {
		StringBuilder retval = new StringBuilder();
		retval.append("\n////START - B+ Tree Statistics - START /////\n");		
		if (this.rootNode.isEmpty()) {
			retval.append("empty");
		} else {
			retval.append("height: " + this.getHeight() + "\n");
			retval.append("# of entries: " + this.getNumberOfEntries() + "\n");
		}
		retval.append("////END - B+ Tree Statistics - END /////\n");
		return retval.toString();
	}
	
	@SuppressWarnings("unchecked")
	public L getFirstChild() {
		if (this.getHeight() == -1)
			return null;
		
		if (this.getHeight() == 0)
			return (L) this.rootNode; 
		
		return ((N) this.rootNode).getFirstChild();
	}
	
	public MemoryMeasurement getSizeOf() {
		return rootNode.getSizeOf();
	}

	abstract protected P allocatePage();
		
	public static int[] getKeyColumns(DataType[] schema) {
		int[] arr = new int[schema.length];
		for (int i = 0; i < schema.length; i++)
			arr[i] = i;
		
		return arr;
	}
	
	public static int getBytesPerKey(DataType[] schema, int[] keyColumns) {		
		int bytesPerKey = 0;
		for (int i = 0; i < keyColumns.length; i++)
			bytesPerKey += schema[keyColumns[i]].getNumberOfBytes();
		
		return bytesPerKey;
	}
	
}
