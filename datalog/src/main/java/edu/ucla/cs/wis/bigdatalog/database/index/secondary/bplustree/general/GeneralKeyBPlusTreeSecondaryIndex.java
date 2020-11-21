package edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.general;

import java.io.Serializable;
import java.util.Arrays;

import edu.ucla.cs.wis.bigdatalog.database.AddressedTuple;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.GeneralKeySecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.TupleAddressArray;
import edu.ucla.cs.wis.bigdatalog.database.index.secondary.bplustree.BPlusTreeSecondaryIndex;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.AddressedTupleStore;
import edu.ucla.cs.wis.bigdatalog.exception.DatabaseException;
import edu.ucla.cs.wis.bigdatalog.measurement.MemoryMeasurement;

// this version only holds byte keys
// used for storing single column relations

public class GeneralKeyBPlusTreeSecondaryIndex 
	extends GeneralKeySecondaryIndex<AddressedTupleStore> 
	implements BPlusTreeSecondaryIndex<GeneralKeyBPlusTreeSecondaryIndexLeaf>, Serializable {
	//private static Logger logger = LoggerFactory.getLogger(GeneralTupleBPlusTreeIndex.class.toString());

	private static final long serialVersionUID = 1L;
	
	protected int nodeSize;
	protected GeneralKeyBPlusTreeSecondaryIndexPage rootNode;
	protected GeneralKeyBPlusTreeSecondaryIndexResult insertResult;
	
	public GeneralKeyBPlusTreeSecondaryIndex(Relation<AddressedTuple> relation, int[] indexedColumns, int nodeSize) {
		super(relation, indexedColumns);		

		this.bytesPerKey = this.getKeySize();			
		if (this.bytesPerKey < 1)
			throw new DatabaseException("The schema for this tuplestore is unknown.  Unable to initialize TupleBPlusTreeIndex.  Use hash index instead.");
			
		this.nodeSize = nodeSize;
		//this.nodeSize = Integer.parseInt(DeALSContext.getConfiguration().getProperty("deals.database.indexes.bplustree.nodesize"));
		this.insertResult = new GeneralKeyBPlusTreeSecondaryIndexResult();
		this.initialize();
	}
		
	protected void initialize() {
		this.numberOfEntries = 0;
		this.rootNode = null;
		this.rootNode = this.allocatePage();
	}
	
	public int getNodeSize() { return this.nodeSize; }
	
	public int getSize() { return this.getNumberOfEntries(); }
			
	public int getHeight() {
		if (this.rootNode.isEmpty())
			return -1;
		
		return this.rootNode.getHeight();
	}
	
	public boolean isEmpty() {
		return this.rootNode.isEmpty();
	}
	
	private GeneralKeyBPlusTreeSecondaryIndexPage allocatePage() {
		if (this.rootNode == null)
			return new GeneralKeyBPlusTreeSecondaryIndexLeaf(this.nodeSize, this.bytesPerKey);
		
		return new GeneralKeyBPlusTreeSecondaryIndexNode(this.nodeSize, this.bytesPerKey);
	}
	
	@Override
	public boolean doPut(byte[] key, AddressedTuple tuple) {		
		// case 1 - room to insert in root node
		this.rootNode.insert(key, tuple.address, this.insertResult);

		if (this.insertResult.success)
			this.numberOfEntries++;

		if (this.insertResult.newPage == null)
			return this.insertResult.success;
		
		// case 2 we have grow the tree as the root node was split 
		GeneralKeyBPlusTreeSecondaryIndexNode newRoot = (GeneralKeyBPlusTreeSecondaryIndexNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = this.insertResult.newPage;
		
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
		
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
		return true;
	}

	@Override
	public AddressedTuple doGet(byte[] key, Tuple tuple) {
		if (this.rootNode.isEmpty())
			return null;
		
		TupleAddressArray entry = this.rootNode.get(key);
		
		if (entry != null) {
			int[] addresses = entry.getAddresses();
			for (int j = 0; j < entry.getNumberOfAddresses(); j++) {
				if (this.tupleStore.get(addresses[j], this.capturedTuple) > 0) {
					if (tuple.equals(this.capturedTuple.columns, this.totalNumberOfColumns))
						return this.capturedTuple;
				}
			}
		}

		return null;
	}
	
	@Override
	protected int[] doGetSimilar(byte[] key) {
		if (this.rootNode.isEmpty())
			return null;
		
		TupleAddressArray entry = this.rootNode.get(key);
		if (entry != null)
			return entry.getAddresses();
		
		return null;
	}

	@Override
	public boolean doRemove(byte[] key, AddressedTuple tuple) {
		if (this.rootNode.isEmpty())
			return true;
				
		if (this.rootNode.delete(key, tuple.address)) {
			this.numberOfEntries--;
			if (this.numberOfEntries == 0) {
				this.rootNode = null;
				this.rootNode = this.allocatePage();
			}
			return true;
		}
		return false;
	}

	@Override
	public void doClear() {
		if (this.rootNode.isEmpty())
			return;
			
		this.rootNode.deleteAll();
		this.rootNode = null;
		this.initialize();		
	}
	
	@Override
	public GeneralKeyBPlusTreeSecondaryIndexLeaf getFirstChild() {
		if (this.getHeight() == -1)
			return null;
		
		if (this.getHeight() == 0)
			return (GeneralKeyBPlusTreeSecondaryIndexLeaf)this.rootNode; 
		
		return ((GeneralKeyBPlusTreeSecondaryIndexNode)this.rootNode).getFirstChild();
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("\n////START - B+ Tree Index Statistics - START /////\n");
		if (this.rootNode.isEmpty()) {
			output.append("empty");
		} else {
			output.append("key columns: " + Arrays.toString(this.indexedColumns) + " | ");
			output.append("height: " + this.getHeight() + "\n");
			output.append("# of entries: " + this.getNumberOfEntries() + "\n");
			output.append(this.rootNode.toString(0));
		}
		output.append("////END - B+ Tree Index Statistics - END /////");
		return output.toString();
	}

	public String toStringStatistics() { 
		StringBuilder output = new StringBuilder();
		output.append("\n////START - B+ Tree Index Statistics - START /////\n");
		if (this.rootNode.isEmpty()) {
			output.append("empty");
		} else {
			output.append("key columns: " + Arrays.toString(this.indexedColumns) + " | ");
			output.append("height: " + this.getHeight() + "\n");
			output.append("# of entries: " + this.getNumberOfEntries() + "\n");
		}
		output.append("////END - B+ Tree Index Statistics - END /////");
		return output.toString();
	}
	
	@Override
	public MemoryMeasurement getSizeOf() {
		return this.rootNode.getSizeOf();
	}
}
