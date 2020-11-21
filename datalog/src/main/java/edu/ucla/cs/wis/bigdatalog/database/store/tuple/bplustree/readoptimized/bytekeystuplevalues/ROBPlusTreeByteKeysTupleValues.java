package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.bytekeystuplevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.TupleBPlusTreeTupleStoreROStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version of the b+tree could be used with:
// 1) unordered heap stores as an index
// 2) as storage - the first column is the key and the remaining n-1 columns are the data

// B+ tree implementation with help from
//  http://www.cs.washington.edu/education/courses/cse326/08sp/lectures/11-b-trees.pdf
//  and http://ozark.hendrix.edu/~burch/cs/340/reading/btree/index.html
// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class ROBPlusTreeByteKeysTupleValues 
	extends BPlusTreeStoreStructure<ROBPlusTreeByteKeysTupleValuesPage, ROBPlusTreeByteKeysTupleValuesLeaf, ROBPlusTreeByteKeysTupleValuesNode>
	implements TupleBPlusTreeTupleStoreROStructure, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected ROBPlusTreeGetResult getResult;
	protected byte[] key;

	public ROBPlusTreeByteKeysTupleValues() { super(); }
	
	public ROBPlusTreeByteKeysTupleValues(int nodeSize, int bytesPerKey, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, bytesPerKey, keyColumns, keyColumnTypes);
		this.getResult = new ROBPlusTreeGetResult();
		this.initialize();
		this.key = new byte[this.bytesPerKey];
	}

	@Override
	protected ROBPlusTreeByteKeysTupleValuesPage allocatePage() {
		if (this.rootNode == null)
			return new ROBPlusTreeByteKeysTupleValuesLeaf(this.nodeSize, this.bytesPerKey);
		
		return new ROBPlusTreeByteKeysTupleValuesNode(this.nodeSize, this.bytesPerKey);
	}

	@Override
	public void insert(Tuple tuple) {
		this.insert(this.getKey(tuple.columns), tuple);
	}
		
	public Boolean insert(byte[] key, Tuple tuple) {
		// case 1 - room to insert in root node
		Pair<ROBPlusTreeByteKeysTupleValuesPage, Boolean> retval = this.rootNode.insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
				
		if (retval.getFirst() == null)
			return retval.getSecond();

		// case 2 we have grow the tree as the root node was split 
		ROBPlusTreeByteKeysTupleValuesNode newRoot = (ROBPlusTreeByteKeysTupleValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = retval.getFirst();
				
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
				
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
		return retval.getSecond();
	}
	
	@Override
	public Tuple[] get(DbTypeBase[] keyColumns) {
		int offset = 0;
		for (int i = 0; i < this.keyColumnTypes.length; i++)
			offset = keyColumns[this.keyColumns[i]].getBytes(this.key, offset);		

		this.rootNode.get(this.key, this.getResult);
		if (this.getResult.success)
			return this.getResult.tuples;
		
		return null;
	}
	@Override
	public boolean delete(Tuple tuple) {
		return this.delete(this.getKey(tuple.columns));
	}
	
	public boolean delete(byte[] key) {
		if (this.rootNode == null)
			return false;
		
		boolean status = this.rootNode.delete(key);
		if (status)
			this.numberOfEntries--;
		return status;
	}	
}
