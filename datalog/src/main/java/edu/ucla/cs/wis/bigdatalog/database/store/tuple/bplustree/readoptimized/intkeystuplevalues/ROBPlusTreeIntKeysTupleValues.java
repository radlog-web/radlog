package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.intkeystuplevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.TupleBPlusTreeTupleStoreROStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version of the b+tree could be used with:
// 1) unordered heap stores as an index
// 2) as storage - the first column is the key and the remaining n-1 columns are the data

// B+ tree implementation with help from
//  http://www.cs.washington.edu/education/courses/cse326/08sp/lectures/11-b-trees.pdf
//  and http://ozark.hendrix.edu/~burch/cs/340/reading/btree/index.html
// THIS CLASS SHOULD ONLY BE USED WHEN KEYS ARE UNIQUE
public class ROBPlusTreeIntKeysTupleValues 
	extends BPlusTreeStoreStructure<ROBPlusTreeIntKeysTupleValuesPage, ROBPlusTreeIntKeysTupleValuesLeaf, ROBPlusTreeIntKeysTupleValuesNode>
	implements TupleBPlusTreeTupleStoreROStructure, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected boolean isIntegerKey;
	protected ROBPlusTreeGetResult getResult;
	
	public ROBPlusTreeIntKeysTupleValues() { super(); }
	
	public ROBPlusTreeIntKeysTupleValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, 4, keyColumns, keyColumnTypes);
		this.isIntegerKey = (keyColumnTypes[0] == DataType.INT);
		this.getResult = new ROBPlusTreeGetResult();
		this.initialize();
	}

	@Override
	protected ROBPlusTreeIntKeysTupleValuesPage allocatePage() {
		if (this.rootNode == null)
			return new ROBPlusTreeIntKeysTupleValuesLeaf(this.nodeSize);
		
		return new ROBPlusTreeIntKeysTupleValuesNode(this.nodeSize);
	}

	@Override
	public void insert(Tuple tuple) {
		if (this.isIntegerKey)
			this.insert(((DbInteger)tuple.columns[this.keyColumns[0]]).getValue(), tuple);
		else
			this.insert(((EncodedType)tuple.columns[this.keyColumns[0]]).getKey(), tuple);
	}

	public Boolean insert(int key, Tuple tuple) {
		// case 1 - room to insert in root node
		Pair<ROBPlusTreeIntKeysTupleValuesPage, Boolean> retval = this.rootNode.insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
				
		if (retval.getFirst() == null)
			return retval.getSecond();

		// case 2 we have grow the tree as the root node was split 
		ROBPlusTreeIntKeysTupleValuesNode newRoot = (ROBPlusTreeIntKeysTupleValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = retval.getFirst();
		newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
				
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
		return retval.getSecond();
	}
	
	@Override
	public Tuple[] get(DbTypeBase[] keyColumns) {
		// this method will only pass the necessary number of keys, so using this.keyColumns[] will error
		if (this.isIntegerKey)
			this.rootNode.get(((DbInteger)keyColumns[0]).getValue(), this.getResult);
		else
			this.rootNode.get(((EncodedType)keyColumns[0]).getKey(), this.getResult);
		
		if (this.getResult.success)
			return this.getResult.tuples;
		
		return null;
	}
	
	@Override
	public boolean delete(Tuple tuple) {
		return this.delete(((EncodedType)tuple.columns[this.keyColumns[0]]).getKey());
	}
	
	public boolean delete(int key) {
		if (this.rootNode == null)
			return false;
		
		boolean status = this.rootNode.delete(key);
		if (status)
			this.numberOfEntries--;
		return status;
	}	
}
