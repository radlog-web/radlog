package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.longkeystuplevalues;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.ROBPlusTreeGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.readoptimized.TupleBPlusTreeTupleStoreROStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.BigEncodedType;
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
public class ROBPlusTreeLongKeysTupleValues 
	extends BPlusTreeStoreStructure<ROBPlusTreeLongKeysTupleValuesPage, ROBPlusTreeLongKeysTupleValuesLeaf, ROBPlusTreeLongKeysTupleValuesNode>
	implements TupleBPlusTreeTupleStoreROStructure, Serializable {
	private static final long serialVersionUID = 1L;
	protected ROBPlusTreeGetResult getResult;
		
	public ROBPlusTreeLongKeysTupleValues() { super(); }
	
	public ROBPlusTreeLongKeysTupleValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, 8, keyColumns, keyColumnTypes);
		this.getResult = new ROBPlusTreeGetResult();
		this.initialize();
	}

	@Override
	protected ROBPlusTreeLongKeysTupleValuesPage allocatePage() {
		if (this.rootNode == null)
			return new ROBPlusTreeLongKeysTupleValuesLeaf(this.nodeSize);
		
		return new ROBPlusTreeLongKeysTupleValuesNode(this.nodeSize);
	}

	@Override
	public void insert(Tuple tuple) {
		this.insert(this.getKeyL(tuple.columns), tuple);
	}
		
	public Boolean insert(long key, Tuple tuple) {
		// case 1 - room to insert in root node
		Pair<ROBPlusTreeLongKeysTupleValuesPage, Boolean> retval = this.rootNode.insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
				
		if (retval.getFirst() == null)
			return retval.getSecond();

		// case 2 we have grow the tree as the root node was split 
		ROBPlusTreeLongKeysTupleValuesNode newRoot = (ROBPlusTreeLongKeysTupleValuesNode)this.allocatePage();
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
		long key;
		if (keyColumns.length == 2) {
			long keyPart1 = ((EncodedType)keyColumns[0]).getKey();
			long keyPart2 = ((EncodedType)keyColumns[1]).getKey();
			key = (keyPart1 << 32) | (keyPart2 & 0xffffffffL);
		} else {
			key = ((BigEncodedType)keyColumns[0]).getKeyL();
		}
	
		this.rootNode.get(key, this.getResult);
		if (this.getResult.success)
			return this.getResult.tuples;
		return null;
	}	

	@Override
	public boolean delete(Tuple tuple) {
		return this.delete(this.getKeyL(tuple.columns));
	}
	
	public boolean delete(long key) {
		if (this.rootNode == null)
			return false;
		
		boolean status = this.rootNode.delete(key);
		if (status)
			this.numberOfEntries--;
		return status;
	}	
}
