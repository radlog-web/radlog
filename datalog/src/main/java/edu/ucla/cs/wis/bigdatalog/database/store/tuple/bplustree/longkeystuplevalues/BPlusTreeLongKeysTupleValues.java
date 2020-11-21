package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.longkeystuplevalues;

import java.io.Serializable;
import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeTupleStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.BigEncodedType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//this version of the b+tree holds tuples indexed by long key

public class BPlusTreeLongKeysTupleValues 
	extends BPlusTreeStoreStructure<BPlusTreeLongKeysTupleValuesPage, BPlusTreeLongKeysTupleValuesLeaf, BPlusTreeLongKeysTupleValuesNode>
	implements TupleBPlusTreeTupleStoreStructure, Serializable {
	private static final long serialVersionUID = 1L;
		
	protected BPlusTreeTupleValuesGetResult getResult;

	public BPlusTreeLongKeysTupleValues() { super(); }
	
	public BPlusTreeLongKeysTupleValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, 8, keyColumns, keyColumnTypes);
		this.getResult = new BPlusTreeTupleValuesGetResult();

		this.initialize();
	}

	@Override
	protected BPlusTreeLongKeysTupleValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeLongKeysTupleValuesLeaf(this.nodeSize);
		
		return new BPlusTreeLongKeysTupleValuesNode(this.nodeSize);
	}

	@Override
	public void insert(Tuple tuple) {
		this.insert(this.getKeyL(tuple.columns), tuple);
	}
		
	public Boolean insert(long key, Tuple tuple) {
		// case 1 - room to insert in root node
		Pair<BPlusTreeLongKeysTupleValuesPage, Boolean> retval = this.rootNode.insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
				
		if (retval.getFirst() == null)
			return retval.getSecond();

		// case 2 we have grow the tree as the root node was split 
		BPlusTreeLongKeysTupleValuesNode newRoot = (BPlusTreeLongKeysTupleValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = retval.getFirst();
		newRoot.keys[0] = newRoot.children[1].getLeftMostLeafKey();
				
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
		return retval.getSecond();
	}
	
	@Override
	public ArrayList<Tuple> get(DbTypeBase[] keyColumns) {
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
		if (this.getResult.status)
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
