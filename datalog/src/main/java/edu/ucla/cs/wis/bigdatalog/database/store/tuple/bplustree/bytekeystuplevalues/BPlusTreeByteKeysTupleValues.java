package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.bytekeystuplevalues;

import java.io.Serializable;
import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeTupleStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

//this version of the b+tree holds tuples indexed by byte keys

public class BPlusTreeByteKeysTupleValues 
	extends BPlusTreeStoreStructure<BPlusTreeByteKeysTupleValuesPage, BPlusTreeByteKeysTupleValuesLeaf, BPlusTreeByteKeysTupleValuesNode>
	implements TupleBPlusTreeTupleStoreStructure, Serializable {
	private static final long serialVersionUID = 1L;
		
	protected BPlusTreeTupleValuesGetResult getResult;

	public BPlusTreeByteKeysTupleValues() { super(); }
	
	public BPlusTreeByteKeysTupleValues(int nodeSize, int bytesPerKey, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, bytesPerKey, keyColumns, keyColumnTypes);
		this.getResult = new BPlusTreeTupleValuesGetResult();

		this.initialize();
	}

	@Override
	protected BPlusTreeByteKeysTupleValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeByteKeysTupleValuesLeaf(this.nodeSize, this.bytesPerKey);
		
		return new BPlusTreeByteKeysTupleValuesNode(this.nodeSize, this.bytesPerKey);
	}

	@Override
	public void insert(Tuple tuple) {
		this.insert(this.getKey(tuple.columns), tuple);
	}
		
	public Boolean insert(byte[] key, Tuple tuple) {
		// case 1 - room to insert in root node
		Pair<BPlusTreeByteKeysTupleValuesPage, Boolean> retval = this.rootNode.insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
				
		if (retval.getFirst() == null)
			return retval.getSecond();

		// case 2 we have grow the tree as the root node was split 
		BPlusTreeByteKeysTupleValuesNode newRoot = (BPlusTreeByteKeysTupleValuesNode)this.allocatePage();
		newRoot.children[0] = this.rootNode;
		newRoot.children[1] = retval.getFirst();
				
		byte[] newLeftKey = newRoot.children[1].getLeftMostLeafKey();
		System.arraycopy(newLeftKey, 0, newRoot.keys, 0, this.bytesPerKey);
				
		newRoot.highWaterMark += 2;
		this.rootNode = newRoot;
		return retval.getSecond();
	}
	
	@Override
	public ArrayList<Tuple> get(DbTypeBase[] keyColumns) {
		this.rootNode.get(this.getKey(keyColumns), this.getResult);		
		if (this.getResult.status)
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
