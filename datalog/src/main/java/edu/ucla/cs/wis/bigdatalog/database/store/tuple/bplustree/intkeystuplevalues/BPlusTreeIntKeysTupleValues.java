package edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.intkeystuplevalues;

import java.io.Serializable;
import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.BPlusTreeTupleValuesGetResult;
import edu.ucla.cs.wis.bigdatalog.database.store.tuple.bplustree.TupleBPlusTreeTupleStoreStructure;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.EncodedType;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

// this version of the b+tree holds tuples indexed by integer key

public class BPlusTreeIntKeysTupleValues 
	extends BPlusTreeStoreStructure<BPlusTreeIntKeysTupleValuesPage, BPlusTreeIntKeysTupleValuesLeaf, BPlusTreeIntKeysTupleValuesNode>
	implements TupleBPlusTreeTupleStoreStructure, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected boolean isIntegerKey;
	protected BPlusTreeTupleValuesGetResult getResult;
	
	public BPlusTreeIntKeysTupleValues() { super(); }
	
	public BPlusTreeIntKeysTupleValues(int nodeSize, int[] keyColumns, DataType[] keyColumnTypes) {
		super(nodeSize, 4, keyColumns, keyColumnTypes);
		this.isIntegerKey = (keyColumnTypes[0] == DataType.INT);
		this.getResult = new BPlusTreeTupleValuesGetResult();
			
		this.initialize();
	}

	@Override
	protected BPlusTreeIntKeysTupleValuesPage allocatePage() {
		if (this.rootNode == null)
			return new BPlusTreeIntKeysTupleValuesLeaf(this.nodeSize);
		
		return new BPlusTreeIntKeysTupleValuesNode(this.nodeSize);
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
		Pair<BPlusTreeIntKeysTupleValuesPage, Boolean> retval = this.rootNode.insert(key, tuple);
		if (retval.getSecond())
			this.numberOfEntries++;
				
		if (retval.getFirst() == null)
			return retval.getSecond();

		// case 2 we have grow the tree as the root node was split 
		BPlusTreeIntKeysTupleValuesNode newRoot = (BPlusTreeIntKeysTupleValuesNode)this.allocatePage();
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
		if (this.isIntegerKey)
			this.rootNode.get(((DbInteger)keyColumns[0]).getValue(), this.getResult);
		else
			this.rootNode.get(((EncodedType)keyColumns[0]).getKey(), this.getResult);
		
		if (this.getResult.status)
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
