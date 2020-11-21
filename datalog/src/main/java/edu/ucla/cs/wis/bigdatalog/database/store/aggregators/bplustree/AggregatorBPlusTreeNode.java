package edu.ucla.cs.wis.bigdatalog.database.store.aggregators.bplustree;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeElement;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeLeaf;
import edu.ucla.cs.wis.bigdatalog.database.store.bplustree.BPlusTreeNode;
import edu.ucla.cs.wis.bigdatalog.interpreter.AggregateFunctionType;

public abstract class AggregatorBPlusTreeNode<P extends BPlusTreeElement, L extends BPlusTreeLeaf<L>> 
	extends BPlusTreeNode<P, L> implements Serializable {
	private static final long serialVersionUID = 1L;

	protected AggregateFunctionType aggregateType;
	
	public AggregatorBPlusTreeNode() { super(); }
	
	public AggregatorBPlusTreeNode(int nodeSize, int bytesPerKey, AggregateFunctionType aggregateType) {
		super(nodeSize, bytesPerKey);
		this.aggregateType = aggregateType;
	}

}
