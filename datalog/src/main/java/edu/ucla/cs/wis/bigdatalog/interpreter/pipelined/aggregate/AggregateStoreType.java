package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.aggregate;

public enum AggregateStoreType {
	Heap("heap"), 
	BPlusTree("bplustree"), 
	Aggregator("aggregator");
	
	private String name;
	private AggregateStoreType(String name) {
		this.name = name;
	}
	
	public String getName() { return this.name; }
	
	public static AggregateStoreType getAggregateStoreType(String name) {
		for (AggregateStoreType aggregateStoreType : AggregateStoreType.values()) {
			if (aggregateStoreType.name.equals(name))
				return aggregateStoreType;
		}
		return null;
	}
}
