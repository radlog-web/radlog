package edu.ucla.cs.wis.bigdatalog.compiler.aggregate;

public enum FSAggregateType {
	FSMAX(21),
	FSCNT(22),
	FSMIN(23),
	FSSUM(24),
	FSMANY(25),
	NONE(26);
	
	private int id;
	
	private FSAggregateType(int id) { this.id = id;}
	
	public int getId() {return this.id;}
	
	public static FSAggregateType getFSAggregateType(String relationName) {
		for (FSAggregateType fsAggregateType : FSAggregateType.values()) 
			if (relationName.toLowerCase().startsWith(fsAggregateType.name().toLowerCase()))
				return fsAggregateType;

		return null;
	}
	
	public static boolean isFSAggregateType(String relationName) {
		return (getFSAggregateType(relationName) != null);
	}
}