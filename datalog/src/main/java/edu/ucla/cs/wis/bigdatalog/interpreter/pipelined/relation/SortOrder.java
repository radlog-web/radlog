package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation;

public enum SortOrder {
	ASC(0), DESC(1);
	private int id;
	
	private SortOrder(int id) { this.id = id; }
	
	public int getId() { return this.id; }
	
	public static SortOrder getSortOrder(String name) {
		if (name.toLowerCase().equals("asc")) return ASC;
		return DESC;
	}
}
