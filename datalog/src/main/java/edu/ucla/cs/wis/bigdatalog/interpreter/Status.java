package edu.ucla.cs.wis.bigdatalog.interpreter;

public enum Status {
	ENTRY_FAIL(-1, "ENTRY_FAIL"), 
	FAIL(0, "FAIL"), 
	BACKTRACK_SUCCESS(1, "BACKTRACK_SUCCESS"), 
	SUCCESS(2, "SUCCESS");
	
	private int id;
	private String name;
	
	private Status(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public int getId() { return this.id; }
	public String getName() { return this.name; }
}
