package edu.ucla.cs.wis.bigdatalog.system;

public enum ReturnStatus {
	ERROR(-1), 
	FAIL(0), 
	SUCCESS(1);
	
	private int id;
	
	private ReturnStatus(int id) {
		this.id = id;
	}
	
	public int getId() {return this.id;}
}
