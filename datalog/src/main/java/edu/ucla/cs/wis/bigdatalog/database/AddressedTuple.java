package edu.ucla.cs.wis.bigdatalog.database;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

public class AddressedTuple extends Tuple implements Serializable {
	private static final long serialVersionUID = 1L;
	
	public int 	address;
	public byte isDeleted;
	
	public AddressedTuple() { super(); }
	
	public AddressedTuple(int arity) {
		super(arity);
		this.isDeleted = 0;
		this.address = -1;
	}

	public AddressedTuple(DbTypeBase[] columnValues) {
		super(columnValues);
		this.isDeleted = 0;
		this.address = -1;
	}
	
	public boolean isDeleted() { return this.isDeleted == 1; }
	
	public void setDeleted() { this.isDeleted = 1; }

	public AddressedTuple copy() {
		AddressedTuple tuple = new AddressedTuple(this.columns.length);
		for (int i = 0; i < this.columns.length; i++)
			tuple.columns[i] = this.columns[i].copy();
		
		tuple.isDeleted = this.isDeleted;
		tuple.address = this.address;	
		
		return tuple;
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(super.toString());
		
		if (this.isDeleted()) 
			output.append(" DELETED");

		return output.toString();
	}
		
	public String toString(boolean showAddress) {
		StringBuilder output = new StringBuilder();
		output.append(this.toString());
		if (showAddress)
			output.append(" {address:" + this.address + "}");
		return output.toString();
	}
	
	public String toJson() {
		return this.toJson(false);
	}
	
	public String toJson(boolean showDeleted) {
		StringBuilder output = new StringBuilder();
		
		if (this.columns != null) {
			output.append("[");
			for (int i = 0; i < this.columns.length; i++) {
				if (i > 0)
					output.append(",");

				output.append("\"");
				if (this.columns[i] != null) {
					output.append(this.columns[i].toString());
				} else {
					output.append("NULL");
				}
				output.append("\"");
			}
			
			if (showDeleted) {
				output.append(",\"DELETED\":");
				if (this.isDeleted()) {
					output.append("true");
				} else {
					output.append("false");
				}
			}
			output.append("]");
		}		
	
		return output.toString();
	}

}
