package edu.ucla.cs.wis.bigdatalog.database.statistics;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;

public class StringColumnStatistics extends ColumnStatistics<String> {
	
	protected int max;
	protected int min;
	
	public StringColumnStatistics(Tuple[] frequencyResults, Tuple statistics) {
		super(frequencyResults);
		
		this.max = ((DbInteger)statistics.getColumn(0)).getValue();
		this.min = ((DbInteger)statistics.getColumn(1)).getValue();
	}
	
	public int getMax() { return this.max; }
	
	public int getMin() { return this.min; }
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(", Max: " + this.max);
		sb.append(", Min: " + this.min);
		return sb.toString();
	}
}
