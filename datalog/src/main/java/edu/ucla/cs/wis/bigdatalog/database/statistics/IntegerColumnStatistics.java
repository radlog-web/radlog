package edu.ucla.cs.wis.bigdatalog.database.statistics;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;

public class IntegerColumnStatistics extends ColumnStatistics<Integer> {
	
	protected double mean;
	protected int max;
	protected int min;
	
	public IntegerColumnStatistics(Tuple[] frequencyResults, Tuple statistics) {
		super(frequencyResults);

		this.mean = ((DbDouble)statistics.getColumn(0)).getValue();
		this.max = ((DbInteger)statistics.getColumn(1)).getValue();
		this.min = ((DbInteger)statistics.getColumn(2)).getValue();
	}

	public double getMean() { return this.mean; }
	
	public int getMax() { return this.max; }
	
	public int getMin() { return this.min; }
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(", Max: " + this.max);
		sb.append(", Min: " + this.min);
		sb.append(", Mean: " + this.mean);
		return sb.toString();
	}
}
