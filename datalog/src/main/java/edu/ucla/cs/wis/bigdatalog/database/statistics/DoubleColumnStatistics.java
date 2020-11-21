package edu.ucla.cs.wis.bigdatalog.database.statistics;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;

public class DoubleColumnStatistics extends ColumnStatistics<Double> {
	
	protected double mean;
	protected double max;
	protected double min;
	
	public DoubleColumnStatistics(Tuple[] frequencyResults, Tuple statistics) {
		super(frequencyResults);

		this.mean = ((DbDouble)statistics.getColumn(0)).getValue();
		this.max = ((DbDouble)statistics.getColumn(1)).getValue();
		this.min = ((DbDouble)statistics.getColumn(2)).getValue();
	}
	
	public double getMean() { return this.mean; }
	
	public double getMax() { return this.max; }
	
	public double getMin() { return this.min; }
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(", Max: " + this.max);
		sb.append(", Min: " + this.min);
		sb.append(", Mean: " + this.mean);
		return sb.toString();
	}
}
