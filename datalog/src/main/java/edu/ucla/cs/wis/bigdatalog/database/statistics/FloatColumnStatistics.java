package edu.ucla.cs.wis.bigdatalog.database.statistics;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;

public class FloatColumnStatistics extends ColumnStatistics<Float> {
	
	protected double mean;
	protected float max;
	protected float min;
	
	public FloatColumnStatistics(Tuple[] frequencyResults, Tuple statistics) {
		super(frequencyResults);

		this.mean = ((DbDouble)statistics.getColumn(0)).getValue();
		this.max = ((DbFloat)statistics.getColumn(1)).getValue();
		this.min = ((DbFloat)statistics.getColumn(2)).getValue();
	}
	
	public double getMean() { return this.mean; }
	
	public float getMax() { return this.max; }
	
	public float getMin() { return this.min; }
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(", Max: " + this.max);
		sb.append(", Min: " + this.min);
		sb.append(", Mean: " + this.mean);
		return sb.toString();
	}
}
