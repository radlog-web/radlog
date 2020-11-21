package edu.ucla.cs.wis.bigdatalog.database.statistics;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;

public class LongColumnStatistics extends ColumnStatistics<Long> {
	
	protected double mean;
	protected long max;
	protected long min;
	
	public LongColumnStatistics(Tuple[] frequencyResults, Tuple statistics) {
		super(frequencyResults);

		this.mean = ((DbDouble)statistics.getColumn(0)).getValue();
		this.max = ((DbLong)statistics.getColumn(1)).getValue();
		this.min = ((DbLong)statistics.getColumn(2)).getValue();
	}

	public double getMean() { return this.mean; }
	
	public long getMax() { return this.max; }
	
	public long getMin() { return this.min; }
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(", Max: " + this.max);
		sb.append(", Min: " + this.min);
		sb.append(", Mean: " + this.mean);
		return sb.toString();
	}
}
