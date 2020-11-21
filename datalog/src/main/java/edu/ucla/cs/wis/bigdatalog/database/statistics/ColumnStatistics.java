package edu.ucla.cs.wis.bigdatalog.database.statistics;

import java.util.HashMap;
import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.type.DbDouble;
import edu.ucla.cs.wis.bigdatalog.database.type.DbFloat;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbLong;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;

abstract public class ColumnStatistics<T extends Comparable<?>> {
	protected HashMap<T, DbNumericType> frequencies;
	protected T mode;
	protected int numberOfDistinctValues;
	
	public ColumnStatistics(Tuple[] frequencyResults) {
		loadMap(frequencyResults);
	}

	public T getMode() { return this.mode; }
	
	public HashMap<T, DbNumericType> getFrequencies() { return this.frequencies; }
	
	public int getNumberOfDistinctValues() { return this.numberOfDistinctValues; }

	@SuppressWarnings("unchecked")
	public void loadMap(Tuple[] frequencyResults) {
		this.numberOfDistinctValues = 0;
		this.frequencies = new HashMap<>();
		
		T value = null;

		// always tuple of arity = 2
		for (Tuple frequency : frequencyResults) {			
			if (this instanceof IntegerColumnStatistics)
				value = (T) Integer.valueOf(((DbInteger)frequency.getColumn(0)).getValue());
			else if (this instanceof DoubleColumnStatistics)
				value = (T) Double.valueOf(((DbDouble)frequency.getColumn(0)).getValue());
			else if (this instanceof FloatColumnStatistics)
				value = (T) Double.valueOf(((DbFloat)frequency.getColumn(0)).getValue());
			else if (this instanceof LongColumnStatistics)
				value = (T) Double.valueOf(((DbLong)frequency.getColumn(0)).getValue());
			else if (this instanceof StringColumnStatistics)
				value = (T) ((DbString)frequency.getColumn(0)).getValue();
			
			this.frequencies.put(value, (DbNumericType) frequency.getColumn(1));

			this.numberOfDistinctValues++;
		}
	}
	
	public void calculateFrequencyStatistics() {
						
		//this.numberOfDistinctValues = this.frequencies.size();
		
		/*
		this.mode =				this.frequencies.size() / 2
		
		
		for (Entry<T, Integer> count : counts) {		
			if (count.getValue() > currentModeHigh) {
				this.mode = count.getKey();
				currentModeHigh = count.getValue();
			}
		}

		this.mean = meanSum/meanCount;
		*/
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(", Mode: " + this.mode);
		sb.append(", # distinct values: " + this.numberOfDistinctValues);
		return sb.toString();
	}
}
