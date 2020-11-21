package edu.ucla.cs.wis.bigdatalog.partitioning;

import java.io.Serializable;
import java.util.Arrays;

public class PartitioningStrategy implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected PartitioningType partitioningType;
	protected String name;
	protected int[] columns;
	
	public PartitioningStrategy(String name, int[] columns) {
		//this.partitioningType = partitioningType;
		this.name = name;
		this.columns = columns;
	}
	
	public PartitioningType getPartitioningType() { return this.partitioningType; }

	public String getName() { return this.name; }
	
	public int[] getColumns() { return this.columns; }
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.name + " " + this.partitioningType.name());
		if (this.columns != null && this.columns.length > 0)
			output.append(" on " + Arrays.toString(this.columns));
		return output.toString();
	}
}
