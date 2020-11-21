package edu.ucla.cs.wis.bigdatalog.measurement;

import java.text.DecimalFormat;

public class MemoryMeasurement {
	private int used;
	private int allocated;
	
	public MemoryMeasurement() {
		this.used = 0;
		this.allocated = 0;
	}
	
	public MemoryMeasurement(int used, int allocated) {
		this.used = used;
		this.allocated = allocated;
	}
	
	public int getUsed() { return this.used; }
	
	public int getAllocated() { return this.allocated; }
	
	public double getUtilizationPercentage() {
		if (this.used == 0) return 0.0;
		if (this.allocated == 0) return Double.NaN;
		
		return (double)this.used / (double)this.allocated; 
	}
	
	public String toString() {
		int kb = 1024;
		int mb = 1024*1024;		
		DecimalFormat df = new DecimalFormat("#.##");
		StringBuilder output = new StringBuilder();
		if (this.used < kb || this.allocated < kb) {
			output.append("Used/Allocated => " + this.used + "/" + this.allocated);
		} else if (this.used < mb || this.allocated < mb){
			output.append("Used/Allocated => " + df.format((double)this.used / kb) + "kb/" + df.format((double)this.allocated / kb) + "kb");
		} else {
			output.append("Used/Allocated => " + df.format((double)this.used / mb) + "mb/" + df.format((double)this.allocated / mb) + "mb");
		}
				
		output.append("[" + df.format(this.getUtilizationPercentage()) + "]");
		return output.toString();
	}
}
