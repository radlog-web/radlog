package edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeneralizedPivotSet implements Serializable {
	private static final long serialVersionUID = 1L;

	public class GeneralizedPivot implements Serializable {
		private static final long serialVersionUID = 1L;

		public String predicateName;
		public int[] positions;
		
		public GeneralizedPivot(String predicateName, int[] positions) {
			this.predicateName = predicateName;
			this.positions = positions;
		}
		
		public String toString() {
			return this.predicateName + "(" + Arrays.toString(this.positions) + ")";
		}
	}
	
	public List<GeneralizedPivot> set;
	
	public GeneralizedPivotSet() {
		this.set = new ArrayList<>();
	}
	
	public int size() { return this.set.size(); }
	
	public boolean isEmpty() { return this.set.isEmpty(); }
	
	public void add(String predicateName, int[] positions) {
		for (GeneralizedPivot gp : this.set)
			if (gp.predicateName.equals(predicateName))
				return;
		
		int i;
		for (i = 0; i < positions.length; i++)
			if (positions[i] > 0)
				break;
		// if we iterated thru entire array, we did not find a nonzero coefficient
		if (i < positions.length)
			this.set.add(new GeneralizedPivot(predicateName, positions));		
	}
	
	public int[] get(String predicateName) {
		for (GeneralizedPivot gp : this.set)
			if (gp.predicateName.equals(predicateName))
				return gp.positions;
		
		return null;
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
		for (GeneralizedPivot gp : this.set) {
			if (output.length() > 0)
				output.append("\n");
			output.append(gp.toString());
		}
		return output.toString();
	}
}
