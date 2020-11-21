package edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting;

import java.util.Arrays;

public class Equation {
	public int[] left;
	public int[] right;
	public String rightPredicateName; // all left are for the head predicate name
	
	public Equation(int[] left, int[] right, String rightPredicateName) {
		this.left = left;
		this.right = right;
		this.rightPredicateName = rightPredicateName;
	}
	
	public String toString() {
		return Arrays.toString(this.left) + " = " + this.rightPredicateName + Arrays.toString(this.right);
	}
}

