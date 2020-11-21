package edu.ucla.cs.wis.bigdatalog.api;

import java.util.List;

import edu.ucla.cs.wis.bigdatalog.interpreter.EvaluationType;

public class RecursionInformation {
	public String name;
	public EvaluationType evaluationType;
	public int iterations;
	public List<Integer> numberOfDeltaFactsByIteration;
	public List<Integer> numberOfGeneratedFactsByIteration;
	
	public RecursionInformation(String name, EvaluationType evaluationType, int iterations, 
			List<Integer> numberOfDeltaFactsByIteration, List<Integer> numberOfGeneratedFactsByIteration) {
		this.name = name;
		this.evaluationType = evaluationType;
		this.iterations = iterations;
		this.numberOfDeltaFactsByIteration = numberOfDeltaFactsByIteration;
		this.numberOfGeneratedFactsByIteration = numberOfGeneratedFactsByIteration;
	}
	
	public long getTotalDeltaFactCount() {
		long counter = 0;
		for (Integer facts : this.numberOfDeltaFactsByIteration)
			counter += facts;
		return counter;
	}
	
	public long getTotalGeneratedFactCount() {
		long counter = 0;
		for (Integer facts : this.numberOfGeneratedFactsByIteration)
			counter += facts;
		return counter;
	}
}
