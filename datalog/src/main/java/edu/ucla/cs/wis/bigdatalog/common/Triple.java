package edu.ucla.cs.wis.bigdatalog.common;

public class Triple<S, T, U> {
	public S first;
	public T second;
	public U third;
	
	public Triple(){}
	
	public Triple(S s, T t, U u) {
		this.first = s;
		this.second = t;
		this.third = u;
	}
	
	public S getFirst() {
		return first;
	}
	
	public T getSecond() {
		return second;
	}
	
	public U getThird() {
		return third;
	}
	
	public String toString() {
		return "Triple:[" + first + ", " + second + ", " + third + "]";
	}
}
