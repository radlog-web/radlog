package edu.ucla.cs.wis.bigdatalog.common;

public class Pair<S,T> {
	public S first;
	public T second;
	
	public Pair(){}
	
	public Pair(S s, T t) {
		this.first = s;
		this.second = t;
	}
	
	public S getFirst() {
		return first;
	}
	
	public T getSecond() {
		return second;
	}
	
	public String toString() {
		return "Pair:[" + first + ", " + second + "]";
	}
}
