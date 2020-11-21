package edu.ucla.cs.wis.bigdatalog.common;

public class Quad<S, T, U, V> {
	public S first;
	public T second;
	public U third;
	public V fourth;
	
	public Quad(){}
	
	public Quad(S s, T t, U u, V v) {
		this.first = s;
		this.second = t;
		this.third = u;
		this.fourth = v;
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
	
	public V getFourth() {
		return fourth;
	}
	
	public String toString() {
		return "Quad:[" + first + ", " + second + " ," + third + " , " + fourth + "]";
	}
}
