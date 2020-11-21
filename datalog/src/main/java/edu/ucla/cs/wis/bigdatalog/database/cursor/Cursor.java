package edu.ucla.cs.wis.bigdatalog.database.cursor;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;

abstract public class Cursor<T extends Tuple> {	
	public final boolean DEBUG = false;

	protected Relation<T> relation;

	protected Cursor(Relation<T> relation)  {
		this.relation = relation;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("Cursor " + this.hashCode() + " for relation " + this.relation.getName());
		return output.toString();
	}
	
	public Relation<T> getRelation() { return this.relation; }
	
	public void reset(DbTypeBase[] keyColumns){ this.reset(); }
	
	public void resetCounters() {
		/*this.timesCalled = new int[this.numberOfCounters];
		this.timeSpent = new long[this.numberOfCounters];
		this.timesNoResults = new int[this.numberOfCounters];	*/	
	}
	
	public String[] getFunctions() { return new String[]{"reset", "getTuple", "match", "refresh", "beginnextphase", "clear"}; }
	
	public T getEmptyTuple() { return this.getRelation().getEmptyTuple(); }
	
	abstract public int getTuple(T tuple);
	
	abstract public void moveNext();
	
	abstract public void reset();
	
}
