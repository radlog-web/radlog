package edu.ucla.cs.wis.bigdatalog.database.cursor;

import java.util.Iterator;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;

public class IteratorWrapperCursor extends Cursor<Tuple> {

	protected Iterable<?> queue;
	protected Iterator<?> iterator;
	
	public IteratorWrapperCursor(Relation<Tuple> relation, Iterable<?> queue) {
		super(relation);
		this.queue = queue;
		this.initialize();
	}

	public void initialize() {
		this.iterator = this.queue.iterator();
	}
	
	@Override
	public int getTuple(Tuple tuple) {
		if (this.iterator.hasNext()) {
			Tuple temp = (Tuple) this.iterator.next();
			for (int i = 0; i < temp.getArity(); i++)
				tuple.columns[i] = temp.columns[i];
			return 1;
		}
		return 0;
	}
	
	@Override
	public void moveNext() {
		this.iterator.next();
	}
	
	@Override
	public void reset() {
		this.initialize();
	}
}
