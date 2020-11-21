package edu.ucla.cs.wis.bigdatalog.database.store.queue;

import java.io.Serializable;
import java.util.Comparator;
import java.util.PriorityQueue;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;

public class FixedSizePriorityQueue extends PriorityQueue<Tuple> implements Serializable {
	private static final long serialVersionUID = 1L;

	protected int capacity;
	protected Comparator<Tuple> comparator;
	
	public FixedSizePriorityQueue() {}
	
	public FixedSizePriorityQueue(int capacity, Comparator<Tuple> comparator) {
		super(capacity, comparator);
		this.capacity = capacity;
		this.comparator = comparator;
	}
	
	@Override
	public boolean add(Tuple tuple) {
		// first one in wins on tie strategy
		if (super.size() == this.capacity) {
			if (this.comparator.compare(tuple, this.peek()) == 0)
				return false;
		}
		
		boolean result = super.add(tuple);
		
		// remove head to maintain max queue size
		if (result && super.size() > this.capacity)
			this.poll();
		return result;
	}
}
