package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

// basic stack implementation
public class NodeStack<T> extends NodeList<T> {
	
	private static final long serialVersionUID = 1L;

	public T pop() {
		if (this.isEmpty())
			return null;
		
		return this.remove(this.size() - 1);
	}

	public void push(T node) {
		this.add(node);
	}
	
	public T peek() {
		if (this.isEmpty())
			return null;
		
		return this.get(this.size() - 1);
	}
}
