package edu.ucla.cs.wis.bigdatalog.database.store.dictionary;

import java.io.Serializable;

public class TrieNode implements Serializable {
	private static final long serialVersionUID = 1L;	
	public TrieNode[] children;
	public String value;
	public int id;

	public TrieNode(){}
	
	public TrieNode(String value, int id) {
		this.value = value;
		this.id = id;
		this.children = new TrieNode[0];
	}
	
	public void delete() {
		for (int i = 0; i < this.children.length; i++) {
			if (this.children[i] != null) {
				this.children[i].delete();
				this.children[i] = null;
			}
		}
		this.id = -1;
		this.value = null;
	}

	public String toString() {
		return toString(0);
	}
	
	public String toString(int depth) { 
		StringBuilder output = new StringBuilder();
		output.append("\n");
		String tabs = "";
		for (int i = 0; i < depth*4; i++)
			tabs += " ";
		output.append(tabs + "Value:'" + this.value + "', id:" + this.id + ", children:[");
		for (int i = 0; i < this.children.length; i++) {
			if (i > 0)
				output.append(", ");
			output.append(this.children[i].toString(depth+1));
		}

		output.append("]");
		return output.toString();
	}
		
}
