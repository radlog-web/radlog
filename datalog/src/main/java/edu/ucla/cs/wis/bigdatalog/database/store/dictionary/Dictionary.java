package edu.ucla.cs.wis.bigdatalog.database.store.dictionary;

import java.io.Serializable;

/* This dictionary supports a 1:1 mapping between Integer id and String value. 
 * put(String value) returns the integer id representing 'value'
 * get(Integer id) returns the string for 'id', if it exists, or null
 * we use array to provide 0(1) access to lookup value by id
 * we use specialized trie as the set to determine if value is new to dictionary 
 * */
public class Dictionary implements Serializable {
	private static final long serialVersionUID = 1L;
	private static int INITIAL_DIRECTORY_SIZE = 8;
	private int idCounter; 
	private String[] directory;
	private TrieNode trie;
	
	public Dictionary() {
		this.initialize();
	}
	
	private void initialize() {
		this.idCounter = 1;
		this.trie = new TrieNode("", 0);
		
		this.directory = new String[INITIAL_DIRECTORY_SIZE];
		this.directory[0] = "";
	}
	
	public int size() { return this.idCounter-1; }
	
	public int put(String value) {
		if ((value == null) || (value.length() == 0))
			return 0;
		
		int newId = this.idCounter;
		TrieNode node = this.trie;
		TrieNode child = null;
		int totalMatchLength = 0;
		int i = 0;
		int matchResult = 4;
		int matchLength = 0;
		String valueToMatch = "";
		
		while (value.length() > totalMatchLength) {
			boolean stop = true;
			valueToMatch = value.substring(totalMatchLength);
			// we search the children, which are ordered by least id to greatest id			
			for (i = 0; i < node.children.length; i++) {
				child = node.children[i];
				
				int diff = 0;		
				matchLength = 0;
				for (; matchLength < child.value.length() && matchLength < valueToMatch.length(); matchLength++) {
					diff = child.value.charAt(matchLength) - valueToMatch.charAt(matchLength);
					if (diff != 0)
						break;
				}
				
				if (diff == 0) {
					// if we have exact match, the trie contains 'value'
					if (valueToMatch.length() == child.value.length()) {
						if (child.id != -1)
							return child.id;
						matchResult = 0;
						break;
					}
					
					// if we have exhaustive match, we have nothing left from 'value' to search
					if (child.value.length() > valueToMatch.length()) {
						totalMatchLength += valueToMatch.length();
						matchResult = 1;
					} else {
						// otherwise, we have more 'value' to examine, so we proceed to the next level under 'child'
						node = child;
						totalMatchLength += child.value.length();
						stop = false;
					}
					
					break;
				} else if (diff > 0) {
					// value @ matchLength is less than child.value @ matchLength
					totalMatchLength += matchLength;
					matchResult = 2;
					break;
				} else if (diff < 0 && matchLength > 0) {
					// value @ matchLength is greater than child.value @ matchLength
					totalMatchLength += matchLength;
					matchResult = 3;
					break;
				}
			}

			if (stop)
				break;			
		}

		/*System.out.println("value: " + value);
		System.out.println("node: " + node.toString());
		if (child != null)
			System.out.println("child: " + child.toString());
		
		System.out.println("matchResult: " + matchResult + ", matchLength: " + matchLength + ", totalMatchLength: " + totalMatchLength);
		*/
		// Cases (excluding already found):
		// 1) node.value = 'cdef' and value = 'c' => node.value = 'c', node.children[0].value='def', node.children[0].id = node.id, node.id = newId
		// 2) node.value = 'abcdef' and value = 'abcdefg' => node.children[N].value = 'g' and node.children[N].id = newId,
		// 			where N is between A and Z children (e.g. 'abcdefa' already exists
		// 3) node.value = 'def' and value = 'dff' => grandParentNode.children[0] = {node.value='d', node.id=-1}, 
		//			node.children[0] = {node1.value = 'ef', node2.id=node.id}, node.children[1] = {node2.value = 'ff', node2.id = newId}
		// 4) node.value = 'abcdef' and value = 'abcdefg' => node.children[0].value = 'g' and node.children[0].id = newId (no previous children)
		
		switch (matchResult) { 
			case 0: {
				child.id = newId;
				break;
			}
			// case 1 => value being searched for wholy exists as substring in this child's value
			case 1: {
				// 1) create new internal node to hold 'value'
				TrieNode newInternalNode = new TrieNode(valueToMatch, newId);
				// 2) append child to new internal node and adjust child.value to reflect its new parent
				newInternalNode.children = new TrieNode[]{child};
				try{
					child.value = child.value.substring(valueToMatch.length());
				} catch (Exception ex) {
					System.out.println(ex);
				}
				// 3) replace child with new internal node in parent
				if (i >= node.children.length)
					System.out.println("should not happen");
				
				node.children[i] = newInternalNode;
				break;
			} 
			case 2: 
			case 3: {
				if (matchLength > 0) {
					// part of 'value' matched 'child.value'
					// 1) create new internal node with newValue and -1 for id
					TrieNode newInternalNode = new TrieNode(child.value.substring(0, matchLength), -1);
					// 2) adjust child.value to reflect being child of newInteralNode
					child.value = child.value.substring(matchLength);
					// 3) create new leaf for new value
					TrieNode newLeafNode = new TrieNode(value.substring(totalMatchLength), newId);

					if (matchResult == 2)
						newInternalNode.children = new TrieNode[]{newLeafNode, child};
					else
						newInternalNode.children = new TrieNode[]{child, newLeafNode};
						
					if (i >= node.children.length)
						System.out.println("should not happen");
					
					node.children[i] = newInternalNode;
					
					break;
				}
			}
			// case 3 => append new node to node.children at right most position
			case 4: {
				// 1) create new child node
				TrieNode newNode = new TrieNode(value.substring(totalMatchLength), newId);
				// 2) expand array by 1
				TrieNode[] newNodes = new TrieNode[node.children.length + 1];
				
				// 3) copy existing children to new array
				if (i > 0)
					System.arraycopy(node.children, 0, newNodes, 0, i);
				
				// 4) add new child
				newNodes[i] = newNode;
				
				// 5) copy existing children to new array 
				if ((node.children.length - i) > 0)
					System.arraycopy(node.children, i, newNodes, i + 1, node.children.length - i);

				// 6) replace children in node
				node.children = newNodes;
				break;
			}
		}
		
		addToDictionary(value, newId);

		return newId;
	}
	
	private void addToDictionary(String value, int id) {
		if (id > (this.directory.length - 1)) {
			String[] newDirectory = new String[(this.directory.length * 5) / 4];
			System.arraycopy(this.directory, 0, newDirectory, 0, this.directory.length);
			this.directory = newDirectory;
		}
		this.idCounter++;
		this.directory[id] = value;
	}
	
	public String get(int id) {
		if (id >= this.idCounter)
			return null;
		//System.out.println(id);
		return this.directory[id];
	}

	public void clear() {
		//this.trie.deleteAll();
		//this.map.clear();
		this.trie.delete();		
		this.initialize();
	}
	
	public String toString() { 
		StringBuilder output = new StringBuilder();
		output.append("Trie:");
		output.append(this.trie.toString());
		output.append("\n");
		output.append("Dictionary [" + this.size() + " entries]:");
		output.append("\n");
		for (int i = 0; i < this.directory.length; i++) {
			if (this.directory[i] == null)
				break;
			if (i > 0)
				output.append(", ");
			output.append(i + ":'" + this.directory[i]+"'");			
		}
		return output.toString();
	}
	
	public void verify() {
		for (int i = 0; i < this.idCounter; i++) {
			boolean wordExists = (this.put(this.directory[i]) == i);
			assert wordExists;
		}
	}
}
