package edu.ucla.cs.wis.bigdatalog.database.store.type;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.store.dictionary.Dictionary;

// storage manager in charge of string objects
// strings are written and never updated
// strings are unique
// id returned to caller identifies string as follows:
public class StringStorageManager implements Serializable {
	private static final long serialVersionUID = 1L;
	private Dictionary dictionary;

	public StringStorageManager() {
		this.dictionary = new Dictionary();
	}
	
	public int write(String value) {
		return this.dictionary.put(value);
	}
	
	public String read(int key) {
		return new String(this.dictionary.get(key));
	}
	
	public void clear() {
		this.dictionary.clear();
	}
	
}
