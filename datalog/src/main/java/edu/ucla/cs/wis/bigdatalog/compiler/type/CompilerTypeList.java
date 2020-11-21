package edu.ucla.cs.wis.bigdatalog.compiler.type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.ucla.cs.wis.bigdatalog.compiler.Utilities;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class CompilerTypeList extends CompilerTypeBase implements Iterable<CompilerTypeBase>, Serializable {
	private static final long serialVersionUID = 1L;
	private List<CompilerTypeBase> innerList;

	public CompilerTypeList() {
		super(CompilerType.COMPILER_TYPE_LIST);
		this.innerList = new ArrayList<>();
	}

	public CompilerTypeList(int size) {
		super(CompilerType.COMPILER_TYPE_LIST);
		this.innerList = new ArrayList<>(size);
	}
	
	public int size() { return this.innerList.size(); }

	public boolean contains(CompilerTypeBase compilerTypeObject) {
		return this.innerList.contains(compilerTypeObject);
	}

	public int getPosition(CompilerTypeBase compilerTypeObject) {
		int index = -1;
		for (int i = 0; i < this.innerList.size(); i++) {
			if (this.innerList.get(i) == compilerTypeObject) {
				index = i;
				break;
			}
		}
		return index;
	}

	public CompilerTypeBase get(int position) {
		return this.innerList.get(position);
	}

	public void add(int position, CompilerTypeBase compilerTypeObject) {
		this.innerList.add(position, compilerTypeObject);
	}

	public void set(int position, CompilerTypeBase compilerTypeObject) {
		this.innerList.set(position, compilerTypeObject);
	}

	public void add(CompilerTypeBase compilerTypeObject) {
		this.innerList.add(compilerTypeObject);
	}

	public CompilerTypeBase getFirst() { return this.innerList.get(0); }

	public CompilerTypeBase getLast() { return this.innerList.get(this.innerList.size()-1); }

	public void push(CompilerTypeBase compilerTypeObject) {
		this.innerList.add(compilerTypeObject);
	}

	public CompilerTypeBase pop() {
		return this.innerList.remove(this.innerList.size()-1);
	}

	public boolean remove(CompilerTypeBase compilerTypeObject) {
		return this.innerList.remove(compilerTypeObject);
	}

	public CompilerTypeBase remove(int position) {
		return this.innerList.remove(position);
	}

	public void addUnique(CompilerTypeBase compilerTypeObject) {
		if (!this.innerList.contains(compilerTypeObject))
			this.innerList.add(compilerTypeObject);
	}

	public void appendList(CompilerTypeList objectList) {
		//for (CompilerTypeBase compilerTypeObject : objectList)
		//	this.innerList.add(compilerTypeObject);
		this.innerList.addAll(objectList.innerList);
	}

	public CompilerTypeList copyList() {
		// Copy a list that points to the same set of elements
		CompilerTypeList objectList = new CompilerTypeList(this.size());

		for (CompilerTypeBase compilerTypeObject : this.innerList)
			objectList.add(compilerTypeObject);

		return objectList;
	}

	public CompilerTypeList copy() {
		// Copy a list that points to the copies of the elements
		CompilerTypeList objectList = new CompilerTypeList(this.size());

		for (CompilerTypeBase compilerTypeObject : this.innerList)
			objectList.add(compilerTypeObject.copy());

		return objectList;
	}

	public void clearExceptVariables() {
		for (int i = (this.innerList.size() - 1); i >= 0 ;i--) {
			if (this.innerList.get(i) != null &&
					!this.innerList.get(i).isVariable()) {
				this.innerList.remove(i);
			}
		}
	}

	public CompilerTypeList copyExceptVariables() {
		CompilerTypeBase copiedObject;
		CompilerTypeList objectList = new CompilerTypeList(this.size());

		for (CompilerTypeBase compilerTypeObject : this.innerList)
			if ((copiedObject = Utilities.copyExceptVariables(compilerTypeObject)) != null)
				objectList.add(copiedObject);
		
		return objectList;
	}

	public CompilerTypeList copy(CompilerVariableList variableList) {
		return copy(0, variableList);
	}
	
	public CompilerTypeList copy(int start, CompilerVariableList variableList) {
		CompilerTypeList objectList = new CompilerTypeList(this.size());

		// Storing NULL value as element of list is ok
		for (int i = start; i < this.innerList.size(); i++) {
			if (this.innerList.get(i) == null) {
				objectList.add(null);
			} else {
				objectList.add(this.innerList.get(i).copy(variableList));
			}				
		}

		return objectList;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		int count = 0;
		for (CompilerTypeBase compilerTypeObject : this.innerList) {
			output.append(count + " ");
			output.append(compilerTypeObject.toString());
			output.append("\n");
			count++;
		}
		return output.toString();
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerTypeList))
			return false;

		CompilerTypeList otherList = (CompilerTypeList)other;
		
		if (this.size() != otherList.size())
			return false;
		
		CompilerTypeBase compilerTypeObject;
		for (int i = 0; i < this.innerList.size(); i++) {
			compilerTypeObject = otherList.get(i);
			
			if (this.get(i) == null 
					|| compilerTypeObject == null)
				return false;
			
			if (!this.get(i).equals(compilerTypeObject))
				return false;
		}
	
		return true;
	}

	public void clear() {
		this.innerList.clear();
	}

	public boolean isEmpty() { return this.innerList.isEmpty(); }

	@Override
	public Iterator<CompilerTypeBase> iterator() {
		return this.innerList.iterator();
	}
	
	public CompilerVariableList toCompilerVariableList() {
		CompilerVariableList variableList = new CompilerVariableList();
		Utilities.getVariables(this, variableList);
		return variableList;
	}
	
	public boolean hasDuplicates() {
		Set<CompilerTypeBase> set = new HashSet<>();
		
		CompilerTypeBase obj;
		Iterator<CompilerTypeBase> iterator = this.iterator();
		while (iterator.hasNext()) {
			obj = iterator.next();
			if (set.contains(obj))
				return true;
			set.add(obj);
		}
		return false;
	}
	
	public static CompilerTypeList create(CompilerTypeBase arg1) {
		CompilerTypeList ctl = new CompilerTypeList();
		ctl.add(arg1);
		return ctl;
	}
	
	public static CompilerTypeList create(CompilerTypeBase arg1, CompilerTypeBase arg2) {
		CompilerTypeList ctl = new CompilerTypeList();
		ctl.add(arg1);
		ctl.add(arg2);
		return ctl;
	}
}
