package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerList extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected CompilerTypeBase head;
	protected CompilerList tail;

	public CompilerList(CompilerTypeBase head, CompilerList tail) {
		super(CompilerType.COMPILER_LIST);
		this.head = head;
		this.tail = tail;
	}

	public CompilerList(CompilerTypeBase head) {
		this(head, null);
	}
	
	public CompilerList(CompilerTypeList list) {
		super(CompilerType.COMPILER_LIST);
		if (list == null || list.size() == 0) {
			this.head = null;
			this.tail = null;
			return;
		}
		
		this.head = list.getFirst();
		
		for (int i = list.size() - 1; i > 0; i--) {
			this.tail = new CompilerList(list.get(i), this.tail);
		}
	}

	public CompilerList() {
		this(null, null);
	}

	public boolean isEmpty() {return this.head == null;}

	public CompilerTypeBase getHead() {return this.head;}

	public CompilerList getTail() {return this.tail;}

	public void setHead(CompilerTypeBase head) {this.head = head;}

	public void setTail(CompilerList tail) {this.tail = tail;}

	public CompilerList copy() { 
		if (this.isEmpty())
			return new CompilerList(null, null);
		
		if (this.tail == null)
			return new CompilerList(this.head.copy(), null);
		
		return new CompilerList(this.head.copy(),this.tail.copy());
	}

	public CompilerList copy(CompilerVariableList variableList) {
		if (this.isEmpty())
			return new CompilerList(null, null);
		
		if (this.tail == null)
			return new CompilerList(this.head.copy(variableList), null);

		return new CompilerList(this.head.copy(variableList),this.tail.copy(variableList));
	}

	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerList))
			return false;

		CompilerList otherList = (CompilerList)other;

		if (this.isEmpty())
			return otherList.isEmpty();

		if (otherList.head == null || (!this.head.equals(otherList.head)))
			return false;	

		if (this.tail != null)
			return this.tail.equals(otherList.tail);

		return (otherList.tail == null);
	}

	public String toString() {
		return toString(0);
	}
	
	private String toString(int length) {
		StringBuilder retval = new StringBuilder();
		if (length == 0)
			retval.append("[");
		
		if (!this.isEmpty()) {
			retval.append(this.head.toString());
			
			if (this.tail != null) {
				if (this.tail.head != null) {
					//if (length == 0)
					//	retval.append("|");
					//else
						retval.append(",");
					
					retval.append(this.tail.toString(length + 1));
				}
			}		
		}

		if (length == 0)
			retval.append("]");
		return retval.toString();
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		if (this.isEmpty())
			return DbList.create();
				
		DbTypeBase dbHead = ((DbConvertible)this.getHead()).toDbType(typeManager);
		DbList dbTail = null;

		if (this.tail != null)
			dbTail = (DbList)((DbConvertible)this.getTail()).toDbType(typeManager);

	    return typeManager.createList(dbHead, dbTail);
	}
	
	@Override
	public DataType getDataType() {
		return DataType.LIST;
	}
	
	public CompilerTypeList toCompilerTypeList() {
		CompilerTypeList ctl = new CompilerTypeList();
		CompilerList list = this;

		while (list != null && !list.isEmpty()) {
			ctl.add(list.getHead());
			list = list.getTail();
		}
		return ctl;
	}
}
