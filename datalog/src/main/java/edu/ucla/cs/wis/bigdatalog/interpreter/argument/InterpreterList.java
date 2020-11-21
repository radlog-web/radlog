package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class InterpreterList 
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected Argument head;
	protected InterpreterList tail;
	
	public InterpreterList(Argument head, InterpreterList tail) {
		this.head = head;
		this.tail = tail;
	}
	
	public InterpreterList(Argument head) {
		this(head, null);
	}
	
	public InterpreterList() {
		this(null, null);
	}
	
	public Argument getHead() { return this.head; }
	
	public void setHead(Argument head) {
		this.head = head;
	}
	
	public InterpreterList getTail() { return this.tail; }

	public void setTail(InterpreterList tail) {
		this.tail = tail;
	}
	
	public int size() { 
		if (this.tail == null) {
			if (this.head == null)
				return 0;
			
			return 1;
		}
		
		return 1 + this.tail.size();
	}
	
	public boolean isEmpty() {
		return (this.head == null);
	}
	
	public InterpreterList copy() {
		if (this.tail == null)
			return new InterpreterList(this.head);
		
		return new InterpreterList(this.head, this.tail.copy());
	}
	
	public InterpreterList copy(ArgumentList argumentList) {
		if (this.tail == null)
			return new InterpreterList(this.head.copy(argumentList));
		
		return new InterpreterList(this.head.copy(argumentList), this.tail.copy(argumentList));
	}
	
	public InterpreterList copyExceptVariables() {
		if (this.isEmpty())
			return new InterpreterList();
		
		Argument newHead;
		if (this.head instanceof InterpreterFunctor)
			newHead = ((InterpreterFunctor)this.head).copyExceptVariables();
		else if (this.head instanceof InterpreterList)
			newHead = ((InterpreterList)this.head).copyExceptVariables();
		else
			newHead = this.head;
		
		if (this.tail == null)
			return new InterpreterList(newHead);
		
		return new InterpreterList(newHead, this.tail.copyExceptVariables());
	}
	
	@Override
	public boolean isGround() {
		if (this.isEmpty())
			return false;
		
		if (!head.isGround())
			return false;
		
		if (tail != null)
			if (!tail.isGround())
				return false;
		
		return true;
	}

	@Override
	public boolean isBound() {
		if (this.isEmpty()) 
			return false;
		
		if (!head.isBound())
			return false;
		
		if (tail != null)
			if (!tail.isBound())
				return false;
		
		return true;
	}
	
	@Override
	public boolean isConstant() {
		if (!head.isConstant())
			return false;
		
		if (tail != null)
			if (!tail.isConstant())
				return false;
		
		return true;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("[");
		if (!this.isEmpty()) {			
			retval.append(head.toString());
			if (tail != null)
				retval.append("|" + tail.toString());
		}
		retval.append("]");
		return retval.toString();
	}
	
	public boolean equals(Argument other) {
		if (other == null)
			return false;
		
		if (!(other instanceof InterpreterList))
			return false;
		
		InterpreterList otherList = (InterpreterList)other;
		
		return (this.head.equals(otherList.getHead()) 
				&& this.tail.equals(otherList.getTail())); 
	}

	@Override
	public DataType getDataType() {
		//if (this.isGround())
		return DataType.LIST;
		//return DataType.UNKNOWN;
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		if (this.isEmpty())
			return DbList.create();

		DbTypeBase dbHead = this.getHead().toDbType(typeManager);
		DbList dbTail = null;
		
		// if we have a null head, we don't care about the tail
		if ((dbHead == null) || ((dbHead instanceof DbList) && ((DbList)dbHead).isEmpty())) {
			dbHead = null;
		} else {	
			if (this.getTail() != null) {
				dbTail = (DbList)this.getTail().toDbType(typeManager);
				if (dbTail != null && !dbTail.isEmpty() 
						&& (dbTail.getHead() instanceof DbList) 
						&& (dbTail.getTail() == null))
					dbTail = (DbList)dbTail.getHead();
			}
		}
		
	    return typeManager.createList(dbHead, dbTail);
	}

	@Override
	public Argument reduce() {
		throw new InterpreterException("Irreducible object type. " + this.toString());
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		boolean status = false;
		if ((dbTypeObject != null) && (dbTypeObject instanceof DbList)) {
			DbList dbList = (DbList)dbTypeObject;
			
			if (dbList.isEmpty() && this.isEmpty()) {
		  		status = true;
		  	} else if (dbList.isEmpty() || this.isEmpty()) {
		  		status = false;
		  	} else if (!this.getHead().match(dbList.getHead())) {
		  		status = false;	
		  	} else {
		  		// if the tail is a variable, we need to bind the tail of the dbList to it
		  		if ((this.getTail() != null) 
		  				&& !this.getTail().isEmpty() 
		  				&& (this.getTail().getHead() instanceof Variable)) {
		  			Argument arg = ((Variable)this.getTail().getHead()).dereference();
		  			if (arg instanceof DbTypeBase) {
		  				status = ((DbTypeBase)arg).equals(dbList.getTail());
		  			} else {
		  				if (dbList.getTail() == null)
		  					((Variable)arg).value = DbList.create();
		  				else
		  					((Variable)arg).value = dbList.getTail();
		  				status = true;
		  			} 
		  		} else {
		  			status = this.getHead().match(dbList.getHead());
		  		}
		  	}
		}
		return status;
	}

	@Override
	public boolean matchByFree(Argument boundArgument) {
		boolean isMatch = false;
		if (boundArgument instanceof DbList)
			isMatch = this.match((DbTypeBase) boundArgument);
			//isMatch = this.matchDbListToList((DbList)tempObject, (InterpreterList)freeObject);
		else
			isMatch = boundArgument.matchByBound(this);
		return isMatch;
	}

	@Override
	public boolean matchByBound(Argument freeArgument) {
		if (freeArgument instanceof InterpreterList)
			return this.matchLists(this, (InterpreterList)freeArgument);
		return false;
	}
	
	private boolean matchLists(InterpreterList boundList, InterpreterList freeList) {
		if (boundList.isEmpty() && freeList.isEmpty())
			return true;

		if (boundList.isEmpty() || freeList.isEmpty())
			return false;

		if (freeList.getHead().matchByFree(boundList.getHead())) {
			InterpreterList boundTail = boundList.getTail();
			InterpreterList freeTail = freeList.getTail();

			if (boundTail != null && freeTail != null)
				return this.matchLists(boundTail, freeTail);
			
			return freeTail.matchByFree(boundTail);
	    }
		
	    return false;
	}
	
	@Override
	public String toFact() {
		StringBuilder fact = new StringBuilder();
		fact.append("argument(");
		fact.append(this.hashCode());
		fact.append(",'list','");
		fact.append(this.toString());
		fact.append("','");
		fact.append(this.getDataType());
		fact.append("').");
		return fact.toString();
	}
}
