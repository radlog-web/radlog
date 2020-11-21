package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbComplex;
import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class InterpreterFunctor 
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;
	
	protected String functorName;
	protected NodeArguments arguments;
	
	public InterpreterFunctor(String functorName, NodeArguments arguments) {
		this.functorName = functorName;
		this.arguments = arguments;
	}

	public String getFunctorName() {return this.functorName;}
	
	public int getArity() {return this.arguments.size();}
			
	public InterpreterFunctor copy() {
		return new InterpreterFunctor(new String(this.functorName), this.arguments);
	}
	
	public InterpreterFunctor copy(ArgumentList argumentList) {
		return new InterpreterFunctor(new String(this.functorName), this.arguments.copy(argumentList));
	}
	
	public InterpreterFunctor copyExceptVariables() {
		NodeArguments nodeArguments = new NodeArguments(this.arguments.size());
		for (int i = 0; i < this.arguments.size(); i++) {
			if (this.arguments.get(i) instanceof InterpreterFunctor)
				nodeArguments.set(i,  ((InterpreterFunctor)this.arguments.get(i)).copyExceptVariables());
			else if (this.arguments.get(i) instanceof InterpreterList)
				nodeArguments.set(i,  ((InterpreterList)this.arguments.get(i)).copyExceptVariables());
			else
				nodeArguments.set(i,  this.arguments.get(i));
		}
		
		return new InterpreterFunctor(this.functorName, nodeArguments);
	}

	public NodeArguments getArguments() { return this.arguments; }

	public Argument getArgument(int position) { return this.arguments.get(position); }

	public void setArgument(int position, Argument argument) {
		this.arguments.set(position, argument);
	}
	
	@Override
	public boolean isGround() {
		for (int i = 0; i < this.arguments.size(); i++) {
			if (!this.arguments.get(i).isGround())
				return false;
		}
		
		return true;
	}

	@Override
	public boolean isBound() {
		for (int i = 0; i < this.arguments.size(); i++) {
			if (!this.arguments.get(i).isBound())
				return false;
		}
		
		return true;
	}
	
	@Override
	public boolean isConstant() {
		for (int i = 0; i < this.getArity(); i++) {
			if (!this.arguments.get(i).isConstant())
				return false;
		}
		return true;
	}
		
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.functorName.toString());
		output.append("(");

		if (this.getArity() > 0) {
			output.append(this.arguments.get(0).toString());
			
			for (int i = 1; i < this.arguments.size(); i++) {
				output.append(", ");
				if (this.arguments.get(1) instanceof InterpreterFunctor)
					output.append(((InterpreterFunctor)this.arguments.get(i)).toStringWithParen());
				else
					output.append(this.arguments.get(i).toString());		
			}
		}			
		output.append(")");
		return output.toString();
	}
	
	public String toStringWithParen() {
		StringBuilder output = new StringBuilder();
    	output.append(this.functorName + "(");
    	if (this.getArity() > 0) {
    		output.append(this.arguments.get(0).toString());
	  
    		for (int i = 1; i < this.arguments.size(); i++) {
    			output.append(", ");
	      
    			if (this.arguments.get(1) instanceof InterpreterFunctor)
    				output.append(((InterpreterFunctor)this.arguments.get(i)).toStringWithParen());
    			else
    				output.append(this.arguments.get(i).toString());
    		}
    	}	      
    	output.append(")");
		return output.toString();
	}

	public boolean equals(Argument other) {
		if (other == null)
			return false;
		
		if (!(other instanceof InterpreterFunctor))
			return false;
		
		InterpreterFunctor otherFunctor = (InterpreterFunctor)other;
		
		return (this.functorName.equals(otherFunctor.getFunctorName()) 
				&& (this.arguments.size() == otherFunctor.getArity()) 
				&& this.arguments.equals(otherFunctor.getArguments())); 
	}
	
	public static InterpreterFunctor createFunctor(String functorName, Argument[] args) {
		NodeArguments nodeArguments = new NodeArguments(args);
		return new InterpreterFunctor(functorName, nodeArguments);
	}
	
	public static InterpreterFunctor createFunctor(Argument[] args) {
		return createFunctor("", args);
	}

	@Override
	public DataType getDataType() {
		//if (this.isGround())
			return DataType.COMPLEX;
		//return DataType.UNKNOWN;
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		// APS 9/16/2013 - for choice - the functor is a wrapper for a single argument
		if (this.getArity() == 1)
			return this.getArgument(0).toDbType(typeManager);
					
		DbTypeBase[] dbTypeArray = new DbTypeBase[this.getArity()];
		for (int i = 0; i < this.getArity(); i++)
			dbTypeArray[i] = this.getArgument(i).toDbType(typeManager);

		return typeManager.createComplex(this.getFunctorName(), dbTypeArray);
	}
	
	@Override
	public Argument reduce() {	
		throw new InterpreterException("Irreducible object type. " + this.toString());
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		// APS 1/20/15 - when matching, we might need to cast the arguments to the correct datatype
		// this is because the baserelations don't know the proper type for the arguments
		if (dbTypeObject instanceof DbComplex) {
			DbComplex complex = (DbComplex)dbTypeObject;
			if (complex.getArity() == this.getArity()) {
				if (complex.getName().getValue().equals(this.getFunctorName())) {
					for (int i = 0; i < complex.getArity(); i++) {
						Argument arg = this.getArgument(i);
						DbTypeBase value = complex.getArgument(i);
						if (DataType.isNumeric(arg.getDataType()) 
								&& (arg instanceof Variable) 
								&& (arg.getDataType() != value.getDataType())) {
							value = DataType.cast((DbNumericType)value, arg.getDataType());
						}
						
						if (!this.getArgument(i).match(value))
							return false;
					}
					return true;
				}
			}	  
		}
		return false;
	}

	@Override
	public boolean matchByFree(Argument boundArgument) {
		if (boundArgument instanceof DbComplex)
			return this.match((DbTypeBase) boundArgument);
		
		return boundArgument.matchByBound(this);
	}

	@Override
	public boolean matchByBound(Argument freeArgument) {
		if (freeArgument instanceof InterpreterFunctor)
			return this.matchFunctors((InterpreterFunctor)freeArgument);	
		return false;
	}
	
	private boolean matchFunctors(InterpreterFunctor freeFunctor) {
		if (this.getArity() == freeFunctor.getArity()) {
			if (this.getFunctorName().equals(freeFunctor.getFunctorName())) {
				for (int i = 0; i < this.getArity(); i++)
					if (!freeFunctor.getArgument(i).matchByFree(this.getArgument(i)))
						return false;
		  
				return true;
			}
	    }

		return false;
	}
	
	@Override
	public String toFact() {
		StringBuilder fact = new StringBuilder();
		fact.append("argument(");
		fact.append(this.hashCode());
		fact.append(",'functor','");
		fact.append(this.toString());
		fact.append("','");
		fact.append(this.getDataType());
		fact.append("').");
		return fact.toString();
	}
	
	public static InterpreterFunctor create(Argument[] arguments) {
		return new InterpreterFunctor("", new NodeArguments(arguments));
	}
}
