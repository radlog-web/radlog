package edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucla.cs.wis.bigdatalog.database.type.DbDateTime;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Arguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;

public class OperatorArguments extends ArrayList<Argument> implements Arguments, Serializable {
	private static final long serialVersionUID = 1L;

	public OperatorArguments() {}
	
	public OperatorArguments(List<Argument> arguments) {
		for (Argument arg : arguments)
			this.add(arg);
	}
	
	public OperatorArguments(Argument[] arguments) {
		for (int i = 0; i < arguments.length; i++)
			this.add(arguments[i]);
	}

	/*@Override
	public boolean add(Argument argument) {
		if ((argument != null) && this.contains(argument))
			return false;
		
		return super.add(argument);
	}
	
	public boolean addAllowDuplicate(Argument argument) {
		return super.add(argument);
	}*/
	
	@Override
	public boolean contains(Argument argument) {
		for (Argument arg : this) {
			if (arg.equals(argument))
				return true;
			
			if ((arg instanceof AggregateArgument) && ((AggregateArgument)arg).contains(argument))
				return true;
			
			if ((arg instanceof AliasedArgument) && ((AliasedArgument)arg).contains(argument))
				return true;
			
			if ((arg instanceof AliasedVariable) && ((AliasedVariable)arg).contains(argument))
				return true;
		}
				
		return false;		
	}
	
	@Override
	public Argument set(int index, Argument argument) {
		return super.set(index, argument);
	}
	
	public void uniqueify() {
		Set<Argument> set = new HashSet<>();
		
		for (int i = this.size() - 1; i >= 0; i--) {
			if (this.get(i) == null)
				continue;
			
			if (set.contains(this.get(i)))
				this.remove(i);
			else
				set.add(this.get(i));
		}
	}
	
	public boolean hasAllVariables() {
		boolean allVariables = true;
		for (Argument arg : this) {
			if (!(arg instanceof Variable))				
				allVariables = false;
			else if ((arg instanceof Variable) && ((Variable)arg).isAnonymous())
				allVariables = false;
		}
		return allVariables;
	}
	
	public boolean hasConstant() { 
		for (Argument arg : this)
			if (arg.isConstant())
				return true;
		
		return false;
	}
	
	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		for (int i = 0; i < this.size(); i++) {
			if (i > 0)
				output.append(", ");
			
			if (this.get(i) != null) {
				if ((this.get(i) instanceof DbString) || (this.get(i) instanceof DbDateTime))
					output.append("'" + this.get(i).toString() + "'");
				else
					output.append(this.get(i).toString());
			}
    	}
		return output.toString();
	}
	
	public OperatorArguments copy() {
		OperatorArguments arguments = new OperatorArguments();
		for (Argument arg : this) {
			arguments.add(arg);
		}
		return arguments;
	}
	
	public boolean equals(OperatorArguments other) {
		if (other == null)
			return false;
		
		if (other.size() != this.size())
			return false;
		
		// compare in order
		for (int i = 0; i < this.size(); i++) {
			if (this.get(i) != other.get(i))
				return false;
		}
		return true;
	}
	
	public OperatorArguments removeConstants() {
		for (int i = this.size() - 1; i >= 0; i--)
			if (this.get(i).isConstant())
				this.remove(i);
		
		return this;
	}
}
