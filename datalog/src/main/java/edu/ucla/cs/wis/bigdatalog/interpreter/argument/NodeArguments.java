package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.common.Pair;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.UnaryExpression;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class NodeArguments implements Argument, Arguments, Serializable {
	private static final long serialVersionUID = 1L;
	
	public Argument[] innerArguments;
	
	public NodeArguments() {
		this(0);
	}
	
	public NodeArguments(int size) {
		this.innerArguments = new Argument[size];
	}
	
	public NodeArguments(Argument[] arguments) {
		this.innerArguments = arguments;
	}
	
	public int size() { return this.innerArguments.length; }
	
	public boolean isEmpty() { return this.innerArguments.length == 0; }
	
	public Argument get(int position) { return this.innerArguments[position]; }
	
	public boolean add(Argument argument) {
		Argument[] newArgs = new Argument[this.innerArguments.length + 1];
		for (int i = 0; i < this.innerArguments.length; i++)
			newArgs[i] = this.innerArguments[i];
		
		newArgs[this.innerArguments.length] = argument;
		this.innerArguments = newArgs;
		return true;
	}
	
	public void remove(Argument arg) {
		Argument[] newArgs = new Argument[this.innerArguments.length - 1];
		int j = 0;
		for (int i = 0; i < this.innerArguments.length; i++) {
			if (this.innerArguments[i] == arg)
				continue;
		
			newArgs[j++] = this.innerArguments[i];
		}
		this.innerArguments = newArgs;
	}
	
	public Argument set(int position, Argument argument) {
		Argument oldArgument = this.innerArguments[position];
		this.innerArguments[position] = argument;
		return oldArgument;
	}
	
	public void clear() {
		this.innerArguments = new Argument[0];
	}
	
	public boolean contains(Argument argument) {
		for (int i = 0; i < this.innerArguments.length; i++) {
			if (this.innerArguments[i] == argument)
				return true;
		}
		return false;
	}
		
	public NodeArguments copy() {
		NodeArguments newArguments = new NodeArguments(this.innerArguments.length);
		
		for (int i = 0; i < this.innerArguments.length; i++) {
			if (this.innerArguments[i] instanceof InterpreterFunctor)
				newArguments.set(i, ((InterpreterFunctor)this.innerArguments[i]).copy());
			else if (this.innerArguments[i] instanceof InterpreterList)
				newArguments.set(i, ((InterpreterList)this.innerArguments[i]).copy());
			else if (this.innerArguments[i] instanceof InputVariable)
				newArguments.set(i, ((InputVariable)this.innerArguments[i]).copy());
			else
				newArguments.set(i, this.innerArguments[i].copy());
		}

		return newArguments;	
	}
	
	public NodeArguments copy(ArgumentList argumentList) {
		NodeArguments newArguments = new NodeArguments(this.innerArguments.length);
		
		for (int i = 0; i < this.innerArguments.length; i++) {
			if (this.innerArguments[i] instanceof InterpreterFunctor)
				newArguments.set(i, ((InterpreterFunctor)this.innerArguments[i]).copy(argumentList));
			else if (this.innerArguments[i] instanceof InterpreterList)
				newArguments.set(i, ((InterpreterList)this.innerArguments[i]).copy(argumentList));
			else if (this.innerArguments[i] instanceof InputVariable)
				newArguments.set(i, ((InputVariable)this.innerArguments[i]).copy(argumentList));
			else if (this.innerArguments[i] instanceof Variable)
				newArguments.set(i, ((Variable)this.innerArguments[i]).copy(argumentList));
			else
				newArguments.set(i, this.innerArguments[i].copy());
		}

		return newArguments;	
	}
	
	public NodeArguments copyExceptVariables() {
		// Copy arguments that points to the copies of the elements
		NodeArguments newArguments = new NodeArguments(this.innerArguments.length);
		
		for (int i = 0; i < this.innerArguments.length; i++) {
			if (this.innerArguments[i] instanceof InterpreterFunctor)
				newArguments.set(i, ((InterpreterFunctor)this.innerArguments[i]).copyExceptVariables());
			else if (this.innerArguments[i] instanceof InterpreterList)
				newArguments.set(i, ((InterpreterList)this.innerArguments[i]).copyExceptVariables());
			else if (this.innerArguments[i] instanceof InputVariable)
				newArguments.set(i, ((InputVariable)this.innerArguments[i]).copy());
			else
				newArguments.set(i, this.innerArguments[i]);
		}

		return newArguments;
	}

	@Override
	public boolean isGround() {
		for (int i = 0; i < this.innerArguments.length; i++)
			if (!this.innerArguments[i].isGround())
				return false;
		
		return true;
	}

	@Override
	public boolean isBound() {
		for (int i = 0; i < this.innerArguments.length; i++)
			if (!this.innerArguments[i].isBound())
				return false;
		
		return true;
	}

	@Override
	public boolean isConstant() {
		for (int i = 0; i < this.innerArguments.length; i++)
			if (!this.innerArguments[i].isConstant())
				return false;
		
		return true;
	}
	
	public Pair<Boolean, int[]> compressVariableAssignments(boolean removeDuplicates) {
		for (int i = this.innerArguments.length - 1; i >= 0; i--) 
			this.innerArguments[i] = this.compressVariables(this.innerArguments[i]);
		
		if (removeDuplicates) {
			int originalLength = this.innerArguments.length;
			
			//System.out.println("Compressing " + this.toString());
							
			// map so caller knows what original position got removed
			int[] map = new int[originalLength];
			
			List<Argument> args = new LinkedList<>();
			
			for (int i = 0; i < this.innerArguments.length; i++) {
				if (!args.contains(this.innerArguments[i])) {
					args.add(this.innerArguments[i]);
					map[i] = 1;
				}
			}
			
			this.innerArguments = new Argument[args.size()];		
			this.innerArguments = args.toArray(this.innerArguments);
			
			//System.out.println("To " + this.toString());
			
			boolean status = (originalLength != this.innerArguments.length);
			return new Pair<>(status, map);
		}
		
		int[] map = new int[this.innerArguments.length];
		for (int i = 0; i < map.length; i++)
			map[i] = 1;
		
		return new Pair<>(false, map);
	}
	
	private Argument compressVariables(Argument arg) {
		if (arg instanceof Variable) {
			return ((Variable) arg).deepDereference();
		} else if (arg instanceof InterpreterFunctor) {
			InterpreterFunctor functor = (InterpreterFunctor)arg;
			for (int j = functor.getArity() - 1; j >= 0; j--)
				functor.setArgument(j, compressVariables(functor.getArgument(j)));
			
			return functor;
		} else if (arg instanceof InterpreterList) {
			InterpreterList list = (InterpreterList)arg;
			list.setHead(compressVariables(list.getHead()));
			this.compressVariables(list.getTail());
			return list;
		} else if (arg instanceof InputVariable) {
			if (((InputVariable) arg).getValue() != null)
				return ((InputVariable) arg).getValue();
			
			return arg;
		} else if (arg instanceof BinaryExpression) {
			BinaryExpression be = (BinaryExpression)arg;
			be.setLeft(this.compressVariables(be.getLeft()));
			be.setRight(this.compressVariables(be.getRight()));
			return be;
		} else if (arg instanceof UnaryExpression) {
			UnaryExpression ue = (UnaryExpression)arg;
			ue.setArgument(this.compressVariables(ue.getArgument()));
			return ue;
		} else {//(arg instanceof DbTypeBase) {
			return arg;
		}
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		for (int i = 0; i < this.innerArguments.length; i++) {
			if (i > 0)
				retval.append(", ");
			retval.append(this.innerArguments[i].toString());
		}
		return retval.toString();
	}
	
	public String toStringValues() {
		StringBuilder retval = new StringBuilder();
		for (int i = 0; i < this.innerArguments.length; i++) {
			if (i > 0)
				retval.append(", ");
			if (this.innerArguments[i].isBound() || this.innerArguments[i].isGround()) {
				retval.append(this.innerArguments[i].toString());
			} else {
				retval.append(this.innerArguments[i].toString());
			}
		}
		return retval.toString();
	}

	@Override
	public DataType getDataType() { return DataType.UNKNOWN; }

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		throw new RuntimeException("getDbType should not be called on NodeArguments.  I know, bad object design....");
	}
	
	// maintains argument uniqueness
	public NodeArguments merge(NodeArguments argumentsToMerge) {		
		List<Argument> args = new LinkedList<>();
		
		for (int i = 0; i < this.innerArguments.length; i++)
			args.add(this.innerArguments[i]);
		
		for (int i = 0; i < argumentsToMerge.size(); i++)
			if (!args.contains(argumentsToMerge.get(i)))
				args.add(argumentsToMerge.get(i));
		
		this.innerArguments = new Argument[args.size()];		
		this.innerArguments = args.toArray(this.innerArguments);

		return this;
	}
	
	public NodeArguments merge(Argument argumentToMerge) {
		Argument[] args = new Argument[]{argumentToMerge};
		return this.merge(new NodeArguments(args));
	}
	
	@Override
	public Argument reduce() {
		throw new InterpreterException("Irreducible object type. " + this.toString());
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		return false; // this method should not be used
	}

	@Override
	public boolean matchByFree(Argument argument) { return false; }

	@Override
	public boolean matchByBound(Argument argument) { return false; }
	
	@Override
	public String toFact() {
		StringBuilder facts = new StringBuilder();
		for (int i = 0; i < this.size(); i++)
			facts.append(this.innerArguments[i].toFact());

		return facts.toString();
	}
	
	public List<String> toFacts() {
		List<String> facts = new ArrayList<>();
		for (int i = 0; i < this.size(); i++)
			facts.add(this.innerArguments[i].toFact());
		return facts;
	}
}
