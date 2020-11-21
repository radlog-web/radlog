package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class CompilerFunctor extends CompilerTypeBase implements DbConvertible {
	private static final long serialVersionUID = 1L;
	protected String functorName;
	protected CompilerTypeList arguments;
	
	public CompilerFunctor(String functorName, CompilerTypeList arguments) {
		super(CompilerType.COMPILER_FUNCTOR);
		this.functorName = functorName;
		this.arguments = arguments;
	}

	public CompilerFunctor(String functorName, int arity) {
		super(CompilerType.COMPILER_FUNCTOR);
		this.functorName = functorName;
		this.arguments = new CompilerTypeList(arity);
	}

	public String getFunctorName() {return this.functorName;}
	
	public int getArity() {return this.arguments.size();}
	
	public CompilerFunctor copy() {
		return new CompilerFunctor(this.functorName,this.arguments.copy());
	}

	public CompilerFunctor copy(CompilerVariableList variableList) {
		return new CompilerFunctor(this.functorName,this.arguments.copy(variableList));
	}

	public CompilerTypeList getArguments() { return this.arguments; }

	public CompilerTypeBase getArgument(int i) { return this.arguments.get(i); }
	
	public void insertArgument(int position, CompilerTypeBase compilerTypeObject) {
		this.arguments.add(position, compilerTypeObject);
	}

	public void appendArgument(CompilerTypeBase compilerTypeObject) {
		this.arguments.add(compilerTypeObject);
	}

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append(this.functorName.toString());
		retval.append("(");

		if (this.getArity() > 0) {
			retval.append(this.arguments.get(0).toString());
			
			for (int i = 1; i < this.arguments.size(); i++) {
				retval.append(", ");
				if (this.arguments.get(i).isFunctor())
					retval.append(((CompilerFunctor)this.arguments.get(i)).toStringWithParen());
				else
					retval.append(this.arguments.get(i).toString());		
			}
		}			
		retval.append(")");

		return retval.toString();
	}
	
	public String toStringWithParen() {
		StringBuilder retval = new StringBuilder();
    	retval.append(this.functorName + "(");
    	if (this.getArity() > 0) {
    		retval.append(this.arguments.get(0).toString());
	  
    		for (int i = 1; i < this.arguments.size(); i++) {
    			retval.append(", ");
	      
    			if (this.arguments.get(i).isFunctor())
    				retval.append(((CompilerFunctor)this.arguments.get(i)).toStringWithParen());
    			else
    				retval.append(this.arguments.get(i).toString());
    		}
    	}	      
    	retval.append(")");

		return retval.toString();
	}

	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof CompilerFunctor))
			return false;
		
		CompilerFunctor otherFunctor = (CompilerFunctor)other;
		
		return (this.functorName.equals(otherFunctor.getFunctorName()) 
				&& (this.arguments.size() == otherFunctor.getArity()) 
				&& this.arguments.equals(otherFunctor.getArguments())); 
	}
	
	public static CompilerFunctor createFunctor(String functorName, CompilerTypeBase[] args) {
		CompilerTypeList funcArgs = new CompilerTypeList();
		for (CompilerTypeBase arg : args)
			if (arg != null)
				funcArgs.add(arg);
		
		return new CompilerFunctor(functorName, funcArgs);
	}
	
	public static CompilerFunctor createFunctor(CompilerTypeBase[] args) {
		return createFunctor("", args);
	}
	
	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		DbTypeBase[] arguments = new DbTypeBase[this.getArity()];
		for (int i = 0; i < this.getArity(); i++)
			arguments[i] = ((DbConvertible)this.getArgument(i)).toDbType(typeManager);

		return typeManager.createComplex(this.getFunctorName(), arguments);
	}
	
	@Override
	public DataType getDataType() {
		//ArithmeticOperation ao = ArithmeticOperation.getArithmeticOperation(this.functorName);
		//if (ao != ArithmeticOperation.NONE)
		//	return TypeInferrer.inferTermDataType(this);
		return DataType.COMPLEX;
	}
}
