package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbNumericType;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.InputVariable;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class Variable 
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;
	public static final String ANONYMOUS_VARIABLE_NAME = "_";
	public static final String ANONYMOUS_PREFIX	= "_$anonymous$";

	protected String name;
	protected Argument value;
	protected DataType dataType;

	public Variable(String name) {
		this(name, DataType.UNKNOWN);
	}
	
	public Variable(String name, DataType dataType) {
		if (name.equals(ANONYMOUS_VARIABLE_NAME))
			this.name = ANONYMOUS_PREFIX + String.valueOf(VariableList.globalVariableCount++);
		else
			this.name = name;

		this.dataType = dataType;
	} 

	public String getName() { return this.name;}
	
	public void rename(String name) { this.name = name; }
	
	public Argument getValue() { return this.value;}

	public void setValue(Argument value) {
		/*if (value instanceof Variable) {
			Variable var = (Variable)value;
			if (var.dataType == DataType.UNKNOWN)
				var.dataType = this.dataType;
			else if (this.dataType == DataType.UNKNOWN)
				this.dataType = var.dataType;			
		}*/
			
		this.value = value;
	}

	public DataType getDataType() { return this.dataType; }
	
	public void setDataType(DataType dataType) { 
		this.dataType = dataType; 
	}

	public boolean isAnonymous() { return this.name.startsWith(ANONYMOUS_PREFIX); }

	public boolean isBound() { return this.value != null;}
	
	public boolean hasValueAssigned() {
		if (!this.isBound())
			return false;
			
		return this.deepDereference().isBound();
	}
	
	public String toStringVariableName() {
		if (this.isAnonymous())
			return Variable.ANONYMOUS_VARIABLE_NAME;
		return this.name.toString();
	}

	public Argument deepDereference() {
		Argument temp = this;

		while (temp instanceof Variable && (((Variable)temp).value != null))
			temp = ((Variable)temp).value;

		if (temp instanceof InputVariable && ((InputVariable)temp).isBound())
			temp = ((InputVariable)temp).getValue();

		return temp;
	}
	
	public Argument dereference() {
		if (this.value == null)
			return this;
		
		return this.value;
	}
		
	public void makeFree() {
		this.value = null;			
	}
	
	public String toString() {
		StringBuilder output = new StringBuilder();
		/*if (this.deALSContext.isDebugEnabled()) {
			output.append(this.toStringVariableName());
			output.append("[" + this.hashCode() + "]");
			if (this.value != null) {
				output.append(" -> ");
				output.append(this.value.toString());
			}
		} else {*/
		Argument argument = this.deepDereference();

		// Display the dereferenced object if it is not a variable
		if (argument instanceof Variable)
			output.append(this.toStringVariableName());
		else
			output.append(argument.toString());
		//}
		
		//if (this.deALSContext.isDebugEnabled()) output.append("[" + this.hashCode() + "]");
		//if (this.deALSContext.isDebugEnabled()) output.append("[" + this.dataType + "]");
		
		return output.toString();
	}

	public Variable copy() {
		return this;
	}

	public Variable copy(VariableList variableList) {
		Variable var = null;
		//APS 1/2/2014 - don't use existing anonymous variables, since the lookup is by name
		// without this, anonymous variables could be shared between literals which is horrible w/ bound values  
		//if (this.isAnonymous())
		//	var = new Variable(ANONYMOUS_VARIABLE_NAME);
		//else
			var = variableList.getVariable(this);
				
		if (var != null)
			var.setDataType(this.getDataType());
				
		return var;
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		Variable var = null;
		
		for (int i = 0; i < argumentList.size(); i++) {
			if ((argumentList.get(i) instanceof Variable) 
					&& ((Variable)argumentList.get(i)).name.equals(this.name))
				var = (Variable)argumentList.get(i);			
		}
			
		if (var == null) {
			var = new Variable(this.name);
			argumentList.add(var);
		}
		var.setDataType(this.getDataType());
				
		return var;
	}

	public Variable deepCopy() {
		Variable copy = new Variable(this.name);
		if (this.value != null)
			copy.setValue(this.value);
		copy.dataType = this.dataType;
		return copy;
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		// this conditional is faster than just using the single return statement
		if (this.value instanceof DbTypeBase)
			return (DbTypeBase)this.value;
		
		return this.value.toDbType(typeManager);
	}

	@Override
	public Argument reduce() {
		return this.dereference();
		/*if (this.value == null)
			return this;
		
		return this.value;*/		
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		if (this.value == null) {
			this.value = dbTypeObject;
			return true;
		}
			
		return this.value.match(dbTypeObject);
	}

	@Override
	public boolean matchByFree(Argument boundArgument) {
		if (this.value == null) {
			if ((this.dataType != boundArgument.getDataType()) 
					&& (boundArgument instanceof DbTypeBase)) {
				this.value = ((DbNumericType)boundArgument).convertTo(this.dataType);
				return true;
			}
			
			this.value = boundArgument;
			return true;
		}
		
		return this.value.matchByFree(boundArgument);
	}

	@Override
	public boolean matchByBound(Argument freeArgument) {
		if (this.value != null)
			return freeArgument.matchByFree(this.value);
		return false;
	}
	
	public String toFact() {
		StringBuilder fact = new StringBuilder();
		fact.append("argument(");
		fact.append(this.hashCode());
		fact.append(",'variable','");
		fact.append(this.name);
		fact.append("','");
		fact.append(this.getDataType());
		fact.append("').");
		return fact.toString();
	}

	@Override
	public boolean isGround() {	return false; }

	@Override
	public boolean isConstant() { return false; }
}
