package edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument;

import java.io.Serializable;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Argument;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.ArgumentList;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AggregateArgument 
	implements Argument, Serializable {
	private static final long serialVersionUID = 1L;
	private String name;
	private Argument term;
	
	public AggregateArgument(String name, Argument term) {
		this.name = name;
		this.term = term;
	}
	
	public String getName() { return this.name; }

	// Youfu Li added this to hack the mavg aggregation
	public void setName(String name) {this.name = name;}
	
	public Argument getTerm() { return this.term; }
	
	public void setTerm(Argument term) { this.term = term; }
		
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.name.toString());
		output.append("(");
		output.append(this.term.toString());
		output.append(")");
		return output.toString();
	}

	@Override
	public DataType getDataType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isGround() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isBound() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isConstant() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DbTypeBase toDbType(TypeManager typeManager) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Argument reduce() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean match(DbTypeBase dbTypeObject) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean matchByFree(Argument argument) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean matchByBound(Argument argument) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String toFact() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Argument copy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Argument copy(ArgumentList argumentList) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean contains(Argument argument) {
		if (this.equals(argument))
			return true;
		
		if ((this.term instanceof Variable) && ((Variable)this.term).equals(argument))
			return true;
		
		return false;
	}

}
