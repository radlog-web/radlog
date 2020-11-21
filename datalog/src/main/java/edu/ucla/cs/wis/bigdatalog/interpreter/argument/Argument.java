package edu.ucla.cs.wis.bigdatalog.interpreter.argument;

import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public interface Argument {
	
	public DataType getDataType();

	public boolean isGround();
	
	public boolean isBound();
	
	public boolean isConstant();
	
	public DbTypeBase toDbType(TypeManager typeManager);
	
	public Argument reduce();
	
	public boolean match(DbTypeBase dbTypeObject);
	
	public boolean matchByFree(Argument argument);
	
	public boolean matchByBound(Argument argument);
	
	public String toFact();
	
	public Argument copy();
	
	public Argument copy(ArgumentList argumentList);
}
