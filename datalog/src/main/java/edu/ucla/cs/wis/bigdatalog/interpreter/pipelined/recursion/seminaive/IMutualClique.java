package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive;

import edu.ucla.cs.wis.bigdatalog.database.cursor.FixpointCursor;

public interface IMutualClique extends IClique {
	
	public FixpointCursor getFixpointCursor();
	
	public String getRecursiveRelationName();
}
