package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;

public interface QueryFormNode {	
	public Relation<?> getRelation();
	
	public Cursor<?> getQueryFormCursor();
}
