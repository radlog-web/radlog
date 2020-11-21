package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.recursion.seminaive;

import edu.ucla.cs.wis.bigdatalog.database.Tuple;
import edu.ucla.cs.wis.bigdatalog.database.cursor.Cursor;
import edu.ucla.cs.wis.bigdatalog.database.relation.Relation;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeList;

public interface IClique {
	public int getAnyTuple(Cursor<?> readCursor, CliqueBaseNode clique, Tuple tuple);
	
	public String getPredicateName();
	
	public int getArity();
	
	public Relation<?> getRecursiveRelation();
	
	public void setMutualCliqueList(NodeList<IMutualClique> mutualCliques);
	
	public NodeList<IMutualClique> getMutualCliqueList();
	
	public String toString();
	
	public String toStringNode();
}
