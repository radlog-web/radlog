package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.xy;

import java.util.ArrayList;

import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.relation.RelationNode;

public class XYLiteralListManager {	
	private ArrayList<NodeList<RelationNode>> xyLiteralLists;
	
	public XYLiteralListManager() {
		this.xyLiteralLists = new ArrayList<>();
	}
	
	public void clearXYLiteralLists() {
		this.xyLiteralLists.clear();
		//for (int i = 0; i < this.xyLiteralLists.size(); i++)
			//this.xyLiteralLists.get(i).clear();
	}
	  
	public NodeList<RelationNode> getXYLiteralList(String relationName, int arity) {
		RelationNode literal;
		for (NodeList<RelationNode> list : this.xyLiteralLists) {
			if ((literal = list.get(0)) != null) {
				if ((literal.getArity() == arity) && 
						literal.getRelation().getName().equals(relationName))
					return list;
			}
		}
		
		return null;
	}

	public void addXYLiteralList(RelationNode literal, String relationName) {
		NodeList<RelationNode> list;

		if ((list = this.getXYLiteralList(relationName, literal.getArity())) == null) {
			list = new NodeList<>();
			this.xyLiteralLists.add(list);
		}
		list.add(literal);
	}
}
