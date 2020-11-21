package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;

public class AppendNode 
	extends OrNode {
	protected DbList createdList;

	public AppendNode(String name, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(name, args, binding, freeVariables);
		this.createdList = null;
	}

	protected int postSelectForAppend(DbTypeBase dbTypeObject, int position) {
		//return ((this.matchDbTypeToArgument(dbTypeObject, this.getArgument(position))) ? 1 : 0);
		return ((this.getArgument(position).match(dbTypeObject)) ? 1 : 0);
	}

	@Override
	public void deleteRelationsAndCursors() { }

	@Override
	public void cleanUp() 	 {
		//this.unsetFreeVariables();
		this.freeVariableList.makeFree();
		this.baseNodeCleanUp();
	}

	@Override
	public boolean initialize() { return true; }

	@Override
	public void partialCleanUp() {
		this.cleanUp();
	}

	@Override
	public String toString() {
		return toStringNode();
	}

	protected void copyElementsIntoArray(DbList dbList, DbTypeBase[] dbTypeArray) {
		DbList list = dbList;

		for (int i = 0; i < dbList.getLength() && !list.isEmpty(); i++, list = list.getTail())
			dbTypeArray[i] = list.getHead();
	}
}
