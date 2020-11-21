package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbInteger;
import edu.ucla.cs.wis.bigdatalog.database.type.DbString;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.Variable;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.OrNode;

public class SubStringNode extends OrNode {

	public SubStringNode(String predicateName, NodeArguments args, Binding binding, VariableList freeVariables) {
		super(predicateName, args, binding, freeVariables);
	}

	public boolean initialize() { return true; }

	public Status getTuple() {
		Status status;
		traceGetTupleEntry();
		if (isEntry) {
			String expr = ((DbString) this.getArgumentAsDbType(0)).getValue();
			int start = ((DbInteger)this.getArgumentAsDbType(1)).getValue() - 1;
			int length = ((DbInteger)this.getArgumentAsDbType(2)).getValue();
			((Variable) arguments.get(3)).setValue(this.typeManager.createString(expr.substring(start, start + length)));
			status = Status.SUCCESS;
			isEntry = false;
		} else {
			isEntry = true;
			status = Status.ENTRY_FAIL;
		}
		traceGetTupleExit(status);
		return status;
	}

	public void deleteRelationsAndCursors() { }

	public void cleanUp() {
		freeVariableList.makeFree();
		baseNodeCleanUp();
	}

	public void partialCleanUp() { cleanUp(); }

	public String toString() { return toStringNode(); }
}