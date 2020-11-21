package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AppendBBBNode 
	extends AppendNode {
	
	public AppendBBBNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super("append_bbb", args, binding, freeVariables);
	}

	@Override
	public Status getTuple() {
		Status status;
		
		this.traceGetTupleEntry();
		
		if (this.isEntry) {
			DbTypeBase dbObject0 = null;
			DbTypeBase dbObject1 = null;
			DbTypeBase dbObject2 = null;

			if (((dbObject0 = this.getArgumentAsDbType(0)) != null)
					&& ((dbObject1 = this.getArgumentAsDbType(1)) != null)
					&& ((dbObject2 = this.getArgumentAsDbType(2)) != null)
					&& (dbObject0.getDataType() == DataType.LIST)
					&& (dbObject1.getDataType() == DataType.LIST)
					&& (dbObject2.getDataType() == DataType.LIST)) {
				if (this.listAppend((DbList)dbObject0, (DbList)dbObject1, (DbList)dbObject2) == 1) {
					status = Status.SUCCESS;
					this.isEntry = false;
				} else {
					status = Status.ENTRY_FAIL;
				}
			} else {
				throw new InterpreterException("Invalid type to append.  Only lists can be appended");
				//status = Status.ENTRY_FAIL;
			}
		} else {
			status = Status.FAIL;
		}

		if (status != Status.SUCCESS)
			this.cleanUp();

		this.traceGetTupleExit(status);

		return status;
	}

	protected int listAppend(DbList list1, DbList list2, DbList list3) {
		if (list1.getLength() + list2.getLength() == list3.getLength()) {
			DbList node1 = list1;
			DbList node2 = list2;
	      	DbList node3 = list3;

	      	for (int i = 0; i < list1.getLength(); node1 = node1.getTail(), node3 = node3.getTail(), i++)
	      		if (node1.getHead() != node3.getHead())
	      			return 0;

	      	if (node2.equals(node3))
	      		return 1;
		}
	    return 0;
	}
	
	@Override
	public AppendBBBNode copy(ProgramContext programContext) {
		AppendBBBNode copy = new AppendBBBNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
