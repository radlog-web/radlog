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

public class AppendFBBNode 
	extends AppendNode {
	
	public AppendFBBNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super("append_fbb", args, binding, freeVariables);
	}

	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();  

		this.freeVariableList.makeFree();

		if (this.isEntry) {
			DbTypeBase dbObject1 = null;
			DbTypeBase dbObject2 = null;

			if (((dbObject1 = this.getArgumentAsDbType(1)) != null)
					&& ((dbObject2 = this.getArgumentAsDbType(2)) != null)
					&& (dbObject1.getDataType() == DataType.LIST)
					&& (dbObject2.getDataType() == DataType.LIST)) {
				DbTypeBase dbObject0  = null;

				if (((dbObject0 = this.listAppend((DbList)dbObject1, (DbList)dbObject2)) != null) &&
						this.postSelectForAppend(dbObject0, 0) == 1) {
					status = Status.SUCCESS;
					this.isEntry = false;
				} else {
					status = Status.ENTRY_FAIL;
				}
			} else {
				throw new InterpreterException("Invalid type to append.  Only lists can be appended");
			}
		} else {
			status = Status.FAIL;
			this.cleanUp();
	    }

		if (status != Status.SUCCESS)
			this.cleanUp();

		this.traceGetTupleExit(status);

		return status;
	}

	protected DbTypeBase listAppend(DbList list2, DbList list3) {
		DbList retvaldbList;
		int list2Length = list2.getLength();
		int list3Length = list3.getLength();

		if (list3Length >= list2Length) {
			int	list1Length = list3Length - list2Length;
			DbList node2 = list2;
			DbList node3 = list3.getSublist(list1Length);

			if (node2.equals(node3)) {
				DbTypeBase[] dbTypeArray = new DbTypeBase[list1Length];

				this.copyElementsIntoArray(list3, dbTypeArray);
				retvaldbList = this.typeManager.createList(dbTypeArray);
				
				this.createdList = retvaldbList;

				return retvaldbList;
			}
	    }

		return null;
	}
	
	@Override
	public AppendFBBNode copy(ProgramContext programContext) {
		AppendFBBNode copy = new AppendFBBNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
