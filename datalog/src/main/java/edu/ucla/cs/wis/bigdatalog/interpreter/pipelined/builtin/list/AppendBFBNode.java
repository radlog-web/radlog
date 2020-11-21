package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.builtin.list;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.database.type.DbList;
import edu.ucla.cs.wis.bigdatalog.database.type.DbTypeBase;
import edu.ucla.cs.wis.bigdatalog.exception.InterpreterException;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.interpreter.pipelined.ProgramContext;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.VariableList;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class AppendBFBNode 
	extends AppendNode {
	
	public AppendBFBNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super("append_bfb", args, binding, freeVariables);
	}

	@Override
	public Status getTuple() {
		Status status;

		this.traceGetTupleEntry();  
		
		this.freeVariableList.makeFree();

		if (this.isEntry) {
			DbTypeBase dbObject0 = null;
			DbTypeBase dbObject1 = null;
			DbTypeBase dbObject2 = null;

			if (((dbObject0 = this.getArgumentAsDbType(0)) != null)
					&& ((dbObject2 = this.getArgumentAsDbType(2)) != null)
					&& (dbObject0.getDataType() == DataType.LIST)
					&& (dbObject2.getDataType() == DataType.LIST)) {

				if (((dbObject1 = this.listAppend((DbList)dbObject0, (DbList)dbObject2)) != null) &&
						this.postSelectForAppend(dbObject1, 1) == 1) {
					status = Status.SUCCESS;
					this.isEntry = false;
				} else {
					status = Status.ENTRY_FAIL;
				}
			} else {
				throw new InterpreterException("Invalid type to append.  Only lists can be appended");
			}
	    } else {
	    	this.isEntry = true;
	    	status = Status.FAIL;
	    }

		if (status != Status.SUCCESS)
			this.cleanUp();

		this.traceGetTupleExit(status);

		return status;
	}

	protected DbTypeBase listAppend(DbList list1, DbList list3) {
		DbTypeBase retvaldbList = null;
		int list1Length = list1.getLength();
		int list3Length = list3.getLength();

		if (list3Length >= list1Length) {
			DbList node1 = list1;
			DbList node3 = list3;
			
			int i;
			for (i = 0; i < list1Length; i++, node1 = node1.getTail(), node3 = node3.getTail())
				if (node1.getHead() != node3.getHead())
					break;
	      
			if (i >= list1Length) {
				retvaldbList = node3;
				this.createdList = (DbList)retvaldbList;				
			}
	    }
		
		return retvaldbList;
	}
	
	@Override
	public AppendBFBNode copy(ProgramContext programContext) {
		AppendBFBNode copy = new AppendBFBNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);
		
		return copy;
	}
}
