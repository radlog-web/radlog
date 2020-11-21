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

public class AppendBBFNode 
	extends AppendNode {
	
	public AppendBBFNode(NodeArguments args, Binding binding, VariableList freeVariables) {
		super("append_bbf", args, binding, freeVariables);
	}

	@Override
	public Status getTuple() {
		Status status;
		
		this.traceGetTupleEntry();  
		
		this.freeVariableList.makeFree();
		
		if (this.isEntry) {			
			DbTypeBase dbObject0 = null;
			DbTypeBase dbObject1 = null;
			
			if (((dbObject0 = this.getArgumentAsDbType(0)) != null)
					&& ((dbObject1 = this.getArgumentAsDbType(1)) != null)
					&& (dbObject0.getDataType() == DataType.LIST)
					&& (dbObject1.getDataType() == DataType.LIST)) {
				
				DbList fullList;
				
				if (((fullList = this.listAppend((DbList)dbObject0, (DbList)dbObject1)) != null)
						&& this.postSelectForAppend(fullList, 2) == 1) {
					this.isEntry = false;
					status = Status.SUCCESS;
				} else {
					status = Status.ENTRY_FAIL;
				}
			} else {
				throw new InterpreterException("Invalid type to append.  Only lists can be appended");
			}
	    } else {
	    	status = Status.FAIL;
	    }

		if (status != Status.SUCCESS)
			this.cleanUp();

		this.traceGetTupleExit(status);

		return status;
	}

	protected DbList listAppend(DbList list1, DbList list2) {
		int	length1 = list1.getLength();
		DbTypeBase[] dbTypeObjectArray = new DbTypeBase[length1];
		DbList tempList2 = list2;
		
		this.copyElementsIntoArray(list1, dbTypeObjectArray);
		
		for (int i = length1 - 1; i >= 0; i--)
			tempList2 = this.typeManager.createList(dbTypeObjectArray[i], tempList2);

		this.createdList = tempList2;

		return tempList2;
	}
	
	@Override
	public AppendBBFNode copy(ProgramContext programContext) {
		AppendBBFNode copy = new AppendBBFNode(programContext.copyArguments(this.arguments), 
				this.bindingPattern.copy(), 
				programContext.copyVariableList(this.freeVariableList));
		
		programContext.getNodeMapping().put(this, copy);

		return copy;
	}
}
